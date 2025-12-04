import pandas as pd
from pathlib import Path
import logging
import numpy as np

logger = logging.getLogger(__name__)

class DataProcessorASC:
    """Process and clean the downloaded ASC Addendum B file"""

    SOURCE_HCPCS_COL = "HCPCS Code"
    SOURCE_DESC_COL = "Short Descriptor"
    # This is the key text to identify the payment column and extract the date
    SOURCE_RATE_MARKER = "Payment Rate"

    def read_excel(self, file_path: Path) -> pd.DataFrame:
        """Read Excel file into DataFrame, finding the correct header row"""
        logger.info(f"📖 Reading Excel file: {file_path}")
        
        if file_path.suffix.lower() not in ['.xlsx', '.xls']:
             raise ValueError(f"File is not an Excel file: {file_path}")

        try:
            # Load the file without a header first to find the real one
            df_temp = pd.read_excel(file_path, header=None)
            
            header_row_idx = None
            
            # Find the header row by searching for key columns
            for idx, row in df_temp.iterrows():
                row_str = " ".join(row.astype(str).fillna("")).upper()
                if (self.SOURCE_HCPCS_COL.upper() in row_str and 
                    self.SOURCE_DESC_COL.upper() in row_str):
                    
                    header_row_idx = idx 
                    logger.info(f"✅ Found header row at index: {header_row_idx}")
                    break
            
            if header_row_idx is None:
                raise ValueError(f"Could not find header row with '{self.SOURCE_HCPCS_COL}' and '{self.SOURCE_DESC_COL}'")

            # Now, read the file again using the correct header row
            df = pd.read_excel(file_path, header=header_row_idx)
            
            logger.info(f"✅ Loaded {len(df)} rows (raw)")
            logger.info(f"📋 Raw columns found: {list(df.columns)}")
            return df

        except Exception as e:
            logger.error(f"❌ Error reading Excel file: {e}")
            raise

    def clean_data(self, df: pd.DataFrame) -> pd.DataFrame:
        """Clean the raw DataFrame based on ASC requirements"""
        logger.info("🧹 Cleaning data...")

        # --- Find and map columns ---
        columns_mapping = {}
        available_columns = df.columns.astype(str)
        
        # Find HCPCS Code
        hcpcs_col = next((col for col in available_columns if self.SOURCE_HCPCS_COL.lower() in col.lower()), None)
        if hcpcs_col: columns_mapping['code'] = hcpcs_col

        # Find Short Descriptor
        desc_col = next((col for col in available_columns if self.SOURCE_DESC_COL.lower() in col.lower()), None)
        if desc_col: columns_mapping['code_description'] = desc_col
        
        # --- MODIFIED LOGIC: Find Payment Rate column generically ---
        rate_col_name = next((col for col in available_columns if self.SOURCE_RATE_MARKER.lower() in col.lower()), None)
        if rate_col_name: 
            columns_mapping['80th'] = rate_col_name
            logger.info(f"✅ Found Payment Rate column: '{rate_col_name}'")

        logger.info(f"📋 Column mapping: {columns_mapping}")

        # --- Check required columns ---
        required_keys = ['code', 'code_description', '80th']
        missing_keys = [key for key in required_keys if key not in columns_mapping]
        if missing_keys:
            raise ValueError(f"Missing required columns: {missing_keys}. Available columns: {list(available_columns)}")

        # Get the full column name we found (e.g., "July 2025 Payment Rate")
        rate_col_full_name = columns_mapping['80th']
        

        rel_date_value = rate_col_full_name.lower().replace(self.SOURCE_RATE_MARKER.lower(), "").strip()
        rel_date_value = rel_date_value.title() 
        
        logger.info(f"📅 Extracted 'rel_date' value: {rel_date_value}")
    
        df_cleaned = df[list(columns_mapping.values())].copy()
        df_cleaned.columns = required_keys

        logger.info("➕ Adding 'data_type' column...")
        df_cleaned['data_type'] = 'Medicare Facility'

        logger.info(f"➕ Adding 'rel_date' column with value: {rel_date_value}")
        df_cleaned['rel_date'] = rel_date_value
    
        df_cleaned = df_cleaned.dropna(subset=["code"])
        df_cleaned = df_cleaned[df_cleaned["code"].astype(str).str.strip().str.len() > 0]

        logger.info("🔢 Converting '80th' column to numeric...")
        

        df_cleaned['80th'] = pd.to_numeric(df_cleaned['80th'], errors='coerce')
        

        logger.info("🔧 Converting ALL NaN values to None for JSON compliance...")
        
        for col in df_cleaned.columns:
            # Check if the column has any nulls (NaN or NaT)
            if df_cleaned[col].isnull().any():
                logger.info(f"    -> Fixing NaNs in column '{col}'")
                # Apply the robust fix: convert to object, replace NaN/NaT with None
                df_cleaned[col] = df_cleaned[col].astype(object).where(pd.notnull(df_cleaned[col]), None)
    
        df_cleaned = df_cleaned.dropna(how="all")
        df_cleaned.reset_index(drop=True, inplace=True)

        logger.info(f"✅ Cleaned data: {len(df_cleaned)} rows remaining")
        logger.info(f"📊 Sample data (post-fix):\n{df_cleaned.head().to_string()}")
        return df_cleaned