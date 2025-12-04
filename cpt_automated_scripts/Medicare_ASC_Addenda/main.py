import logging
from pathlib import Path
from scraper import ASCScraper
from data_processor import DataProcessorASC
from database import SupabaseHandlerASC

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("asc_pipeline.log"),
        logging.StreamHandler()
    ]
)

logger = logging.getLogger(__name__)

def run_asc_pipeline():
    """
    Complete pipeline for ASC data: Scrape -> Clean -> Archive -> Insert -> Log
    """
    try:
        # Step 1: Scrape/Download
        logger.info("=" * 50)
        logger.info("STEP 1: DOWNLOADING ASC FILE")
        logger.info("=" * 50)
        scraper = ASCScraper()
        # This single method now handles download, zip, and extraction
        # It now returns a LIST of file paths
        data_file_paths = scraper.download_and_extract_file()
        
        # Handle the list of files
        if not data_file_paths:
            logger.error("❌ No data files were extracted by the scraper.")
            raise Exception("Scraper did not return any files to process.")
        
        # Process the first file found
        data_file_path = data_file_paths[0]
        logger.info(f"✅ Processing first extracted file: {data_file_path.name}")

        # Step 2: Clean Data
        logger.info("\n" + "=" * 50)
        logger.info("STEP 2: CLEANING ASC DATA")
        logger.info("=" * 50)
        processor = DataProcessorASC()
        
        # First, read the Excel file into a raw DataFrame
        df_raw = processor.read_excel(data_file_path)
        # Second, clean the raw DataFrame
        df_cleaned = processor.clean_data(df_raw)
        
        # Convert DataFrame to list of dictionaries for Supabase
        records = df_cleaned.to_dict('records')
        logger.info(f"✅ Prepared {len(records)} records for database")
        
        # Step 3: Archive Old + Insert New to Supabase
        logger.info("\n" + "=" * 50)
        logger.info("STEP 3: ARCHIVING OLD & INSERTING NEW DATA")
        logger.info("=" * 50)
        db = SupabaseHandlerASC()
        result = db.insert_records(records)
        
        # Final Summary
        logger.info("\n" + "=" * 50)
        logger.info("✅ ASC PIPELINE COMPLETED SUCCESSFULLY")
        logger.info("=" * 50)
        logger.info(f"📁 Downloaded & Extracted: {data_file_path.name}")
        logger.info(f"📊 Records processed: {len(records)}")
        logger.info(f"📦 Records archived: {result.get('records_archived', 0)}")
        logger.info(f"🗑️ Records deleted: {result.get('records_deleted', 0)}")
        logger.info(f"📤 Records inserted: {result.get('records_inserted', 0)}")
        logger.info(f"💾 Table: {result.get('table', 'N/A')}")
        
        return result
        
    except Exception as e:
        logger.error(f"\n❌ ASC PIPELINE FAILED: {str(e)}")
        # Log the full traceback to the file
        logger.exception("Full traceback:")
        raise

if __name__ == "__main__":
    run_asc_pipeline()
