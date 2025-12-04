import os
import logging
from typing import List, Dict
import dotenv
from supabase import create_client, Client
from postgrest import APIError
from datetime import datetime

logger = logging.getLogger(__name__)
dotenv.load_dotenv()

class SupabaseHandlerFairHealth:
    """Handle Supabase database operations for FairHealth Facility data with historical archival"""
    
    def __init__(self):
        self.supabase_url = os.getenv("SUPABASE_URL")
        self.supabase_key = os.getenv("SUPABASE_KEY")
        
        # --- Table Configuration ---
        self.updated_table = "updated_medical_benchmarking_data" 
        self.historical_table = "historical_medical_benchmarking_data"
        self.logging_table = "logging_table"
        
        # --- Script Configuration ---
        self.script_name = "Fairhealth Facility"
        # Handling multiple specific data types as requested
        self.data_types = ["Facility USA", "Facility 070", "Facility 074"]
        
        if not self.supabase_url or not self.supabase_key:
            logger.error("❌ Missing Supabase credentials. Ensure SUPABASE_URL and SUPABASE_KEY are in .env file.")
            raise ValueError("Missing Supabase credentials in environment variables")
        
        try:
            self.client: Client = create_client(self.supabase_url, self.supabase_key)
            logger.info(f"✅ Supabase client initialized for table: '{self.updated_table}'")
        except Exception as e:
            logger.error(f"❌ Failed to initialize Supabase client: {e}")
            raise

    def insert_records(self, records: List[Dict]) -> dict:
        """
        Complete pipeline for FairHealth Facility data:
        1. Query existing records for ALL target data_types
        2. Archive them to historical table
        3. Delete them from updated table
        4. Insert new records
        5. Log the operation
        """
        if not records:
            logger.warning("⚠️ No records to insert.")
            return {
                "status": "no_records", 
                "records_inserted": 0, 
                "table": self.updated_table,
                "records_archived": 0
            }

        logger.info(f"🚀 Starting pipeline for '{self.script_name}'...")
        logger.info(f"   Processing {len(records)} new records")
        logger.info(f"   Target data_types: {self.data_types}")
        
        try:
            # ===== STEP 1: Query existing records =====
            logger.info(f"📥 STEP 1: Querying existing records for target data_types...")
            
            # Use .in_() to find records matching any of the 3 types
            existing_response = self.client.table(self.updated_table)\
                .select("*")\
                .in_("data_type", self.data_types)\
                .execute()
            
            existing_records = existing_response.data
            records_to_archive = len(existing_records)
            logger.info(f"   Found {records_to_archive} existing records to archive")

            # ===== STEP 2: Archive to historical =====
            if records_to_archive > 0:
                logger.info(f"📦 STEP 2: Archiving {records_to_archive} records...")
                
                # Remove 'id' to allow new UUID generation in history table
                records_for_history = []
                for record in existing_records:
                    historical_record = {k: v for k, v in record.items() if k != 'id'}
                    records_for_history.append(historical_record)
                
                self.client.table(self.historical_table).insert(records_for_history).execute()
                logger.info(f"   ✅ Archived records")
            else:
                logger.info("   ℹ️ No existing records to archive")

            # ===== STEP 3: Delete old records =====
            if records_to_archive > 0:
                logger.info(f"🗑️ STEP 3: Deleting old records...")
                
                # Delete all records matching the data types
                self.client.table(self.updated_table)\
                    .delete()\
                    .in_("data_type", self.data_types)\
                    .execute()
                
                logger.info(f"   ✅ Deleted old records")

            # ===== STEP 4: Insert new records =====
            records_to_insert = len(records)
            logger.info(f"📤 STEP 4: Inserting {records_to_insert} new records...")
            
            response = self.client.table(self.updated_table).insert(records).execute()
            
            # Verify insertion count
            actual_inserted = len(response.data) if response.data else 0
            logger.info(f"   ✅ Successfully inserted {actual_inserted} records")
            
            # ===== STEP 5: Log operation =====
            log_message = self._create_log_message(records_to_archive, actual_inserted)
            self._log_operation(log_message, success=True)
            
            return {
                "status": "success",
                "records_inserted": actual_inserted,
                "records_archived": records_to_archive,
                "table": self.updated_table
            }
            
        except APIError as e:
            error_msg = f"API Error: {e.message} (Details: {e.details})"
            logger.error(f"❌ {error_msg}")
            self._log_operation(f"Failed: {error_msg}", success=False)
            raise
        except Exception as e:
            error_msg = f"Unexpected Error: {str(e)}"
            logger.error(f"❌ {error_msg}")
            self._log_operation(f"Failed: {error_msg}", success=False)
            raise

    def _create_log_message(self, archived_count: int, inserted_count: int) -> str:
        """Create a human-readable log message"""
        timestamp = datetime.now().strftime("%B %d, %Y at %I:%M %p")
        
        if archived_count > 0:
            return (f"{self.script_name}: Archived {archived_count:,} old records "
                    f"and inserted {inserted_count:,} new records ({timestamp})")
        else:
            return (f"{self.script_name}: Inserted {inserted_count:,} new records "
                    f"({timestamp})")

    def _log_operation(self, message: str, success: bool = True):
        """
        Log to database. 
        Note: Fits the schema (id, created_at, message, script)
        """
        try:
            log_entry = {
                "script": self.script_name,
                "message": message
            }
            # Insert into logging table
            self.client.table(self.logging_table).insert(log_entry).execute()
            
            status_icon = "✅" if success else "❌"
            logger.info(f"{status_icon} Logged to DB: {message}")
            
        except Exception as e:
            logger.error(f"⚠️ Failed to write to logging_table: {e}")