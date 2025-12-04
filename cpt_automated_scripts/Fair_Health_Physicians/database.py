import logging
from supabase import create_client, Client
from typing import List, Dict
import os
from dotenv import load_dotenv
from datetime import datetime

logger = logging.getLogger(__name__)

class SupabaseHandlerPhysician:
    """Handle Supabase operations for Fair Health Physician data with historical archival"""
    
    def __init__(self):
        """Initialize Supabase client and configuration"""
        load_dotenv()
        
        self.supabase_url = os.getenv("SUPABASE_URL")
        self.supabase_key = os.getenv("SUPABASE_KEY")
        
        # --- Table Configuration ---
        self.updated_table = "updated_medical_benchmarking_data"
        self.historical_table = "historical_medical_benchmarking_data"
        self.logging_table = "logging_table"
        
        # --- Script Configuration ---
        self.script_name = "Fairhealth Physician"
        # Handling multiple specific data types
        self.data_types = ["Physician USA", "Physician 070"]
        
        if not self.supabase_url or not self.supabase_key:
            raise ValueError("SUPABASE_URL and SUPABASE_KEY must be set in environment variables")
        
        try:
            self.client: Client = create_client(self.supabase_url, self.supabase_key)
            logger.info(f"✅ Supabase client initialized for table: {self.updated_table}")
        except Exception as e:
            logger.error(f"❌ Failed to initialize Supabase client: {e}")
            raise
    
    def insert_records(self, records: List[Dict]) -> Dict:
        """
        Complete pipeline for FairHealth Physician:
        1. Query existing records for BOTH data_types
        2. Archive them to historical table
        3. Delete them from updated table
        4. Insert new records
        5. Log the operation
        """
        if not records:
            logger.warning("⚠️ No records to insert")
            return {
                "records_inserted": 0, 
                "records_archived": 0,
                "table": self.updated_table,
                "status": "no_records"
            }
        
        logger.info(f"🚀 Starting pipeline for '{self.script_name}'...")
        logger.info(f"   Processing {len(records)} new records")
        logger.info(f"   Target data_types: {self.data_types}")
        
        try:
            # ===== STEP 1: Query existing records =====
            logger.info(f"📥 STEP 1: Querying existing records for target data_types...")
            
            # Use .in_() to find records matching either Physician USA or Physician 070
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
            
            inserted_count = len(response.data) if response.data else 0
            logger.info(f"   ✅ Successfully inserted {inserted_count} records")
            
            # ===== STEP 5: Log operation =====
            log_message = self._create_log_message(records_to_archive, inserted_count)
            self._log_operation(log_message, success=True)
            
            return {
                "records_inserted": inserted_count,
                "records_archived": records_to_archive,
                "table": self.updated_table,
                "success": True
            }
            
        except Exception as e:
            error_msg = f"Failed to process {self.script_name}: {str(e)}"
            logger.error(f"❌ {error_msg}")
            logger.exception("Full traceback:")
            
            # Attempt to log the failure
            self._log_operation(f"FAILED: {error_msg}", success=False)
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
        Log to logging_table.
        Schema: (id, created_at, message, script)
        """
        try:
            log_entry = {
                "script": self.script_name,
                "message": message
            }
            
            self.client.table(self.logging_table).insert(log_entry).execute()
            
            status_icon = "✅" if success else "❌"
            logger.info(f"{status_icon} Logged to DB: {message}")
            
        except Exception as e:
            logger.error(f"⚠️ Failed to write to logging_table: {e}")