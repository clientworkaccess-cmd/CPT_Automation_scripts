import os
import logging
from typing import List, Dict
import dotenv
from supabase import create_client, Client
from postgrest import APIError
from datetime import datetime

logger = logging.getLogger(__name__)
dotenv.load_dotenv()

class SupabaseHandler:
    """Handle Supabase database operations for NJ Medical PIP data with historical archival"""
    
    def __init__(self):
        self.supabase_url = os.getenv("SUPABASE_URL")
        self.supabase_key = os.getenv("SUPABASE_KEY")
        
        # Table names
        self.updated_table = "updated_medical_benchmarking_data"
        self.historical_table = "historical_medical_benchmarking_data"
        self.logging_table = "logging_table"
        
        # Script configuration
        self.script_name = "New Jersey DOBI"
        self.data_types = ["Facility PIP", "Physician PIP"]  # Multiple data types
        
        if not self.supabase_url or not self.supabase_key:
            logger.error("❌ Missing Supabase credentials. Ensure SUPABASE_URL and SUPABASE_KEY are in .env file.")
            raise ValueError("Missing Supabase credentials in environment variables")
        
        try:
            self.client: Client = create_client(self.supabase_url, self.supabase_key)
            logger.info(f"✅ Supabase client initialized for table: '{self.updated_table}'")
        except Exception as e:
            logger.error(f"Failed to initialize Supabase client: {e}")
            raise

    def insert_records(self, records: List[Dict]) -> dict:
        """
        Complete pipeline with historical archival for MULTIPLE data types:
        1. Query existing records for BOTH data_types from updated_table
        2. Insert those records into historical_table (archive)
        3. Delete those records from updated_table
        4. Insert new cleaned records into updated_table
        5. Log the operation
        
        Returns: Summary of insertion results
        """
        if not records:
            logger.warning("⚠️ No records to insert.")
            return {
                "status": "no_records",
                "records_inserted": 0,
                "table": self.updated_table,
                "records_archived": 0,
                "records_deleted": 0
            }
        
        logger.info(f"🚀 Starting pipeline for '{self.script_name}'...")
        logger.info(f"   Processing {len(records)} new records")
        logger.info(f"   Target data_types: {self.data_types}")
        
        try:
            # ===== STEP 1: Query existing records for BOTH data_types =====
            logger.info(f"📥 STEP 1: Querying existing records for data_types: {self.data_types}...")
            
            # Query using .in_() for multiple values
            existing_response = self.client.table(self.updated_table)\
                .select("*")\
                .in_("data_type", self.data_types)\
                .execute()
            
            existing_records = existing_response.data
            records_to_archive = len(existing_records)
            
            # Count by data_type for logging
            facility_count = sum(1 for r in existing_records if r.get('data_type') == 'Facility PIP')
            physician_count = sum(1 for r in existing_records if r.get('data_type') == 'Physician PIP')
            
            logger.info(f"   Found {records_to_archive} existing records total:")
            logger.info(f"      - Facility PIP: {facility_count}")
            logger.info(f"      - Physician PIP: {physician_count}")
            
            # ===== STEP 2: Archive existing records to historical_table =====
            if records_to_archive > 0:
                logger.info(f"📦 STEP 2: Archiving {records_to_archive} records to historical table...")
                
                # Remove 'id' field before inserting (let historical table generate new IDs)
                records_for_history = []
                for record in existing_records:
                    historical_record = {k: v for k, v in record.items() if k != 'id'}
                    records_for_history.append(historical_record)
                
                self.client.table(self.historical_table).insert(records_for_history).execute()
                logger.info(f"   ✅ Archived {records_to_archive} records to historical table")
            else:
                logger.info("   ℹ️ No existing records to archive (first run or fresh data)")
            
            # ===== STEP 3: Delete old records from updated_table =====
            if records_to_archive > 0:
                logger.info(f"🗑️ STEP 3: Deleting {records_to_archive} old records from updated table...")
                
                # Delete using .in_() for multiple data_types
                self.client.table(self.updated_table)\
                    .delete()\
                    .in_("data_type", self.data_types)\
                    .execute()
                
                logger.info(f"   ✅ Deleted {records_to_archive} old records")
            
            # ===== STEP 4: Insert new records into updated_table =====
            records_to_insert = len(records)
            logger.info(f"📤 STEP 4: Inserting {records_to_insert} new records into updated table...")
            
            # Log sample record for debugging
            logger.info(f"   Sample record: {records[0]}")
            
            response = self.client.table(self.updated_table).insert(records).execute()
            
            # Count by data_type for new records
            new_facility_count = sum(1 for r in records if r.get('data_type') == 'Facility PIP')
            new_physician_count = sum(1 for r in records if r.get('data_type') == 'Physician PIP')
            
            logger.info(f"   ✅ Successfully inserted {records_to_insert} new records:")
            logger.info(f"      - Facility PIP: {new_facility_count}")
            logger.info(f"      - Physician PIP: {new_physician_count}")
            
            # ===== STEP 5: Log the operation =====
            log_message = self._create_log_message(records_to_archive, records_to_insert)
            self._log_operation(log_message)
            
            # ===== Return summary =====
            return {
                "status": "success",
                "records_inserted": records_to_insert,
                "records_archived": records_to_archive,
                "records_deleted": records_to_archive,
                "table": self.updated_table,
                "script_name": self.script_name
            }
            
        except APIError as e:
            error_message = f"Failed to process {self.script_name}: API Error - {e.message}"
            logger.error(f"❌ {error_message}")
            logger.error(f"    Details: {e.details}")
            logger.error(f"    Hint: {e.hint}")
            
            # Log the failure
            self._log_operation(error_message, success=False)
            
            raise
        except Exception as e:
            error_message = f"Failed to process {self.script_name}: {str(e)}"
            logger.error(f"❌ {error_message}")
            
            # Log the failure
            self._log_operation(error_message, success=False)
            
            raise Exception(error_message)
    
    def _create_log_message(self, archived_count: int, inserted_count: int) -> str:
        """Create a human-readable log message"""
        timestamp = datetime.now().strftime("%B %d, %Y at %I:%M %p")
        
        if archived_count > 0:
            message = (f"{self.script_name}: Archived {archived_count:,} old records "
                      f"and inserted {inserted_count:,} new records ({timestamp})")
        else:
            message = (f"{self.script_name}: Inserted {inserted_count:,} new records "
                      f"({timestamp})")
        
        return message
    
    def _log_operation(self, message: str, success: bool = True):
        """Log operation to logging_table"""
        try:
            log_entry = {
                "message": message,
                "script": self.script_name
            }
            
            self.client.table(self.logging_table).insert(log_entry).execute()
            
            status = "✅" if success else "❌"
            logger.info(f"{status} Logged to database: {message}")
            
        except Exception as e:
            logger.error(f"⚠️ Failed to write log entry: {e}")
            # Don't raise - logging failure shouldn't break the pipeline