# Author: Jericho Tismo #
# Date: December 08, 2024 #
# Background Service #

import logging
import os
import shutil
from pymongo import MongoClient
import time
from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler
import csv
from pymongo.errors import DuplicateKeyError

# Set up logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(message)s")

# Define the archive directory for processed files
ARCHIVE_PATH = "storage/app/medalists/archive/"
os.makedirs(ARCHIVE_PATH, exist_ok=True)

class MedalistsHandler(FileSystemEventHandler):
    def __init__(self, db):
        self.db = db

    def validate_data(self, row):
        """Validate data entries (required fields, correct types)."""
        required_fields = ["name", "medal_type", "medal_code", "event", "medal_date"]
        for field in required_fields:
            if field not in row or not row[field]:
                logging.error(f"Missing or empty field: {field} in row {row}")
                return False

        # Validate data types (e.g., medal_date should be a valid date)
        try:
            if row["medal_date"] and not self.is_valid_date(row["medal_date"]):
                logging.error(f"Invalid date format in row {row}: {row['medal_date']}")
                return False
        except Exception as e:
            logging.error(f"Error validating row {row}: {e}")
            return False

        return True

    def is_valid_date(self, date_str):
        """Validate the date format (adjust the format to your needs)."""
        from datetime import datetime
        try:
            datetime.strptime(date_str, "%Y-%m-%d")  # Assuming the date format is YYYY-MM-DD
            return True
        except ValueError:
            return False

    def transform_data(self, row):
        """Perform necessary data transformations."""
        row["medal_code"] = row["medal_code"].upper()
        row["name"] = row["name"].title()
        return row

    def process_file(self, event):
        """Process a single file."""
        if event.is_directory or not event.src_path.endswith(".csv"):
            return

        # Log when a new file is created
        logging.info(f"New CSV file detected: {event.src_path}")

        try:
            # Process CSV
            with open(event.src_path, "r") as file:
                reader = csv.DictReader(file)
                for row in reader:
                    # Validate data before processing
                    if not self.validate_data(row):
                        continue  # Skip invalid rows

                    # Transform data as needed
                    row = self.transform_data(row)

                    # Create a unique key or condition based on your identifiers
                    unique_identifier = {
                        "name": row["name"],
                        "medal_code": row["medal_code"],
                        "event": row["event"]
                    }

                    # Check if the record already exists based on the unique identifier
                    existing_record = self.db.medalists.find_one(unique_identifier)

                    if existing_record:
                        # uncomment the code below to see each record that is skipped due to duplicate record in the database
                        # logging.warning(f"Duplicate record skipped: {row['name']}, {row['medal_code']}, {row['event']}")
                        continue  # Skip to the next record

                    try:
                        # Log each insertion
                        logging.info(f"Inserting record: {row['name']}, {row['medal_type']}")
                        self.db.medalists.insert_one({
                            "name": row["name"],
                            "medal_type": row["medal_type"],
                            "gender": row["gender"],
                            "country": row["country"],
                            "country_code": row["country_code"],
                            "nationality": row["nationality"],
                            "medal_code": row["medal_code"],
                            "discipline": row["discipline"],
                            "event": row["event"],
                            "medal_date": row["medal_date"]
                        })
                    except DuplicateKeyError:
                        # Log when a duplicate is skipped
                        logging.warning(f"Duplicate record skipped: {row['name']}")

            # After processing, move the file to the archive directory
            archive_file_path = os.path.join(ARCHIVE_PATH, os.path.basename(event.src_path))
            shutil.move(event.src_path, archive_file_path)
            logging.info(f"File moved to archive: {archive_file_path}")

        except Exception as e:
            # Log any errors encountered during processing
            logging.error(f"Error processing file {event.src_path}: {e}")

    def on_any_event(self, event):
        """Handle events and process files synchronously."""
        self.process_file(event)

def start_service():
    """Start the background service."""
    try:
        client = MongoClient("mongodb://localhost:27017/") 
        logging.info("MongoDB client created successfully.")

        # Check if the 'medalists_db' exists
        db = client.medalists_db  # Access the 'medalists_db' database
        logging.info(f"Accessing database: {db.name}")

        # Create event handler for monitoring CSV file uploads
        event_handler = MedalistsHandler(db)

        # Set up file system observer
        observer = Observer()
        observer.schedule(event_handler, path="storage/app/medalists/", recursive=True)
        observer.start()

        logging.info("Background service is running...")  
        try:
            while True:
                time.sleep(1)
        except KeyboardInterrupt:
            observer.stop()
            logging.info("Background service stopped.")
        observer.join()

    except Exception as e:
        logging.error(f"Error starting the service: {e}")
        time.sleep(5)  # Retry after a brief pause
        start_service()  # Retry service start if an error occurs

if __name__ == "__main__":
    start_service()