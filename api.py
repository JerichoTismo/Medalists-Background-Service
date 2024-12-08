# Author: Jericho Tismo #
# Date: December 08, 2024 #
# API #

from fastapi import FastAPI, File, UploadFile, HTTPException
from pathlib import Path
import shutil
from pymongo import MongoClient
import logging

app = FastAPI()

# Define storage path
STORAGE_PATH = Path("storage/app/medalists/")
STORAGE_PATH.mkdir(parents=True, exist_ok=True)

client = MongoClient("mongodb://localhost:27017/") 
logging.debug("MongoDB client created successfully.")

# Check if the 'medalists_db' exists
db = client.medalists_db  # Access the 'medalists_db' database

#for testing of fast api response
@app.get("/")
def read_root():
    return {"message": "Welcome to the Medalists API!"}

# Post request for uploading csv files
@app.post("/upload")
async def upload_csv(file: UploadFile = File(...)):
    # Validate file extension
    if not file.filename.endswith(".csv"):
        raise HTTPException(status_code=400, detail="Only CSV files are allowed.")
    
    # Save the file
    file_path = STORAGE_PATH / file.filename
    logging.info(f"saved in {file_path}")
    with file_path.open("wb") as buffer:
        shutil.copyfileobj(file.file, buffer)
    
    return {"message": "File uploaded successfully", "filename": file.filename}


# Get Event Aggregate Stats Endpoint
from pymongo import MongoClient
from fastapi import Query

client = MongoClient("mongodb://localhost:27017/")
db = client.medalists_db

@app.get("/aggregated_stats/event")
def get_aggregated_stats(page: int = Query(1, ge=1), per_page: int = Query(10, le=50)):
    skip = (page - 1) * per_page
    data = list(db.medalists.aggregate([
        {"$group": {
            "_id": {"discipline": "$discipline", "event": "$event", "event_date": "$medal_date"},
            "medalists": {"$push": {
                "name": "$name",
                "medal_type": "$medal_type",
                "gender": "$gender",
                "country": "$country",
                "medal_date": "$medal_date"
            }}
        }},
        {"$skip": skip},
        {"$limit": per_page}
    ]))

    total_count = db.medalists.count_documents({})
    total_pages = -(-total_count // per_page)
    return {
        "data": data,
        "paginate": {
            "current_page": page,
            "total_pages": total_pages,
            "next_page": page + 1 if page < total_pages else None,
            "previous_page": page - 1 if page > 1 else None
        }
    }
