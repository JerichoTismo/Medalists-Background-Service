# Medalists-Background-Service
This project is a background service designed to monitor a directory for new CSV files, validate and process their contents, and insert the data into a MongoDB database. The service also moves processed files to an archive directory and logs all operations.

## Features
- **CSV Monitoring:** Automatically detects new CSV files in a specific directory.
- **Data Validation:** Ensures data consistency and validity before insertion.
- **Duplicate Record Prevention:** Avoids duplicate entries using unique identifiers.
- **Archiving:** Moves processed files to an archive directory.
- **Robust Logging:** Logs all activities and errors for debugging.
- **Error Handling:** Ensures continuous operation even when exceptions occur.

---

## Installation

### Prerequisites
1. **Python 3.8 or later**: Install from the [official website](https://www.python.org/).
2. **MongoDB**: Install and configure MongoDB as follows:
   - Download and install MongoDB from the [official website](https://www.mongodb.com/try/download/community).
   - Ensure MongoDB is running on `localhost:27017`.

### Steps
1. Clone the repository:
   ```bash
   git clone <your-repository-url>
   cd <your-project-directory>
2. Setup Python Environment:
   ```bash
     python -m venv venv
     source venv/bin/activate   # For Linux/Mac
     venv\Scripts\activate      # For Windows
4. Install Dependencies:
   ```bash
     pip install -r requirements.txt
6. Create the necessary directory:
   ```bash
   mkdir -p storage/app/medalists/archive

Usage
API Usage
Endpoint to upload CSV files:
POST /upload
Endpoint to get aggregated stats:
GET /aggregated_stats/event?page=<page_number>&per_page=<number>

Start the Background Service
Run the following command to start the service:
```bash
      uvicorn api:app --reload
      python .\service.py





