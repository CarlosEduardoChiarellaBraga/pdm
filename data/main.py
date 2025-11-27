from fastapi import FastAPI, HTTPException
from google.cloud import storage
import yfinance as yf
import pandas as pd
from datetime import datetime, timezone
import os

app = FastAPI()
DATA_BUCKET = os.getenv("DATA_BUCKET")  # set this in Cloud Run

@app.get("/")
def root():
    return {"message": "Running!"}


def save_to_gcs(df: pd.DataFrame, filename: str):
    """Upload dataframe as CSV to GCS."""
    client = storage.Client()
    bucket = client.bucket(DATA_BUCKET)
    blob = bucket.blob(f"{filename}")

    # convert df to CSV string
    csv_data = df.to_csv(index=False)
    blob.upload_from_string(csv_data, content_type="text/csv")


@app.get("/download/{ticker}")
def download_stock(ticker: str):
    """Downloads and stores at cloud storage the data of given stock."""
    data = yf.Ticker(ticker) #
    df = data.history(period="5y") #
    df.reset_index(inplace=True) #
    df['Ticker'] = ticker #

    if df.empty:
        raise HTTPException(status_code=404, detail="Symbol not found or no data.")

    timestamp = datetime.now(timezone.utc).strftime("%Y%m%d-%H%M%S")
    filename = f"raw/{ticker}_{timestamp}.csv"

    save_to_gcs(df, filename)

    return {"status": "success", "file": filename}


"""
gcloud run deploy pdm-projeto-final --source . --region us-central1 --allow-unauthenticated  --set-env-vars DATA_BUCKET=mydata-pdm
"""
