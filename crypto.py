# Last run: Tue Sep  8 05:00:48 UTC 2026
# Import Packages
import os
import sys
import json
import time
import requests
import pandas as pd
from google.api_core.exceptions import NotFound
from datetime import datetime, timedelta, timezone

from google.cloud import bigquery
from google.oauth2.service_account import Credentials

# Get Date
now = datetime.now()
year = now.year
month = now.strftime("%b").lower()  # jan, feb, mar

table_suffix = f"{year}_{month}"
table_suffix

# Initialize BigQuery client
client = bigquery.Client()

API_URL = "https://api.coingecko.com/api/v3/coins/markets"
PARAMS = {
    "vs_currency": "usd",
    "order": "market_cap_desc",
    "per_page": 10,
    "page": 1,
    "sparkline": "false",
    "price_change_percentage": "7d"
}

def fetch_crypto_data():
    response = requests.get(API_URL, params=PARAMS)
    data = response.json()
    df = pd.DataFrame(data)

    # Compute total volume (% of all 24h volumes)
    df["total_vol"] = (df["total_volume"] / df["total_volume"].sum()) * 100

    # Add timestamp in UTC+3 (Kenya time)
    local_time = datetime.now(timezone.utc) + timedelta(hours=3)
    df["timestamp"] = local_time.strftime("%Y-%m-%d %H:%M:%S")

    # Select and rename columns to your preferred structure
    df = df[[
        "timestamp",
        "name",
        "symbol",
        "current_price",
        "total_volume",
        "total_vol",
        "price_change_percentage_24h",
        "price_change_percentage_7d_in_currency",
        "market_cap"
    ]]

    df = df.rename(columns={
        "current_price": "price_usd",
        "total_volume": "vol_24h",
        "price_change_percentage_24h": "chg_24h",
        "price_change_percentage_7d_in_currency": "chg_7d",
        "market_cap": "market_cap"
    })

    # Format numeric fields for readability
    df["price_usd"] = df["price_usd"].map("${:,.2f}".format)
    df["market_cap"] = df["market_cap"].map("${:,.0f}".format)
    df["vol_24h"] = df["vol_24h"].map("${:,.2f}".format)
    df["chg_24h"] = df["chg_24h"].map(lambda x: f"{x:+.2f}%" if pd.notna(x) else "N/A")
    df["chg_7d"] = df["chg_7d"].map(lambda x: f"{x:+.2f}%" if pd.notna(x) else "N/A")
    df["total_vol"] = df["total_vol"].map("{:.2f}%".format)

    return df

# Example usage
if __name__ == "__main__":
    bigdata = fetch_crypto_data()

# Standardize Data Types
bigdata['price_usd'] = bigdata['price_usd'].astype(str)

# Define Table ID
table_id = f"data-storage-485106.investing.crypto_{table_suffix}"

if now.day == 1 or now.day == 2: 

    # Check if current month table already has current month data
    try:
        check_sql = f"""
                    SELECT COUNT(*) AS cnt
                    FROM `{table_id}`
                    WHERE EXTRACT(MONTH FROM CAST(timestamp AS DATETIME)) = {now.month}
                      AND EXTRACT(YEAR FROM CAST(timestamp AS DATETIME)) = {now.year}
                    """
        check_df = client.query(check_sql).to_dataframe()
        has_current_month_data = check_df.loc[0, "cnt"] > 0
    except NotFound:
        has_current_month_data = False  # Table doesn't exist yet
  
    if not has_current_month_data:
      try:
        prev_month_date = now.replace(day=1) - timedelta(days=1)
        prev_table_suffix = f"{prev_month_date.year}_{prev_month_date.strftime('%b').lower()}"
        prev_table_id = f"data-storage-485106.investing.crypto_{prev_table_suffix}"
        
        try:
            prev_data = client.query(
                f"SELECT * FROM `{prev_table_id}` ORDER BY timestamp DESC"
            ).to_dataframe()
            bigdata = pd.concat([prev_data, bigdata], ignore_index=True)
            print(f"Appended {len(prev_data)} rows from previous month table.")
        except NotFound:
            print("No previous month table found, skipping append.")
        
        job = client.load_table_from_dataframe(
            bigdata,
            table_id,
            job_config=bigquery.LoadJobConfig(write_disposition="WRITE_APPEND")
        )
        job.result()
        print(f"All data loaded into {table_id}, total rows: {len(bigdata)}")

      except Exception as e:
          print(f"Error during 1st-of-month load: {e}")

else:
    # 🔥 NORMAL WORKFLOW (this was missing)
    job = client.load_table_from_dataframe(
        bigdata,
        table_id,
        job_config=bigquery.LoadJobConfig(write_disposition="WRITE_APPEND")
    )
    job.result()
    print(f"Normal load completed into {table_id}, rows: {len(bigdata)}")

# Define SQL Query to Retrieve Open Weather Data from Google Cloud BigQuery
sql = (f"""
        SELECT *
        FROM `{table_id}`
       """)
    
# Run SQL Query
data = client.query(sql).to_dataframe()

# Check Shape of data from BigQuery
print(f"Shape of dataset from BigQuery : {data.shape}")

# Delete Original Table
client.delete_table(table_id)
print(f"Table deleted successfully.")

# Check Total Number of Duplicate Records
duplicated = data.duplicated(subset=[
    'timestamp', 
    'name', 
    'symbol', 
    'price_usd', 
    'vol_24h', 
    'total_vol', 
    'chg_24h', 
    'chg_7d', 
    'market_cap']).sum()
    
# Remove Duplicate Records
data.drop_duplicates(subset=[
    'timestamp', 
    'name', 
    'symbol', 
    'price_usd', 
    'vol_24h', 
    'total_vol', 
    'chg_24h', 
    'chg_7d', 
    'market_cap'], inplace=True)

# Define the dataset ID and table ID
dataset_id = 'investing'
table_id = f"crypto_{table_suffix}"
    
# Define the table schema for new table
schema = [
        bigquery.SchemaField("timestamp", "STRING"),
        bigquery.SchemaField("name", "STRING"),
        bigquery.SchemaField("symbol", "STRING"),
        bigquery.SchemaField("price_usd", "STRING"),
        bigquery.SchemaField("vol_24h", "STRING"),
        bigquery.SchemaField("total_vol", "STRING"),
        bigquery.SchemaField("chg_24h", "STRING"),
        bigquery.SchemaField("chg_7d", "STRING"),
        bigquery.SchemaField("market_cap", "STRING"),
    ]
    
# Define the table reference
table_ref = client.dataset(dataset_id).table(table_id)
    
# Create the table object
table = bigquery.Table(table_ref, schema=schema)

try:
    # Create the table in BigQuery
    table = client.create_table(table)
    print(f"Table {table.table_id} created successfully.")
except Exception as e:
    print(f"Table {table.table_id} failed")

# Define the BigQuery table ID
table_id = f"data-storage-485106.investing.crypto_{table_suffix}"

# Load the data into the BigQuery table
job = client.load_table_from_dataframe(data, table_id)

# Wait for the job to complete
while job.state != 'DONE':
    time.sleep(2)
    job.reload()
    print(job.state)

# Return Data Info
print(f"Data {data.shape} has been successfully retrieved, saved, and appended to the BigQuery table.")

# Exit 
print(f'Cryptocurrency Data Export to Google BigQuery Successful')
