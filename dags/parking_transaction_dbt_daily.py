from airflow.decorators import dag, task
from sodapy import Socrata
from datetime import datetime, timedelta
import pandas as pd
import os
from google.cloud import bigquery, storage

@dag(
    start_date=datetime(2024, 2, 1),
    schedule="@daily",
    catchup=False,
    doc_md=__doc__,
    default_args={"owner": "Kibby", "retries": 3},
    tags=["kibret"],
)
def parking_transaction_dbt_daily():
    
    @task()
    def process_query_result():
        client = bigquery.Client()
        PROJECT_ID = os.getenv("PROJECT_ID")
        DATASET_NAME = os.getenv("DATASET_NAME")
        TABLE_NAME = os.getenv("TABLE_NAME")
        latest_date_value = None

        table_ref = client.dataset(DATASET_NAME, project=PROJECT_ID).table(TABLE_NAME)
        table = client.get_table(table_ref)

        query_1 = f"SELECT MAX(transactiondatetime) FROM `{DATASET_NAME}.{TABLE_NAME}`"
        latest_date_result = client.query(query_1)
        for row in latest_date_result:
            latest_date_value = row[0]
        print(f"Latest transaction date from BigQuery: {latest_date_value}")

        if latest_date_value is None:
            return None

        return pd.to_datetime(latest_date_value).to_pydatetime().isoformat()
    
    @task()
    def get_transaction_yesterdays(ti=None):
        
        latest_date = ti.xcom_pull(task_ids="process_query_result")
        APP_TOKEN = os.getenv("APP_TOKEN")
        USERNAME = os.getenv("USERNAME")
        PASSWORD = os.getenv("PASSWORD")
        query = ""
        results_all = []
        batches = 10000
        offset = 0
        client = Socrata("data.seattle.gov", APP_TOKEN, username=USERNAME, password=PASSWORD, timeout=30)

        if latest_date is None:
            while True:
                results = client.get("gg89-k5p6", limit=batches, offset=offset)
                if not results:
                    break
                results_all.extend(results)
                offset += batches
        else:
            latest_date = pd.to_datetime(latest_date).to_pydatetime().strftime("%Y-%m-%dT%H:%M:%S")
            query = f"transactiondatetime > '{latest_date}'"
            while True:
                results = client.get("gg89-k5p6", where=query, limit=batches, offset=offset)
                if not results:
                    break
                results_all.extend(results)
                offset += batches

        client.close()
        results_df = pd.DataFrame.from_records(results_all)

        if results_df.empty:
            print("No data received from API.")
            return None

        results_df = results_df.drop('parkingspacenumber', axis=1)
        results_df["transaction_id"] = pd.to_numeric(results_df["transaction_id"], errors="coerce").fillna(0).astype("Int64")
        results_df["meter_code"] = pd.to_numeric(results_df["meter_code"], errors="coerce").fillna(0).astype("Int64")
        results_df["transactiondatetime"] = pd.to_datetime(results_df["transactiondatetime"])
        results_df["payment_mean"] = results_df["payment_mean"].astype(str)
        results_df["amount_paid"] = pd.to_numeric(results_df["amount_paid"], errors="coerce").fillna(0.0).astype(float)
        results_df["durationinminutes"] = pd.to_numeric(results_df["durationinminutes"], errors="coerce").fillna(0).astype("Int64")
        results_df["blockface_name"] = results_df["blockface_name"].astype(str)
        results_df["sideofstreet"] = results_df["sideofstreet"].astype(str)
        results_df["elementkey"] = pd.to_numeric(results_df["elementkey"], errors="coerce").fillna(0).astype("Int64")
        results_df["latitude"] = pd.to_numeric(results_df["latitude"], errors="coerce").fillna(0.0).astype(float)
        results_df["longitude"] = pd.to_numeric(results_df["longitude"], errors="coerce").fillna(0.0).astype(float)

        file_dir = "/tmp/parking_data"
        os.makedirs(file_dir, exist_ok=True)

        file_path = os.path.join(file_dir, f"parking_transactions_{datetime.now().strftime('%Y-%m-%d')}.csv")
        results_df.to_csv(file_path, index=False)

        print(f"{results_df.shape} is the shpe Data saved to: {file_path}, earliest date: {results_df['transactiondatetime'].min()}, latest date: {results_df['transactiondatetime'].max()} and columns: {results_df.columns} latest date = {latest_date}")
        return file_path

    @task()
    def load_data_to_bucket(ti=None):
        latest_date = ti.xcom_pull(task_ids="process_query_result")
        from_file_path = ti.xcom_pull(task_ids="get_transaction_yesterdays")


        if not from_file_path:
            print("No file to upload.")
            return None

        if latest_date is None:
            latest_date = "initialPull"

        client = storage.Client()
        bucket = client.bucket(os.getenv("BUCKET_NAME"))
        destination_blob_name = f"parking_data/{latest_date}/{os.path.basename(from_file_path)}"
        blob = bucket.blob(destination_blob_name)
        blob.upload_from_filename(from_file_path)

        gcs_file_path = f"gs://{os.getenv('BUCKET_NAME')}/{destination_blob_name}"
        print(f"✅ File uploaded to GCS: {gcs_file_path}")

        return gcs_file_path  

    

    
    @task
    def load_data_to_bigquery(ti=None):
        gcs_path = ti.xcom_pull(task_ids="load_data_to_bucket")
        
        if not gcs_path:
            print("No file found in GCS.")
            return 
        

        
        PROJECT_ID = os.getenv('PROJECT_ID')
        DATASET_NAME = os.getenv('DATASET_NAME')
        TABLE_NAME = os.getenv('TABLE_NAME') 
        

        client = bigquery.Client()

        table_id = f"{PROJECT_ID}.{DATASET_NAME}.{TABLE_NAME}"
        
        job_config = bigquery.LoadJobConfig(
            schema=[
                bigquery.SchemaField("transaction_id", "INTEGER","REQUIRED"),
                bigquery.SchemaField("meter_code", "INTEGER","REQUIRED"),
                bigquery.SchemaField("transactiondatetime", "TIMESTAMP","REQUIRED"),
                bigquery.SchemaField("payment_mean", "STRING"),
                bigquery.SchemaField("amount_paid", "FLOAT"),
                bigquery.SchemaField("durationinminutes", "INTEGER"),
                bigquery.SchemaField("blockface_name", "STRING"),
                bigquery.SchemaField("side_of_street", "STRING"),
                bigquery.SchemaField("elementkey", "INTEGER"),
                bigquery.SchemaField("latitude", "FLOAT"),
                bigquery.SchemaField("longitude", "FLOAT")
            ],
            skip_leading_rows=1,
            source_format=bigquery.SourceFormat.CSV,
        )
        uri = gcs_path

        load_job = client.load_table_from_uri(
            uri, table_id, job_config=job_config
        )  

        load_job.result() 

        destination_table = client.get_table(table_id)  # Make an API request.
        print("Loaded {} rows.".format(destination_table.num_rows))

        # print(f"Data successfully loaded to {table_ref}")

    process_query_result() >> get_transaction_yesterdays() >> load_data_to_bucket() >> load_data_to_bigquery()

parking_transaction_dbt_daily()
