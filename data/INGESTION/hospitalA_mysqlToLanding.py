from google.cloud import storage, bigquery
from pyspark.sql import SparkSession
import datetime
import json

storage_client = storage.Client()
bq_client = bigquery.Client()

# Initialize Spark session
spark = SparkSession.builder.appName('HospitalAMYSQLToLanding').getOrCreate()

# Configuration variables
GCS_Bucket = 'heathcare-bucket-25032025'
Hospital_name = 'hospital-a'
Landing_path = f'gs://{GCS_Bucket}/landing/{Hospital_name}/'
Archive_path = f'gs://{GCS_Bucket}/landing/{Hospital_name}/archive/'
Config_File_path = f"gs://{GCS_Bucket}/config/load_config.csv"

BQ_Project = 'avd-practice-1'
BQ_Audit_Table = f'{BQ_Project}.temp_dataset.audit_log'
BQ_Log_Table = f'{BQ_Project}.temp_dataset.Pipeline_logs'
BQ_Temp_Path = f'gs://{GCS_Bucket}/temp/'

MYSQL_Config = {
    'url': 'jdbc:mysql://35.188.175.88:3306/hospital-a-db?useSSL=false&allowPublicKeyRetrieval=true',
    'driver': 'com.mysql.cj.jdbc.Driver',
    'user': 'myuser',
    'password': 'pass123'
}

# Logging Mechanism
log_entries = []


def log_event(event_type, message, table=None):
    log_entry = {
        "timestamp": datetime.datetime.now().isoformat(),
        "event_type": event_type,
        "message": message,
        "table": table
    }
    log_entries.append(log_entry)
    print(f"[{log_entry['timestamp']}] {event_type} - {message}")


def save_logs_to_gcs():
    log_filename = f"pipeline_log_{datetime.datetime.now().strftime('%Y%m%d%H%M%S')}.json"
    log_filepath = f"temp/pipeline_logs/{log_filename}"

    json_data = json.dumps(log_entries, indent=4)
    bucket = storage_client.bucket(GCS_Bucket)
    blob = bucket.blob(log_filepath)
    blob.upload_from_string(json_data, content_type="application/json")
    print(f"✅ Logs successfully saved to GCS at gs://{GCS_Bucket}/{log_filepath}")


def save_logs_to_bigquery():
    if log_entries:
        log_df = spark.createDataFrame(log_entries)
        log_df.write.format("bigquery") \
            .option("table", BQ_Log_Table) \
            .option("temporaryGcsBucket", GCS_Bucket) \
            .mode("append") \
            .save()
        print("✅ Logs stored in BigQuery for future analysis")


def move_existing_files_to_archive(table):
    blobs = list(storage_client.bucket(GCS_Bucket).list_blobs(prefix=f"landing/{Hospital_name}/{table}/"))
    existing_files = [blob.name for blob in blobs if blob.name.endswith(".json")]

    if not existing_files:
        log_event("INFO", f"No existing files for table {table}")
        return

    for file in existing_files:
        source_blob = storage_client.bucket(GCS_Bucket).blob(file)
        date_part = file.split("_")[-1].split(".")[0]
        year, month, day = date_part[-4:], date_part[2:4], date_part[:2]
        archive_path = f"landing/{Hospital_name}/archive/{table}/{year}/{month}/{day}/{file.split('/')[-1]}"
        destination_blob = storage_client.bucket(GCS_Bucket).blob(archive_path)
        storage_client.bucket(GCS_Bucket).copy_blob(source_blob, storage_client.bucket(GCS_Bucket),
                                                    destination_blob.name)
        source_blob.delete()
        log_event("INFO", f"Moved {file} to {archive_path}", table=table)


def get_latest_watermark(table_name):
    query = f"""
        SELECT MAX(load_timestamp) AS latest_timestamp
        FROM `{BQ_Audit_Table}`
        WHERE tablename = '{table_name}' and data_source = "hospital-a-db"
    """
    query_job = bq_client.query(query)
    result = query_job.result()
    for row in result:
        return row.latest_timestamp if row.latest_timestamp else "1900-01-01 00:00:00"
    return "1900-01-01 00:00:00"


def extract_and_save_to_landing(table, load_type, watermark_col):
    try:
        last_watermark = get_latest_watermark(table) if load_type.lower() == "incremental" else None
        log_event("INFO", f"Latest watermark for {table}: {last_watermark}", table=table)

        query = f"(SELECT * FROM {table}) AS t" if load_type.lower() == "full" else \
            f"(SELECT * FROM {table} WHERE {watermark_col} > '{last_watermark}') AS t"

        df = (spark.read.format("jdbc")
              .option("url", MYSQL_Config["url"])
              .option("user", MYSQL_Config["user"])
              .option("password", MYSQL_Config["password"])
              .option("driver", MYSQL_Config["driver"])
              .option("dbtable", query)
              .load())

        log_event("SUCCESS", f"✅ Successfully extracted data from {table}", table=table)

        today = datetime.datetime.today().strftime('%d%m%Y')
        json_file_path = f"landing/{Hospital_name}/{table}/{table}_{today}.json"
        bucket = storage_client.bucket(GCS_Bucket)
        blob = bucket.blob(json_file_path)
        blob.upload_from_string(df.toPandas().to_json(orient="records", lines=True), content_type="application/json")
        log_event("SUCCESS", f"✅ JSON file successfully written to gs://{GCS_Bucket}/{json_file_path}", table=table)

        audit_df = spark.createDataFrame([
            ("hospital-a-db", table, load_type, df.count(), datetime.datetime.now(), "SUCCESS")],
            ["data_source", "tablename", "load_type", "record_count", "load_timestamp", "status"])

        audit_df.write.format("bigquery") \
            .option("table", BQ_Audit_Table) \
            .option("temporaryGcsBucket", GCS_Bucket) \
            .mode("append") \
            .save()
        log_event("SUCCESS", f"✅ Audit log updated for {table}", table=table)

    except Exception as e:
        log_event("ERROR", f"Error processing {table}: {str(e)}", table=table)


def read_config_file():
    df = spark.read.csv(Config_File_path, header=True)
    log_event('info', '✅ Successfully read config file')
    return df


# Main Execution
config_df = read_config_file()

for row in config_df.collect():
    if row["is_active"] == '1' and row["datasource"] == "hospital-a-db":
        db, src, table, load_type, watermark, _, targetpath = row
        move_existing_files_to_archive(table)
        extract_and_save_to_landing(table, load_type, watermark)

save_logs_to_gcs()
save_logs_to_bigquery()
