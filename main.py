import os
import time
import json
from flask import Flask, jsonify, request
import pandas as pd

from google.cloud import storage, bigquery, secretmanager
from google.oauth2 import service_account
import logging

logging.basicConfig(filename='Ingestion.log', level=logging.DEBUG, format='%(asctime)s - %(message)s',
                    datefmt='%d-%b-%y %H:%M:%S')

app = Flask(__name__)
# ________________

# Google Cloud Project ID
project_id = 'fivetran-408613'
secret_name = 'CRun_sec'
client_sm = secretmanager.SecretManagerServiceClient()

# Access the secret
name = f"projects/{project_id}/secrets/{secret_name}/versions/latest"
response = client_sm.access_secret_version(request={"name": name})

# Extract the secret value (JSON string)
secret_value_json = response.payload.data.decode("UTF-8")

# Load the JSON string into a dictionary
secret_value_dict = json.loads(secret_value_json)

# Log the retrieved credentials
# print("Retrieved credentials from Secret Manager: %s", secret_value_dict)

# Use the dictionary to create credentials
credentials = service_account.Credentials.from_service_account_info(
    secret_value_dict
)

# Use the content to initialize the clients
storage_client = storage.Client(credentials=credentials, project=credentials.project_id)
bigquery_client = bigquery.Client(credentials=credentials, project=credentials.project_id)
project_id = credentials.project_id


# ________________


def load_json_to_dataframe(file_content):
    # Create an empty list to store the records
    records = []

    # Read the file line by line, each line is a JSON object
    for line in file_content.splitlines():
        if line.strip():  # Check if the line is not empty
            try:
                json_obj = json.loads(line)  # Load the JSON object
                # Check if the required fields are present and the change_type is 'DELETE'
                if 'payload' in json_obj:
                    record = {
                        'change_type': json_obj['source_metadata']['change_type'],
                        'source_timestamp': json_obj['source_timestamp'],
                        **json_obj['payload']
                    }
                    records.append(record)  # Append the record to the list
            except json.JSONDecodeError as e:
                logging.error(f"Failed to decode JSON in line: {line}, Error: {str(e)}")

    # Convert the records list to a DataFrame
    if records:
        df = pd.DataFrame(records)  # Convert the records list to a DataFrame
        return df
    else:
        logging.warning("No valid records found in the JSONL file.")
        return pd.DataFrame()  # Return an empty DataFrame if no records found


def create_or_append_table(blob, table_ref, df, bucket_name):
    logging.info("-------------- Creating or Appending to Table -----------------")

    job_config = bigquery.LoadJobConfig(
        schema=[bigquery.SchemaField(col, "STRING") for col in df.columns],
        source_format=bigquery.SourceFormat.CSV,
        write_disposition=bigquery.WriteDisposition.WRITE_APPEND  # Append if table exists
    )

    job = bigquery_client.load_table_from_dataframe(df, table_ref, job_config=job_config)
    logging.warning(f"File {blob.name} successfully ingested into BigQuery!")
    job.result()


def process_blob(bucket, bucket_name, blob, dataset_ref, table_name):
    file_content = blob.download_as_text()

    # Check if the file content is empty
    if not file_content.strip():
        logging.warning(f"Skipping empty JSON file: {blob.name}")
        move_blob_to_folder(bucket, blob, "Error")
        return

    df = load_json_to_dataframe(file_content)
    logging.info(f"Blob DataFrame: {df}")
    table_ref = dataset_ref.table(table_name)

    create_or_append_table(bucket, table_ref, df, bucket_name)


def create_folders(bucket):
    """
    Creates Archive/Error/Streaming folders if they do not exist in the bucket.
    """
    archive_folder = bucket.blob(f"Archive/")
    if not archive_folder.exists():
        archive_folder.upload_from_string('')
        logging.info(" Folder Archive created")

    error_folder = bucket.blob(f"Error/")
    if not error_folder.exists():
        error_folder.upload_from_string('')
        logging.info(" Folder Error created")


def move_blob_to_folder(bucket, blob, folder_name):
    """
    Moves a blob from the specified bucket to an archive/error folder within the same bucket.
    """
    try:
        source_blob = blob
        destination_blob = bucket.blob(f"{folder_name}/{blob.name}")
        destination_blob.upload_from_string(source_blob.download_as_string())
        source_blob.delete()

        logging.warning(f"Blob '{blob.name}' successfully moved to '{folder_name}' folder.")
    except Exception as e:
        logging.error(f"Error moving blob '{blob.name}': {e}")


def process_blob_with_error_handling(bucket, bucket_name, blob, dataset_ref, table_name):
    """
    Process the blob and then move it to archive if successful, else move to error.
    """
    try:
        logging.info("-------------- Start Processing Blob -----------------")
        process_blob(bucket, bucket_name, blob, dataset_ref, table_name)
        if not blob.exists():
            logging.error(f"Blob | {blob} | not found or moved already to error folder")
        else:
            move_blob_to_folder(bucket, blob, "Archive")
            logging.info(f"Archiving after processing with error handling: {blob}")
    except Exception as e:
        move_blob_to_folder(bucket, blob, "Error")
        logging.info(f"Blob | {blob} | moved to ERROR after processing with error handling")
        raise


@app.route('/ingest_json', methods=['POST'])
def ingest_json_to_bigquery():
    start_time = time.time()

    try:
        data_config = request.json
        logging.info(f"Request received to /ingest_json endpoint")
        dataset_id = data_config.get('dataset')
        dataset_ref = bigquery.DatasetReference(project_id, dataset_id)
        bucket_name = data_config.get('bucket_name')
        bucket = storage_client.bucket(bucket_name)

        logging.info(
            f" Data extracted from the Request : bucket : {bucket}:{bucket_name}, Dataset reference: {dataset_ref}")
        logging.info("-------------- END of variables ----------------")

        create_folders(bucket)
        logging.info("-------------- Folders Created -----------------")

        for blob in bucket.list_blobs():
            logging.info(f"Looping over Blobs | blob path: {blob.name}")

            # Exclude blobs in specific folders directly
            if blob.name.startswith("Error/") or blob.name.startswith("Archive/"):
                continue
            if not blob.name.endswith('.jsonl'):
                logging.warning(f"Skipping non-JSON file: {blob.name}")
                move_blob_to_folder(bucket, blob, "Error")
                continue  # Skip non-JSON files

            parts = os.path.splitext(os.path.basename(blob.name))[0].rsplit('_')
            table_name = parts[1]
            logging.info(f"Table name from Blob: {table_name}")

            logging.info(f"Processing blob: {blob.name}")
            process_blob_with_error_handling(bucket, bucket_name, blob, dataset_ref, table_name)

        total_time = time.time() - start_time
        logging.info(f"Total execution time: {total_time:.2f} seconds")
        return jsonify({'message': 'JSON files ingested into BigQuery tables successfully!'})

    except Exception as e:
        return jsonify({'error': str(e)})


if __name__ == "__main__":
    app.run(debug=True, port=5000)