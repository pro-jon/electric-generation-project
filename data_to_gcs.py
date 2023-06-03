import pandas as pd
import os
from dotenv import load_dotenv
import io
from google.cloud import storage

load_dotenv()

init_data = '/home/jon/Projects/capstone/electric-generation-project/data/electric-generate-data.csv'
BUCKET = os.environ.get('GCP_GCS_BUCKET')
destination_blob_name = 'electricity_generation/electric-generate-data.parquet'


def data_to_parquet(init_data):
    """Convert CSV to Parquet and Upload to GCS"""

    # Convert CSV to Parquet
    df = pd.read_csv(init_data)
    init_data = init_data.replace('.csv', '.parquet')
    df.to_parquet(init_data, engine='pyarrow')
    print(f"Parquet: {init_data}")

    # Upload to GCS
    upload_blob(BUCKET, destination_blob_name, init_data)


def upload_blob(bucket, destination_blob_name, init_data):
    """Upload local data file to Google Cloud Storage as Parquet"""


    client = storage.Client()
    bucket = client.bucket(BUCKET)
    blob = bucket.blob(destination_blob_name)

    generation_match_precondition = 0

    blob.upload_from_filename(init_data, if_generation_match=generation_match_precondition)

    rint(
        f"File {init_data} uploaded to {destination_blob_name}."
    )


if __name__ == "__main__":
    data_to_parquet(init_data)



