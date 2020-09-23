# Import libraries:
import os
from google.cloud import storage

# Download blobs from GCP:
def GCS_Upload(upload_file, keys, bucket_name, blob_path):

  # Auth GCP account: 
  os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = keys
  # Build storage client: 
  storage_client = storage.Client()
  # Upload data:
  bucket = storage_client.bucket(bucket_name)
  blob = bucket.blob(blob_path + os.path.basename(upload_file))
  blob.upload_from_filename(upload_file)
  print('Upload Complete.')
  return None
