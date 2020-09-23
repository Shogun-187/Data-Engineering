# Import libraries:
import os
import re
from google.cloud import storage

def GCS_Download(keys, blob_url):

  """ Download a BLOB from GCS to current directory and 
  returns the name of the new file. """

  # Auth GCP account: 
  os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = keys
  # Build storage client: 
  storage_client = storage.Client()

  # Select bucket: 
  blob_name = re.search('(?<=.com/).*', blob_url).group()
  bucket_name = blob_name.split('/')[0]
  bucket = storage_client.bucket(bucket_name)

  # Select BLOB:
  blob_path = ''
  for i in blob_name.split('/')[1:]:
    blob_path = blob_path + '/' + i
  blob = bucket.blob(blob_path[1:])

  # Download file to local directory: 
  new_file = os.path.basename(blob_name)
  blob.download_to_filename(new_file)
  print('Download Complete')
  return new_file
