# Import libraries:
import os
import pandas as pd
import pandas_gbq
from pytz import timezone
from datetime import datetime
from google.cloud import bigquery

def BigQuery_Upload(
    df,
    keys,
    upload_dataset_name='upload_dataset_name',
    target_table_name='target_table_name',
    backup=True,
    backup_dataset_name='backup_dataset_name', 
    ):

  """ Upload a dataframe to Bigquery. Option to backup table to be updated
  first in case you need to keep previous states available."""

  # Initialize parameters:
  tz = timezone('America/Costa_Rica') 
  ts = datetime.now(tz).strftime("%m%d%Y_%H%M")
  upload_dataset_name = upload_dataset_name
  upload_table_name = target_table_name
  backup_dataset_name = backup_dataset_name
  backup_table_name = f'{target_table_name}_{ts}'

  # Set GCP keys to env variable:
  os.environ["GOOGLE_APPLICATION_CREDENTIALS"]=keys
  # Build a BigQuery client object.
  client = bigquery.Client()

  # Build backup and load it to BigQuery:

  if backup:

    try:

      print('Building backup...')
      # Build identifiers for BQ tables:
      upload_table_id = f'{upload_dataset_name}.{target_table_name}'
      backup_table_id = f'{backup_dataset_name}.{backup_table_name}'    

      # Delete backup table if already exists:
      client.delete_table(backup_table_id, not_found_ok=True)
      # Load new back up table:
      job = client.copy_table(upload_table_id, backup_table_id)
      job.result()
      backup_completed = True
      print(f'Created backup for: {upload_table_id}')
      print(f'Stored copy in {backup_table_id}')

    except Exception as e:

      # Print exception:
      print(f'Exception cccurred while uploading backups. : {e}')
      return None

  else:

    print('Backup not required.')

  # Upload table to BigQuery:

  try:

    # Post dataframe to BigQuery:
    pandas_gbq.to_gbq(
        df,
        f'{upload_dataset_name}.{upload_table_name}',
        if_exists='replace')
    
    print(f'\nUpdated {upload_dataset_name}.{upload_table_name} successfully.')

  except Exception as e:

    # Print exception:
    print(f'Exception occured while uploading df to BigQuery. : {e}')

  return None
