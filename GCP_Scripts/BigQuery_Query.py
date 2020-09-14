# Import libraries:
import pandas as pd
import pandas_gbq
import os 

def BigQuery_Query(
    keys = None,
    dataset_name = None,
    table_name = None,
    project_id = None
    ):

  """
  Query an entire table for a given dataset. You have to pass the
  path to the json file with the service account keys for your GCP
  project to authenticate. Otherwise, it will ask you to authenticate
  your account using end user auth and you will be required to specify
  a project id\n.

  Params:

  keys : None or str. Path to the json file for your GCP service account.
  If None, it will try to infer the credentials from the env variables.

  dataset_name : str. Name of the BigQuery dataset. 

  table_name : str. Name of the BigQuery table. 

  project_id (optional) : str. Name of the project id for the GCP project.

  """

  if keys:

    # Set GCP keys to env variable:
    os.environ["GOOGLE_APPLICATION_CREDENTIALS"]=keys

    # Write a SQL query:
    SQL = """
          SELECT * 
          FROM `{}`
          """.format(dataset_name + '.' + table_name)

    # Execute query and save results in a pandas df:
    df = pandas_gbq.read_gbq(query=SQL)

  else:

    # Write a SQL query:
    SQL = """
          SELECT * 
          FROM `{}`
          """.format(dataset_name + '.' + table_name)

    # Execute query and save results in a pandas df:
    df = pandas_gbq.read_gbq(query=SQL, project_id=project_id)

  print(f'Imported {table_name}, Table Dimensions: {df.shape}')
  return df
