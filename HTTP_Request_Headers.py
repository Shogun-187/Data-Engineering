import requests
import os

def HTTP_Headers():

  """ Fetch HTTP Headers for User_Agent Rotation."""

  # File location:
  headers = 'https://storage.googleapis.com/python_files_0187/HTTP_Request_Headers.py'

  # Get data:
  response = requests.get(headers)

  # Save file:
  file_name = os.path.basename(headers)
  open(file_name, 'wb').write(response.content)

  # Import data:
  from HTTP_Request_Headers import headers_list
  print('Headers imported successfully.')
  return headers_list
