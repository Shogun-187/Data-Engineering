# Import libraries:
import pandas as pd
import geopy
from geopy.geocoders import Nominatim
from time import sleep

def Geopy_Fetch_LatLon(Queries):

  """ This function takes a list of queries in the format of
  'city, state, zip code' and makes an API for each query to
  fetch lat and lon data using the GeoPy Nominatim API. """

  # Initialize parameters:    
  print('Initializing script.\n') 
  results = []
  cooldown = 1 # Seconds between each API request to avoid requests overload.
  retries = range(2) # Add retries in case of failure. 
  n = 1
  
  for query in Queries:

    for retry in retries:

      try:

        # Loop over each query and make an API request:
          print(f'Request {n} from {len(Queries)} total, Query: {query}')
          geolocator = Nominatim(user_agent="Triborg-187LK")
          location = geolocator.geocode(query) 

          # Save response in a dictionary:
          results.append(
              {
                  'Query':query,
                  'Address':location.address,
                  'Lat':location.latitude,
                  'Lon':location.longitude
              }
          )

          # Time out to avoid API overload:
          print(f'Fetched data successfully.\n')
          sleep(cooldown)
          n += 1
          break

      except Exception as e:

        # Raise exception and take time out:
        print(f'Exception raised: {e}.')    
        sleep(cooldown)
        n += 1            

  print('\nEnd of script.')  
  return pd.DataFrame(results)
  
