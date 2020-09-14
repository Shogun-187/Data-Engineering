# Import libraries:
import requests
import random
from itertools import cycle

def Get_Private_Proxies(Proxy_List_URL):

  """ This function fetches proxies from a Webshare account. It outputs
  a set with IP addresses to connect to a proxy server. You have to pass
  the link provided from the Proxy List API.
  Every proxy is formatted to be used within the requests library under
  the proxies parameter."""

  # Download Proxy List from the API:
  response = requests.get(Proxy_List_URL).text.split()
  # Initialize an empty list to store parsed results: 
  proxies = []

  for proxy in response:

    # Extract elements for each proxy:
    split_proxy = proxy.split(':')
    # Append them to our results list and apply proper format:
    proxies.append(
        'http://{}:{}@{}:{}/'.format(
            split_proxy[2],
            split_proxy[3],
            split_proxy[0],
            split_proxy[1]
            )
        )

  # Suffle proxies and assign them to a set:  
  random.shuffle(proxies)  
  set_proxies = set(proxies)
  # Create an iterable object:
  proxy_cycle = cycle(set_proxies)
  print('Fetched Proxies Successfully.')
    
  return proxy_cycle
