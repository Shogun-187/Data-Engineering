# pip install these if you haven't:
# %pip install python-Levenshtein
# %pip install fuzzywuzzy

# Import libraries
import pandas as pd
from datetime import datetime
from fuzzywuzzy import fuzz
from fuzzywuzzy import process

def Fuzzy_Lookup(df_1, df_2, key1, key2, threshold=90, limit=1):

  """ This function performs a fuzzy lookup on a column of a df.
  It finds matches from a second df based on a threshhold from
  0-100 set by the user. 
  The limit parameter is used to select how many results you are 
  looking for on the second df. 
  The output is a list with 2 elements. The 1st element is a joined
  df built from the 2 input df. That joined df contains
  all the matches and non-matching elements. 
  The second output is a df only containing the data points that 
  did not have a match during the lookup function. """

  # Initialize parameters:
  print('Initializing fuzzy lookup script.\n' + '='*80)
  input_1 = df_1.copy()
  input_2 = df_2.copy()
  output = pd.DataFrame()
  ts = round(datetime.timestamp(datetime.now()))

  # Build a list of keys from the comparison df:
  ckeys = input_2[key2].tolist()

  # Compute fuzzy lookup comparison score:
  matches = input_1[key1].apply(
      lambda x: process.extract(x, ckeys, limit=limit)
      )  

  # Assign scores to a column in the 1st df:
  input_1['Matches'] = matches

  # Only include results above the required threshhold:
  filtered_matches = input_1['Matches'].apply(
    lambda x: ', '.join([i[0] for i in x if i[1] >= threshold])
    )
  
  # Update df with the filtered matches:
  input_1['Matches'] = filtered_matches

  # Replace empty cells with null values:
  input_1.replace('',np.NaN, inplace=True)

  # Create a new df with the final output:
  output = pd.concat([output, input_1], ignore_index=True)

  # Join input tables:
  joined_output = output.join(input_2.set_index(key2),
                          on='Matches',
                          rsuffix='_DF2').copy()

  # Create a df with the accounts with no matches:
  missing = output.loc[output.Matches.isna()].copy()                          

  # Print results summary: 
  matches_count = output.loc[output.Matches.notnull()].shape[0]
  print('Summary:')
  print(f'There are {matches_count} matches out of {output.shape[0]} total data points.')
  print(f'There are {missing.shape[0]} missing matches.')
  # Save results in a CSV file:
  csv_file = f'Fuzzy_Lookup_Results_{ts}.csv'
  joined_output.to_csv(csv_file)
  print(f'Saved results here: {csv_file}')
  print('='*80 + '\nEnd of script.')

  return [joined_output, missing]
