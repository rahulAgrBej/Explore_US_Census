

import requests
import pprint
import pandas as pd

pp = pprint.PrettyPrinter(indent=3)

# GET US CENSUS API KEY
f = open('us_census_api_key.txt', 'r')
acs_api_key = f.read().rstrip('\n')
f.close()

# Get a list of all state FIPS
states_fips = list(pd.read_csv('data/census/fips_states_2010.csv', dtype={0:str})['fips'])

# General format for ACS data retrieval for all census tracts in a state
ACS_BASE_URL = 'https://api.census.gov/data/2019/acs/acs5?get=NAME,'
ACS_SUBJECT_URL = 'https://api.census.gov/data/2019/acs/acs5/subject?get=NAME,'
ACS_TRACTS = '&for=tract:*&in=state:'
ACS_KEY = '&key=' + acs_api_key

# Specific variables being queried
census_vars = {
   'B28002_001E': 'internet_access',
   'S1701_C01_001E': 'poverty_rate',
   'C17002_001E': 'ratio_income_poverty',
   'S0102_C01_064E': 'lang_other_eng',
   'B25035_001E': 'median_yr_structure_built'
}

# build url and send api request
result = None
for k, v in census_vars.items():
   var_num = k
   var_name = v
   print(f'Query for {var_name} {var_num}')
   result_var = None
   for fip in states_fips:
      if var_num[0] == 'S':
         base_url = ACS_SUBJECT_URL
      else:
         base_url = ACS_BASE_URL
      
      send_url = base_url + var_num + ACS_TRACTS + fip + ACS_KEY
      resp = requests.get(send_url)
      print(resp.status_code)
      if resp.status_code == 200:
         dat = resp.json()
         df = pd.DataFrame(dat[1:], columns = dat[0])
         # create a GEOID column
         df['GEOID'] = df['state'].astype(str) + df['county'].astype(str) + df['tract'].astype(str)
         result_var = pd.concat([result_var, df])
      else:
         print(resp.content)
   
   if not (result_var is None):
      if not (result is None):
         result = pd.merge(result, result_var[[var_num, 'GEOID']], on='GEOID')
      else:
         result = result_var


result = result.rename(columns=census_vars)
print('Writing to file')
result.to_csv('data/acs_2019.csv', index=False)