import requests
import pandas as pd
import numpy as np
import pandas_datareader.data as web
from datetime import datetime

def get_all_vintages(series_id, api_key):
    """Pull all available vintages for a FRED series"""
    vintage_url = "https://api.stlouisfed.org/fred/series/vintagedates"
    vintage_params = {
        'series_id': series_id,
        'api_key': api_key,
        'file_type': 'json'
    }
    
    vintage_response = requests.get(vintage_url, params=vintage_params)
    response_data = vintage_response.json()
    
    if 'vintage_dates' not in response_data:
        return pd.DataFrame()
    
    vintage_dates = response_data['vintage_dates']
    all_data = []
    
    obs_url = "https://api.stlouisfed.org/fred/series/observations"
    
    for vintage_date in vintage_dates:
        params = {
            'series_id': series_id,
            'api_key': api_key,
            'file_type': 'json',
            'vintage_dates': vintage_date
        }
        
        response = requests.get(obs_url, params=params)
        data = response.json()
        
        if 'observations' in data:
            for obs in data['observations']:
                all_data.append({
                    'date': obs['date'],
                    'value': obs['value'],
                    'vintage_date': vintage_date
                })
    
    return pd.DataFrame(all_data)

# Configuration
api_key = "API_KEY_HERE"
series_ids = ["UNRATEMD", "GDPC1MD", "PCECTPIMD"]

# Collect all data
all_series_data = {}
for series_id in series_ids:
    print(f"Fetching {series_id}...")
    df = get_all_vintages(series_id, api_key)
    df['value'] = pd.to_numeric(df['value'], errors='coerce')
    df['date'] = pd.to_datetime(df['date'])
    df['vintage_date'] = pd.to_datetime(df['vintage_date'])
    df = df.dropna(subset=['value'])
    all_series_data[series_id] = df

# Merge into combined dataset
combined_rows = []

# Get unique vintage dates across all series
all_vintage_dates = set()
for df in all_series_data.values():
    all_vintage_dates.update(df['vintage_date'].unique())

for vintage_date in sorted(all_vintage_dates):
    # Change vintage_date to first day of month
    first_of_month = vintage_date.replace(day=1)
    row = {'date': first_of_month}
    
    for series_id in series_ids:
        df = all_series_data[series_id]
        
        # Filter to this vintage
        vintage_data = df[df['vintage_date'] == vintage_date].copy()
        
        if not vintage_data.empty:
            vintage_data['year_diff'] = (
                vintage_data['date'].dt.year - 
                vintage_data['vintage_date'].dt.year
            )   
            
            # Get values for 0, 1, and 2 year differences
            for diff in [0, 1, 2]:
                matching = vintage_data[vintage_data['year_diff'] == diff]
                if not matching.empty:
                    # Take the last observation if multiple exist
                    value = matching.iloc[-1]['value']
                    row[f'{series_id}{diff}'] = value
    
    combined_rows.append(row)

# Create final dataframe
combined_df = pd.DataFrame(combined_rows)

# Ensure all columns exist (fill missing with NaN)
expected_cols = ['date']
for series_id in series_ids:
    for diff in [0, 1, 2]:
        expected_cols.append(f'{series_id}{diff}')

for col in expected_cols:
    if col not in combined_df.columns:
        combined_df[col] = np.nan

# Reorder columns
df = combined_df[expected_cols]

# Load SEP2015.csv
# This will extend the data and correct two errors from FRED 
sep2015_url = 'https://raw.githubusercontent.com/paulbousquet/SEPData/main/SEP2015.csv'
sep2015_df = pd.read_csv(sep2015_url)
sep2015_df['date'] = pd.to_datetime(sep2015_df['date'])

overlapping_dates = df['date'].isin(sep2015_df['date'])

df_no_overlap = df[~overlapping_dates]

final_df = pd.concat([df_no_overlap, sep2015_df], ignore_index=True)

final_df = final_df.sort_values('date').reset_index(drop=True)

df = web.get_data_fred('DFEDTARU', start='2014-01-01', end=datetime.now())

# Aggregate to quarterly using end of period values
quarterly = df.resample('Q').last()

# Reset index to work with dates
quarterly = quarterly.reset_index()

# Map quarters to first month of quarter
quarterly['DATE'] = quarterly['DATE'].dt.to_period('Q').dt.start_time + pd.DateOffset(months=2)

# Set DATE back as index
quarterly = quarterly.set_index('DATE')

# Calculate quarterly change
quarterly['DFFR'] = quarterly['DFEDTARU'].diff()

# Import FEDTARMDLR
fedtarmdlr = web.get_data_fred('FEDTARMDLR', start='2015-01-01', end=datetime.now())

# Change date to first day of month
fedtarmdlr.index = fedtarmdlr.index.to_period('M').to_timestamp()

final_df = final_df.merge(
    quarterly[['DFFR']].reset_index(),
    left_on='date', 
    right_on='DATE', 
    how='left'
).drop('DATE', axis=1)

# Merge FEDTARMDLR (monthly data)
final_df = final_df.merge(
    fedtarmdlr.reset_index(),
    left_on='date',
    right_on='DATE',
    how='left'
).drop('DATE', axis=1)

final_df.to_csv('SEP_full.csv', index=False)
