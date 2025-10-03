import requests
import pandas as pd
import numpy as np
import time
import pandas_datareader.data as web
from datetime import datetime

def fetch_series_data(series_id, api_key):
    """Fetch all vintage data for a FRED series"""
    # Get vintage dates
    vintage_resp = requests.get(
        "https://api.stlouisfed.org/fred/series/vintagedates",
        params={'series_id': series_id, 'api_key': api_key, 'file_type': 'json'}
    )
    vintages = vintage_resp.json()['vintage_dates']
    
    # Fetch observations for each vintage
    records = []
    for vintage in vintages:
        obs_resp = requests.get(
            "https://api.stlouisfed.org/fred/series/observations",
            params={
                'series_id': series_id,
                'api_key': api_key,
                'file_type': 'json',
                'vintage_dates': vintage
            }
        )
        for obs in obs_resp.json()['observations']:
            if obs['value'] != '.':
                records.append({
                    'vintage': pd.Timestamp(vintage),
                    'obs_date': pd.Timestamp(obs['date']),
                    'value': float(obs['value'])
                })
    
    return pd.DataFrame(records)

# Fetch data
api_key = "API_KEY_HERE"
series_ids = ["UNRATEMD", "GDPC1MD", "PCECTPIMD", "FEDTARMD"]

data = {}
for i, sid in enumerate(series_ids):
    print(f"Fetching {sid}...")
    data[sid] = fetch_series_data(sid, api_key)
    if i < len(series_ids) - 1:
        time.sleep(11)

# Build output dataframe
all_vintages = sorted(set().union(*[set(df['vintage']) for df in data.values()]))
rows = []

for vintage in all_vintages:
    row = {'date': vintage.replace(day=1)}
    
    for sid in series_ids:
        df = data[sid]
        vintage_obs = df[df['vintage'] == vintage]
        
        if not vintage_obs.empty:
            vintage_obs = vintage_obs.copy()
            vintage_obs['year_ahead'] = vintage_obs['obs_date'].dt.year - vintage.year
            
            for ahead in [0, 1, 2, 3]:
                match = vintage_obs[vintage_obs['year_ahead'] == ahead]
                if not match.empty:
                    row[f'{sid}{ahead}'] = match.iloc[-1]['value']
    
    rows.append(row)

df = pd.DataFrame(rows)

# Fill missing columns
for sid in series_ids:
    for ahead in [0, 1, 2, 3]:
        col = f'{sid}{ahead}'
        if col not in df.columns:
            df[col] = np.nan
df = df.ffill()
# make sure didn't fill where we shouldn't 
df['date'] = pd.to_datetime(df['date'])
df.loc[df['date'].dt.month.isin([3, 6]), df.columns[df.columns.str.endswith('3')]] = np.nan

# Load SEP2015.csv
# This will extend the data 
sep2015_url = 'https://raw.githubusercontent.com/paulbousquet/SEPData/main/SEP2015.csv'
sep2015_df = pd.read_csv(sep2015_url)
sep2015_df['date'] = pd.to_datetime(sep2015_df['date'])

overlapping_dates = df['date'].isin(sep2015_df['date'])

df_no_overlap = df[~overlapping_dates]

final_df = pd.concat([df_no_overlap, sep2015_df], ignore_index=True)

final_df = final_df.sort_values('date').reset_index(drop=True)

df = web.get_data_fred('DFEDTARU', start='2011-01-01', end=datetime.now())

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
fedtarmdlr = web.get_data_fred('FEDTARMDLR', start='2011-01-01', end=datetime.now())

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
final_df['DFFR'] = final_df['DFFR'].fillna(0)
final_df.to_csv('SEP_full.csv', index=False)
