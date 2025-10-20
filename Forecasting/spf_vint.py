import requests
import pandas as pd
import numpy as np
from datetime import datetime

def get_all_vintages(series_id, api_key):
    """Pull all available vintages for a FRED series in one call"""
    obs_url = "https://api.stlouisfed.org/fred/series/observations"
    params = {
        'series_id': series_id,
        'api_key': api_key,
        'file_type': 'json',
        'realtime_start': '1776-07-04',
        'realtime_end': '9999-12-31'
    }
    
    response = requests.get(obs_url, params=params)
    
    if response.status_code != 200:
        print(f"Error: {response.status_code}")
        return pd.DataFrame()
    
    data = response.json()
    
    if 'observations' not in data:
        print(f"No observations found")
        return pd.DataFrame()
    
    all_data = []
    for obs in data['observations']:
        all_data.append({
            'date': obs['date'],
            'value': obs['value'],
            'vintage_date': obs['realtime_start']
        })
    
    return pd.DataFrame(all_data)

api_key = "YOUR_KEY_HERE"
series_ids = ["GNPC96","GDPC1"]

all_series_data = {}
for series_id in series_ids:
    print(f"Fetching {series_id}...")
    df = get_all_vintages(series_id, api_key)
    df['value'] = pd.to_numeric(df['value'], errors='coerce')
    df['date'] = pd.to_datetime(df['date'])
    df['vintage_date'] = pd.to_datetime(df['vintage_date'])
    df = df.dropna(subset=['value'])
    all_series_data[series_id] = df

# Download SPF data
spf_url = "https://www.philadelphiafed.org/-/media/FRBP/Assets/Surveys-And-Data/survey-of-professional-forecasters/data-files/files/Individual_RGDP.xlsx?sc_lang=en&hash=5D704094EC8A5F6A1E12D87808753982"
spf_df = pd.read_excel(spf_url)
spf_df = spf_df[['YEAR', 'QUARTER', 'ID', 'RGDP1']]

#spf_df = spf_df[spf_df['YEAR'] >= 1974].copy()
spf_df = spf_df.replace('#N/A', np.nan)
spf_df['RGDP1'] = pd.to_numeric(spf_df['RGDP1'].astype(str).str.replace(',', ''), errors='coerce')

# Best guess for lagged annual data will be 
# vintage most people are using.So find vintage by
# mode of RGDP1 for each YEAR+QUARTER
modes = spf_df.groupby(['YEAR', 'QUARTER'])['RGDP1'].agg(
    lambda x: x.mode()[0] if len(x.mode()) > 0 else np.nan
).reset_index()
modes.columns = ['YEAR', 'QUARTER', 'MODE_RGDP1']

modes['lagged_year'] = modes['YEAR'] - (modes['QUARTER'] == 1).astype(int)
modes['lagged_quarter'] = modes['QUARTER'].apply(lambda q: 4 if q == 1 else q - 1)
modes['fred_date'] = pd.to_datetime(
    modes['lagged_year'].astype(str) + '-' + 
    ((modes['lagged_quarter'] - 1) * 3 + 1).astype(str) + '-01'
)

spf_df['RGDPL'] = 0.0

for idx, row in modes.iterrows():
    mode_gdp = row['MODE_RGDP1']
    fred_date = row['fred_date']
    year = row['YEAR']
    quarter = row['QUARTER']
    prev_year = year - 1
    
    # Using correct series 
    if year < 1992:
        gdp_df = all_series_data['GNPC96']
    else:
        gdp_df = all_series_data['GDPC1']

    if pd.isna(mode_gdp):
        continue
    
    gdp_vintages = gdp_df[gdp_df['date'] == fred_date].sort_values('vintage_date')
    
    if len(gdp_vintages) == 0:
        continue
    
    # Note from FRED team: if no change in data, missing value for vintage
    # Means this loop will be a bit cumbersome 
    for _, vintage_row in gdp_vintages.iterrows():
        if abs(vintage_row['value'] - mode_gdp) <= 1:
            vintage = vintage_row['vintage_date']
            
            prev_year_rgdps = []
            for q in range(1, 5):
                q_date = pd.to_datetime(f"{prev_year}-{(q-1)*3+1}-01")
                
                gdp_val = gdp_df[(gdp_df['date'] == q_date) & 
                                 (gdp_df['vintage_date'] == vintage)]['value'].values
                
                if len(gdp_val) == 0:
                    gdp_val = gdp_df[(gdp_df['date'] == q_date) & 
                                     (gdp_df['vintage_date'] <= vintage)].sort_values('vintage_date', ascending=False)['value'].values
                if len(gdp_val) > 0:
                    prev_year_rgdps.append(gdp_val[0])
            
            rgdpl = np.mean(prev_year_rgdps)
            mask = (spf_df['YEAR'] == year) & (spf_df['QUARTER'] == quarter)
            spf_df.loc[mask, 'RGDPL'] = rgdpl
            break
