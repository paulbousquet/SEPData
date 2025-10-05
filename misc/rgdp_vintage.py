import requests
import pandas as pd
import numpy as np
import time

def fetch_targeted_vintages(series_id, api_key, start_year=1990):
    """Fetch only the vintages needed for quarterly analysis"""
    # Get all available vintage dates
    vintage_resp = requests.get(
        "https://api.stlouisfed.org/fred/series/vintagedates",
        params={'series_id': series_id, 'api_key': api_key, 'file_type': 'json'}
    )
    all_vintages = pd.to_datetime(vintage_resp.json()['vintage_dates'])
    
    # Generate target quarters
    current_quarter = pd.Timestamp.now().to_period('Q')
    quarters = pd.period_range(f'{start_year}Q1', current_quarter, freq='Q')
    
    # For each quarter, find vintage in first month (Jan=1, Apr=4, Jul=7, Oct=10)
    target_vintages = []
    for quarter in quarters:
        first_month = quarter.to_timestamp().month
        year = quarter.to_timestamp().year
        
        # Filter vintages to this year and month
        matching = all_vintages[(all_vintages.year == year) & (all_vintages.month == first_month)]
        if len(matching) > 0:
            target_vintages.append(matching[0].strftime('%Y-%m-%d'))
    
    # Fetch only these vintages
    records = []
    i=1
    for vintage in target_vintages:
        if(i==120):
            time.sleep(11)
        i+=1
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
        time.sleep(0.1)
    
    return pd.DataFrame(records)

def create_quarterly_dataset(df, start_year=1991):
    """Create dataset with lagged GDP and prior year average"""
    results = []
    
    current_quarter = pd.Timestamp.now().to_period('Q')
    quarters = pd.period_range(f'{start_year}Q1', current_quarter, freq='Q')
    
    for quarter in quarters:
        first_month = quarter.to_timestamp()
        vintage_date = df[df['vintage'].dt.to_period('M') == first_month.to_period('M')]['vintage'].unique()
        
        if len(vintage_date) == 0:
            continue
        vintage_date = vintage_date[0]
        
        vintage_df = df[df['vintage'] == vintage_date].copy()
        vintage_df['quarter'] = vintage_df['obs_date'].dt.to_period('Q')
        vintage_df = vintage_df.groupby('quarter')['value'].first()
        
        lag_quarter = quarter - 1
        lagged_gdp = vintage_df.get(lag_quarter, np.nan)
        
        prior_year = quarter.year - 1
        prior_year_quarters = [pd.Period(f'{prior_year}Q{q}') for q in [1, 2, 3, 4]]
        prior_year_values = [vintage_df.get(q, np.nan) for q in prior_year_quarters]
        
        # Only compute average if ALL four quarters are present
        if all(~np.isnan(prior_year_values)):
            avg_prior_year = np.mean(prior_year_values)
        else:
            avg_prior_year = np.nan
        
        results.append({
            'quarter': quarter,
            'vintage_date': vintage_date,
            'lagged_gdp': lagged_gdp,
            'avg_prior_year_gdp': avg_prior_year
        })
    
    return pd.DataFrame(results)


# Usage
api_key = 'YOUR_KEY_HERE'
gdp_data = fetch_targeted_vintages('GDPC1', api_key)
quarterly_data = create_quarterly_dataset(gdp_data)
