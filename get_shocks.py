import pandas as pd
import numpy as np
from sklearn.linear_model import LinearRegression
data_url = 'https://raw.githubusercontent.com/paulbousquet/SEPData/main/SEP_full.csv'

df = pd.read_csv(data_url)
# Process the dataframe
df['date'] = pd.to_datetime(df['date'])
df = df.sort_values('date').reset_index(drop=True)

# Identify March 1st dates (first month of quarter)
df['is_march'] = (df['date'].dt.month == 3) & (df['date'].dt.day == 1)

# Define prefixes and horizons (ONLY 0, 1, 2 - NOT 3!)
prefixes = ['UNRATEMD', 'GDPC1MD', 'PCECTPIMD']
horizons = [0, 1, 2]

# Create new columns for processed variables
processed_df = df.copy()

# Create lag and revision columns
for prefix in prefixes:
    for h in horizons:
        var = f'{prefix}{h}'
        if var in df.columns:
            # Create lag - just shift by 1 period
            processed_df[f'L.{var}'] = df[var].shift(1)
            
            # Create revision
            if h == 0:
                # Special handling for H=0 on March 1st
                revision_normal = df[var] - df[var].shift(1)
                revision_march = df[var] - df[f'{prefix}1'].shift(1)
                processed_df[f'D.{var}'] = np.where(
                    df['is_march'],
                    revision_march,
                    revision_normal
                )
            else:
                # Standard revision for other horizons
                processed_df[f'D.{var}'] = df[var] - df[var].shift(1)

# Process FEDTARMDLR
if 'FEDTARMDLR' in df.columns:
    processed_df['L.FEDTARMDLR'] = df['FEDTARMDLR'].shift(1)
    processed_df['D.FEDTARMDLR'] = df['FEDTARMDLR'] - df['FEDTARMDLR'].shift(1)

# Drop helper column and H=3 columns
cols_to_drop = ['is_march'] + [col for col in processed_df.columns if col.endswith('3')]
processed_df = processed_df.drop(cols_to_drop, axis=1)

# Filter dates
processed_df = processed_df[(processed_df['date'] >= '2015-09-01') & (processed_df['date'].dt.year != 2020)]

# Prepare for regression
X_cols = [col for col in processed_df.columns if col not in ['DFFR', 'date', 'UNRATEMD1', 'UNRATEMD2']]

# For regression, drop rows with NaN in any X columns or y
regression_data = processed_df[['DFFR'] + X_cols].dropna()

X = regression_data[X_cols]
y = regression_data['DFFR']

# Run regression
reg = LinearRegression()
reg.fit(X, y)

# Calculate residuals
residuals = y - reg.predict(X)

# Create shocks dataframe
shocks = pd.DataFrame({
    'date': processed_df.loc[regression_data.index, 'date'],
    'residuals': residuals.values
})
new_rows = pd.DataFrame({
    'date': pd.to_datetime(['2020-03-01', '2020-06-01', '2020-09-01', '2020-12-01']),
    'residuals': 0
})

# Concatenate with existing data and sort
shocks = pd.concat([shocks, new_rows], ignore_index=True)
shocks = shocks.sort_values('date').reset_index(drop=True)

shocks.to_csv('shocks.csv', index=False)

