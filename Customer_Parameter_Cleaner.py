# customer_parameters.py

import pandas as pd

# Load the table
file_path = 'Customer Account Parameters.xlsx'  # adjust extension if needed

try:
    if file_path.endswith('.csv'):
        df = pd.read_csv(file_path)
    else:
        df = pd.read_excel(file_path)
except Exception as e:
    print(f"Error loading file: {e}")
    exit()

# Display the first few rows to check
print("Original data:")
print(df.head())

# Clean whitespace and standardize column names
df.columns = [col.strip() for col in df.columns]
df['CUSTOMER_NUMBER'] = df['CUSTOMER_NUMBER'].str.strip()
df['PARAMETER'] = df['PARAMETER'].str.strip()

# Optional: check unique parameters
print("\nUnique PARAMETERS:")
print(df['PARAMETER'].unique())

# Save cleaned table
clean_file = 'Customer_Account_Parameters_Clean.csv'
df.to_csv(clean_file, index=False)
print(f"\nCleaned table saved to {clean_file}")
