import pandas as pd
import os

# Load the file
file_path = r"C:\Users\drunk\Documents\3rd Year\Second Semester\CMPG_321\Project Data\Payment Lines (1).xlsx"
df = pd.read_excel(file_path)

# Strip whitespace from string columns
str_cols = df.select_dtypes(include='object').columns
for col in str_cols:
    df[col] = df[col].str.strip()

# Convert DEPOSIT_DATE to datetime and keep only the date
df['DEPOSIT_DATE'] = pd.to_datetime(df['DEPOSIT_DATE'], errors='coerce').dt.date

# Ensure numeric columns are numeric, without rounding
num_cols = ['BANK_AMT', 'DISCOUNT', 'TOT_PAYMENT']
for col in num_cols:
    df[col] = pd.to_numeric(df[col], errors='coerce')

# Optional: sort by CUSTOMER_NUMBER and DEPOSIT_DATE
df.sort_values(by=['CUSTOMER_NUMBER', 'DEPOSIT_DATE'], inplace=True)

# Reset index
df.reset_index(drop=True, inplace=True)

# Save to the existing 'cleaned' folder with CLEAN_ prefix
clean_folder = r"C:\Users\drunk\Documents\3rd Year\Second Semester\CMPG_321\Project Data\cleaned"

# Extract original file name
original_name = os.path.basename(file_path)
clean_file_name = "CLEAN_" + original_name
clean_file_path = os.path.join(clean_folder, clean_file_name)

df.to_excel(clean_file_path, index=False)

print(f"Cleaned data saved to: {clean_file_path}")
