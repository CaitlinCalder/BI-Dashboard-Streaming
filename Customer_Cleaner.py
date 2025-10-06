import pandas as pd
import os

# Input/output paths
input_file = r"C:\Users\drunk\Documents\3rd Year\Second Semester\CMPG_321\Project Data\Customer.xlsx"
output_folder = r"C:\Users\drunk\Documents\3rd Year\Second Semester\CMPG_321\Project Data\cleaned"
output_file = os.path.join(output_folder, "CLEAN_Customer.xlsx")

os.makedirs(output_folder, exist_ok=True)

# Read Excel
df = pd.read_excel(input_file, dtype=str)

# Strip whitespace
df = df.applymap(lambda x: x.strip() if isinstance(x, str) else x)

# Title-case textual fields (except codes)
text_columns = ['CUSTOMER_NUMBER', 'CCAT_CODE', 'REGION_CODE', 'REP_CODE']  # codes stay as-is
for col in df.columns:
    if col not in text_columns and df[col].dtype == object:
        df[col] = df[col].str.title()

# Standardize placeholder codes in CUSTOMER_NUMBER, REGION_CODE, CCAT_CODE
placeholder_codes = [
    'ZZZ', 'CONS', 'STAND', 'CON3S', 'CON4S', 'CON5', 'CONS2', 
    'CON4', 'CON2S', 'STAN2', '01EXP', '04C', '05C', '06C', '04MA*', '02C'
]
for col in ['CUSTOMER_NUMBER', 'REGION_CODE', 'CCAT_CODE']:
    df[col] = df[col].replace({code: 'Unknown' for code in placeholder_codes})

# Ensure numeric columns are numeric
numeric_columns = ['SETTLE_TERMS', 'NORMAL_PAYTERMS', 'DISCOUNT', 'CREDIT_LIMIT']
for col in numeric_columns:
    if col in df.columns:
        df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0)

# Save cleaned file
df.to_excel(output_file, index=False)

print(f"✅ Cleaned Excel file saved → {output_file}")
