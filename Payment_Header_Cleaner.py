import pandas as pd
import os

# Input/output paths
input_file = r"C:\Users\drunk\Documents\3rd Year\Second Semester\CMPG_321\Project Data\Payment Header.xlsx"
output_folder = r"C:\Users\drunk\Documents\3rd Year\Second Semester\CMPG_321\Project Data\cleaned"
output_file = os.path.join(output_folder, "CLEAN_Payment_Header.xlsx")

os.makedirs(output_folder, exist_ok=True)

# Read Excel
df = pd.read_excel(input_file, dtype=str)

# Strip whitespace
df = df.applymap(lambda x: x.strip() if isinstance(x, str) else x)

# Standardize placeholder customer codes
placeholder_codes = [
    'ZZZ', 'CONS', 'STAND', 'CON3S', 'CON4S', 'CON5', 'CONS2', 
    'CON4', 'CON2S', 'STAN2', '01EXP', '04C', '05C', '06C', '04MA*', '02C'
]
df['CUSTOMER_NUMBER'] = df['CUSTOMER_NUMBER'].replace({code: 'Unknown' for code in placeholder_codes})

# Optionally, uppercase deposit references for consistency
df['DEPOSIT_REF'] = df['DEPOSIT_REF'].str.upper()

# Save cleaned file
df.to_excel(output_file, index=False)

print(f"✅ Cleaned Payment Header Excel file saved → {output_file}")
