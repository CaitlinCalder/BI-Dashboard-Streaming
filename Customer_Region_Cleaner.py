import pandas as pd
import os

# Input and output paths
input_file = r"C:\Users\drunk\Documents\3rd Year\Second Semester\CMPG_321\Project Data\Customer Regions (1).xlsx"
output_folder = r"C:\Users\drunk\Documents\3rd Year\Second Semester\CMPG_321\Project Data\cleaned"
output_file = os.path.join(output_folder, "CLEAN_Customer Regions.xlsx")

# Ensure the output folder exists
os.makedirs(output_folder, exist_ok=True)

# Read the Excel file
df = pd.read_excel(input_file, dtype={'REGION_CODE': str})

# Strip whitespace
df = df.applymap(lambda x: x.strip() if isinstance(x, str) else x)

# Standardize capitalization
df['REGION_DESC'] = df['REGION_DESC'].str.title()

# Replace unclear or placeholder values
df['REGION_DESC'] = df['REGION_DESC'].replace({
    'Cons': 'Unknown',
    'Adapro': 'Unknown',
    'Closed - Bad Debts': 'Closed - Bad Debts',
    'Closed': 'Closed'
})

# Fix known messy geographic names
replace_dict = {
    'Pretoria North/South/West': 'Pretoria North/South/West',
    'Witbank/Middelburg': 'Witbank / Middelburg',
    'Potch / Krugersdorp / Kuruman': 'Potchefstroom / Krugersdorp / Kuruman',
    'Gauteng N - Eastgate/Norwood/Midrand': 'Gauteng North - Eastgate / Norwood / Midrand',
    'Gauteng N - Randburg/Rivonia': 'Gauteng North - Randburg / Rivonia',
    'Gauteng E - Alberton/Benoni/Kempton': 'Gauteng East - Alberton / Benoni / Kempton'
}

df['REGION_DESC'] = df['REGION_DESC'].replace(replace_dict)

# Save cleaned Excel
df.to_excel(output_file, index=False)

print(f"✅ Cleaned Excel file saved → {output_file}")
