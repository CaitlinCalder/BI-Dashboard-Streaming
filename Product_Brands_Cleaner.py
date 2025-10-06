import pandas as pd
import os

# Input/output
input_file = r"C:\Users\drunk\Documents\3rd Year\Second Semester\CMPG_321\Project Data\Product Categories (1).xlsx"
output_folder = r"C:\Users\drunk\Documents\3rd Year\Second Semester\CMPG_321\Project Data\cleaned"
os.makedirs(output_folder, exist_ok=True)
output_file = os.path.join(output_folder, "CLEAN_Product Categories.xlsx")

# Read Excel
df = pd.read_excel(input_file, dtype=str)  # keep everything as string

# Normalize text
for col in ['PRODCAT_DESC']:
    df[col] = df[col].str.strip().str.title()

# Ensure year or year ranges are proper strings
def fix_years(val):
    if pd.isna(val):
        return val
    val = val.strip()
    if val.replace('+','').isdigit():
        return val
    return val

df['PRODCAT_DESC'] = df['PRODCAT_DESC'].apply(fix_years)

# Save
if os.path.exists(output_file):
    os.remove(output_file)
df.to_excel(output_file, index=False)
