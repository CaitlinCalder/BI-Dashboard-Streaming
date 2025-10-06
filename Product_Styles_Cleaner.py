import pandas as pd
import os
import re

input_file = r"C:\Users\drunk\Documents\3rd Year\Second Semester\CMPG_321\Project Data\Products Styles (1).xlsx"
output_folder = r"C:\Users\drunk\Documents\3rd Year\Second Semester\CMPG_321\Project Data\cleaned"
os.makedirs(output_folder, exist_ok=True)
output_file = os.path.join(output_folder, "CLEAN_Products_Styles.xlsx")

# Read Excel
df = pd.read_excel(input_file, dtype=str)

# Trim spaces
df = df.applymap(lambda x: x.strip() if isinstance(x, str) else x)

# Replace empty cells / NaN with 'N/A'
df = df.fillna("N/A")

# Clean INVENTORY_CODE: remove spaces
df['INVENTORY_CODE'] = df['INVENTORY_CODE'].apply(lambda x: re.sub(r"\s+", "", x))

# Standardize GENDER
df['GENDER'] = df['GENDER'].str.lower().replace({'n/a': 'N/A'})

# Columns to title case but keep N/A as-is
text_cols = ['MATERIAL', 'STYLE', 'COLOUR', 'BRANDING', 'QUAL_PROBS']
for col in text_cols:
    df[col] = df[col].apply(lambda x: 'N/A' if str(x).lower() in ['n/a', 'na'] else str(x).title())

# Save cleaned file
if os.path.exists(output_file):
    os.remove(output_file)
df.to_excel(output_file, index=False)
print("Cleaned file saved to:", output_file)
