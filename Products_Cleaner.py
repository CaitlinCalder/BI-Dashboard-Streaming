import pandas as pd
import os
import re

# Paths
input_file = r"C:\Users\drunk\Documents\3rd Year\Second Semester\CMPG_321\Project Data\Products (1).xlsx"
output_folder = r"C:\Users\drunk\Documents\3rd Year\Second Semester\CMPG_321\Project Data\cleaned"
os.makedirs(output_folder, exist_ok=True)
output_file = os.path.join(output_folder, "CLEAN_Products.xlsx")

# Read Excel
df = pd.read_excel(input_file, dtype=str)

# Trim spaces
df = df.applymap(lambda x: x.strip() if isinstance(x, str) else x)

# Fix INVENTORY_CODE: remove internal spaces
df['INVENTORY_CODE'] = df['INVENTORY_CODE'].apply(lambda x: re.sub(r"\s+", "", x))

# LAST_COST: convert to numeric
df['LAST_COST'] = pd.to_numeric(df['LAST_COST'], errors='coerce').fillna(0)

# STOCK_IND: standardize Yes/No
df['STOCK_IND'] = df['STOCK_IND'].str.strip().str.title()

# Save
if os.path.exists(output_file):
    os.remove(output_file)
df.to_excel(output_file, index=False)
print("Cleaned file saved to:", output_file)
