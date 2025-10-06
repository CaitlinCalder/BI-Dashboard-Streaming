import pandas as pd
import os

# Paths
input_file = r"C:\Users\drunk\Documents\3rd Year\Second Semester\CMPG_321\Project Data\Purchases Lines (1).xlsx"
output_folder = r"C:\Users\drunk\Documents\3rd Year\Second Semester\CMPG_321\Project Data\cleaned"
os.makedirs(output_folder, exist_ok=True)
output_file = os.path.join(output_folder, "CLEAN_Purchases_Lines.xlsx")

# Read Excel
df = pd.read_excel(input_file, dtype=str)

# Trim spaces
df = df.applymap(lambda x: x.strip() if isinstance(x, str) else x)

# Standardize INVENTORY_CODE and PURCH_DOC_NO to upper case
df['PURCH_DOC_NO'] = df['PURCH_DOC_NO'].str.upper()
df['INVENTORY_CODE'] = df['INVENTORY_CODE'].str.upper()

# Convert numeric columns to float
numeric_cols = ['QUANTITY', 'UNIT_COST_PRICE', 'TOTAL_LINE_COST']
for col in numeric_cols:
    df[col] = pd.to_numeric(df[col], errors='coerce')

# Save cleaned file
if os.path.exists(output_file):
    os.remove(output_file)
df.to_excel(output_file, index=False)
print("Cleaned Purchases Lines file saved to:", output_file)
