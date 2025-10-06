import pandas as pd
import os

input_file = r"C:\Users\drunk\Documents\3rd Year\Second Semester\CMPG_321\Project Data\Purchases Headers.xlsx"
output_folder = r"C:\Users\drunk\Documents\3rd Year\Second Semester\CMPG_321\Project Data\cleaned"
os.makedirs(output_folder, exist_ok=True)
output_file = os.path.join(output_folder, "CLEAN_Purchases_Headers.xlsx")

# Read Excel
df = pd.read_excel(input_file, dtype=str)

# Trim spaces
df = df.applymap(lambda x: x.strip() if isinstance(x, str) else x)

# Standardize SUPPLIER_CODE and PURCH_DOC_NO to upper case
df['SUPPLIER_CODE'] = df['SUPPLIER_CODE'].str.upper()
df['PURCH_DOC_NO'] = df['PURCH_DOC_NO'].str.upper()

# Convert PURCH_DATE to datetime and remove time
df['PURCH_DATE'] = pd.to_datetime(df['PURCH_DATE'], errors='coerce').dt.date

# Save cleaned file
if os.path.exists(output_file):
    os.remove(output_file)
df.to_excel(output_file, index=False)
print("Cleaned file saved to:", output_file)
