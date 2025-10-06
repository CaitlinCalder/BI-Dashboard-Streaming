import pandas as pd
import os

# Input/output
input_file = r"C:\Users\drunk\Documents\3rd Year\Second Semester\CMPG_321\Project Data\Product Ranges (1).xlsx"
output_folder = r"C:\Users\drunk\Documents\3rd Year\Second Semester\CMPG_321\Project Data\cleaned"
os.makedirs(output_folder, exist_ok=True)
output_file = os.path.join(output_folder, "CLEAN_Product Ranges.xlsx")

# Read Excel
df = pd.read_excel(input_file, dtype=str)  # keep everything as string

# Normalize descriptions
df['PRAN_DESC'] = df['PRAN_DESC'].str.strip().str.title()

# Save
if os.path.exists(output_file):
    os.remove(output_file)
df.to_excel(output_file, index=False)
