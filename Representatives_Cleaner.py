import pandas as pd
import os

# Paths
input_file = r"C:\Users\drunk\Documents\3rd Year\Second Semester\CMPG_321\Project Data\Representatives.xlsx"
output_folder = r"C:\Users\drunk\Documents\3rd Year\Second Semester\CMPG_321\Project Data\cleaned"
os.makedirs(output_folder, exist_ok=True)
output_file = os.path.join(output_folder, "CLEAN_Representatives.xlsx")

# Read Excel
df = pd.read_excel(input_file, dtype=str)

# Trim spaces and title case for relevant columns only
for col in ['REP_DESC', 'COMM_METHOD']:
    if col in df.columns:
        df[col] = df[col].apply(lambda x: x.strip().title() if isinstance(x, str) else x)

# Standardize numeric column
if 'COMMISSION' in df.columns:
    df['COMMISSION'] = pd.to_numeric(df['COMMISSION'], errors='coerce')

# Save cleaned file
if os.path.exists(output_file):
    os.remove(output_file)
df.to_excel(output_file, index=False)
print("Cleaned Representatives file saved to:", output_file)
