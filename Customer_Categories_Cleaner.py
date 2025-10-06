import pandas as pd
import os

# Input and output paths
input_file = r"C:\Users\drunk\Documents\3rd Year\Second Semester\CMPG_321\Project Data\Customer Categories.xlsx"
output_folder = r"C:\Users\drunk\Documents\3rd Year\Second Semester\CMPG_321\Project Data\cleaned"
output_file = os.path.join(output_folder, "CLEAN_Customer Categories.xlsx")

# Ensure the output folder exists
os.makedirs(output_folder, exist_ok=True)

# Read the Excel file
df = pd.read_excel(input_file, dtype={'CCAT_CODE': str})

# Strip whitespace
df = df.applymap(lambda x: x.strip() if isinstance(x, str) else x)

# Standardize capitalization and handle unknowns
df['CCAT_DESC'] = df['CCAT_DESC'].str.strip().str.title()  # Title case
df['CCAT_DESC'] = df['CCAT_DESC'].replace({'No': 'Unknown', '?': 'Unknown', 'Unknown': 'Unknown'})

# Fix known messy geographic names
replace_dict = {
    'Johannesburg Cbdl': 'Johannesburg CBD',
    'N- Prov. Witbank Midd Burg': 'North Province - Witbank / Middelburg',
    'Gaut.N-Eastgatenorwoodmidr': 'Gauteng North - Eastgate / Norwood / Midrand',
    'Stock Movemnt Shipments/Backup': 'Stock Movement Shipments / Backup',
    'Departmentstore': 'Department Store'
}

df['CCAT_DESC'] = df['CCAT_DESC'].replace(replace_dict)

# Save cleaned Excel
df.to_excel(output_file, index=False)

print(f"✅ Cleaned Excel file saved → {output_file}")
