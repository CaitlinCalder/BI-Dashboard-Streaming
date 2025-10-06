import pandas as pd
import os

file_path = r"C:\Users\drunk\Documents\3rd Year\Second Semester\CMPG_321\Project Data\Trans Types.xlsx"
df = pd.read_excel(file_path)

# Clean TRANSTYPE_CODE
df['TRANSTYPE_CODE'] = pd.to_numeric(df['TRANSTYPE_CODE'], errors='coerce').fillna(0).astype(int)

# Clean TRANSTYPE_DESC: title case
df['TRANSTYPE_DESC'] = df['TRANSTYPE_DESC'].astype(str).str.strip().str.title()

# Save cleaned file
cleaned_folder = os.path.join(os.path.dirname(file_path), "cleaned")
os.makedirs(cleaned_folder, exist_ok=True)
clean_file_path = os.path.join(cleaned_folder, "CLEAN_TransTypes.xlsx")
df.to_excel(clean_file_path, index=False)

print(f"Cleaned file saved to: {clean_file_path}")
