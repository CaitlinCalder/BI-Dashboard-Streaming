import pandas as pd
import os

# Load file
file_path = r"C:\Users\drunk\Documents\3rd Year\Second Semester\CMPG_321\Project Data\Suppliers (1).xlsx"
df = pd.read_excel(file_path)

# Clean SUPPLIER_CODE
df['SUPPLIER_CODE'] = df['SUPPLIER_CODE'].astype(str).str.strip().str.upper()
df['SUPPLIER_CODE'] = df['SUPPLIER_CODE'].replace("999999", "Unknown")

# Clean SUPPLIER_DESC
df['SUPPLIER_DESC'] = df['SUPPLIER_DESC'].astype(str).str.strip().str.upper()

# Clean EXCLSV
df['EXCLSV'] = df['EXCLSV'].astype(str).str.strip().str.upper()
df['EXCLSV'] = df['EXCLSV'].apply(lambda x: x if x in ['Y', 'N'] else 'N')

# Clean numeric columns
df['NORMAL_PAYTERMS'] = pd.to_numeric(df['NORMAL_PAYTERMS'], errors='coerce').fillna(0).astype(int)
df['CREDIT_LIMIT'] = pd.to_numeric(df['CREDIT_LIMIT'], errors='coerce').fillna(0).astype(int)

# Save cleaned file
cleaned_folder = os.path.join(os.path.dirname(file_path), "cleaned")
os.makedirs(cleaned_folder, exist_ok=True)
clean_file_path = os.path.join(cleaned_folder, "CLEAN_Suppliers.xlsx")
df.to_excel(clean_file_path, index=False)

print(f"Cleaned file saved to: {clean_file_path}")
