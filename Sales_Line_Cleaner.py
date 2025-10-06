import pandas as pd
import os

# Load file
file_path = r"C:\Users\drunk\Documents\3rd Year\Second Semester\CMPG_321\Project Data\Sales Line.xlsx"
df = pd.read_excel(file_path)

# Clean INVENTORY_CODE
df['INVENTORY_CODE'] = df['INVENTORY_CODE'].astype(str).str.replace(" ", "").str.upper()
df['INVENTORY_CODE'] = df['INVENTORY_CODE'].replace("999999", "Unknown")

# Ensure numeric columns are numbers
numeric_cols = ['QUANTITY', 'UNIT_SELL_PRICE', 'TOTAL_LINE_PRICE', 'LAST_COST']
df[numeric_cols] = df[numeric_cols].apply(pd.to_numeric, errors='coerce').fillna(0).astype(int)

# Create cleaned folder if it doesn't exist
cleaned_folder = os.path.join(os.path.dirname(file_path), "cleaned")
os.makedirs(cleaned_folder, exist_ok=True)

# Save cleaned file
clean_file_path = os.path.join(cleaned_folder, "CLEAN_Sales Line.xlsx")
df.to_excel(clean_file_path, index=False)

print(f"Cleaned file saved to: {clean_file_path}")
