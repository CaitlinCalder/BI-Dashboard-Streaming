import pandas as pd
import os

# Paths
original_path = r"C:\Users\drunk\Documents\3rd Year\Second Semester\CMPG_321\Project Data\Sales Header.xlsx"
cleaned_folder = r"C:\Users\drunk\Documents\3rd Year\Second Semester\CMPG_321\Project Data\cleaned"
cleaned_file = os.path.join(cleaned_folder, "CLEAN_Sales Header.xlsx")

# Create cleaned folder if it doesn't exist
os.makedirs(cleaned_folder, exist_ok=True)

# Read file
df = pd.read_excel(original_path)

# Strip whitespace from all string columns
for col in df.select_dtypes(include='object'):
    df[col] = df[col].str.strip()

# Uppercase REP_CODE (if not already)
df['REP_CODE'] = df['REP_CODE'].str.upper()

# Ensure dates are date-only
df['TRANS_DATE'] = pd.to_datetime(df['TRANS_DATE']).dt.date

# Remove exact duplicate rows
df = df.drop_duplicates()

# Save cleaned file
df.to_excel(cleaned_file, index=False)
print(f"Cleaned file saved at: {cleaned_file}")
