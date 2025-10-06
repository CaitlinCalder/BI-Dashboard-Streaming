import pandas as pd
import glob
import os

def clean_age_analysis(file_path, output_dir="cleaned"):
    # Read Excel (treat everything as string initially to avoid weird parse errors)
    df = pd.read_excel(file_path, dtype=str)
    
    # Standardize column names (strip spaces, uppercase)
    df.columns = [col.strip().upper() for col in df.columns]
    
    # Identify numeric columns (all except CUSTOMER_NUMBER & FIN_PERIOD)
    non_numeric = ["CUSTOMER_NUMBER", "FIN_PERIOD"]
    num_cols = [col for col in df.columns if col not in non_numeric]
    
    # Clean numeric values: remove spaces, replace commas with dots, convert to float
    for col in num_cols:
        df[col] = (
            df[col]
            .astype(str)
            .str.replace(" ", "", regex=False)
            .str.replace(",", ".", regex=False)
        )
        df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0.0)
    
    # Ensure FIN_PERIOD is integer (if possible)
    df["FIN_PERIOD"] = pd.to_numeric(df["FIN_PERIOD"], errors="coerce").astype("Int64")
    
    # Save cleaned file as Excel with "CLEAN_" prefix
    os.makedirs(output_dir, exist_ok=True)
    base_name = os.path.basename(file_path)
    clean_name = f"CLEAN_{base_name}"
    out_file = os.path.join(output_dir, clean_name)
    
    # Save to Excel
    df.to_excel(out_file, index=False)
    
    print(f"✅ Cleaned Excel file saved → {out_file}")

# Example usage for one file
clean_age_analysis("Age Analysis.xlsx")

# Example usage for batch (all Excel files in current folder)
# for f in glob.glob("*.xlsx"):
#     clean_age_analysis(f)
