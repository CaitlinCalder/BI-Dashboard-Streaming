import pandas as pd
import os

# Input folder
input_folder = r"C:\Users\drunk\Documents\3rd Year\Second Semester\CMPG_321\Project Data\cleaned"
# Output folder
output_folder = os.path.join(input_folder, "JsonOutput")
os.makedirs(output_folder, exist_ok=True)

# Loop through all files
for filename in os.listdir(input_folder):
    file_path = os.path.join(input_folder, filename)
    
    try:
        # Read Excel files
        if filename.endswith((".xlsx", ".xls")):
            df = pd.read_excel(file_path)
        # Read CSV files
        elif filename.endswith(".csv"):
            df = pd.read_csv(file_path)
        else:
            print(f"Skipped {filename} (unsupported file type)")
            continue
        
        # Convert column headers and string values to title case
        df.columns = [col.title() for col in df.columns]
        for col in df.select_dtypes(include=['object']).columns:
            df[col] = df[col].str.title()
        
        # Save as JSON
        json_filename = os.path.splitext(filename)[0] + ".json"
        json_path = os.path.join(output_folder, json_filename)
        df.to_json(json_path, orient="records", indent=4)
        
        print(f"Converted {filename} -> {json_filename}")
        
    except Exception as e:
        print(f"Failed to process {filename}: {e}")

print("All supported files have been converted to JSON.")
