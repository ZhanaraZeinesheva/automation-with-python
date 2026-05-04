import pandas as pd
import os

# Folder containing Excel files
input_folder = "excel_files"  # change if needed
merged_file = "merged_output.xlsx"

all_data = []

# Loop through all Excel files in the folder
for file in os.listdir(input_folder):
    if file.endswith(".xlsx"):
        file_path = os.path.join(input_folder, file)

        try:
            df = pd.read_excel(file_path)
            df["source_file"] = file  # optional: track where data came from
            all_data.append(df)
            print(f"Loaded: {file}")
        except Exception as e:
            print(f"Error reading {file}: {e}")

# Merge all data
if all_data:
    merged_df = pd.concat(all_data, ignore_index=True)

    # Save to Excel
    merged_df.to_excel(merged_file, index=False)
    print(f"\nMerged file saved as: {merged_file}")
else:
    print("No Excel files found.")