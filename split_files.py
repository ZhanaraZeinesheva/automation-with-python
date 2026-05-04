import pandas as pd
import os
import re

input_file = "merged_output.xlsx"
output_folder = "split_files"
split_column = "Region"  # change this to your column name

os.makedirs(output_folder, exist_ok=True)

df = pd.read_excel(input_file)

def clean_filename(value):
    value = str(value).strip()
    value = re.sub(r'[\\/*?:"<>|]', "_", value)
    return value

if split_column not in df.columns:
    raise ValueError(f"Column '{split_column}' not found. Available columns: {list(df.columns)}")

for value, group in df.groupby(split_column):
    file_name = clean_filename(value)
    output_path = os.path.join(output_folder, f"{file_name}.xlsx")
    group.to_excel(output_path, index=False)
    print(f"Created: {output_path}")

print("Done splitting file.")