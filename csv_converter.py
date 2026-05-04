import pandas as pd
import os

input_file = "merged_output.xlsx"

# Get same folder path
folder = os.path.dirname(input_file) or "."

# Create output file name
output_file = os.path.join(folder, "merged_output.csv")

try:
    df = pd.read_excel(input_file)
    df.to_csv(output_file, index=False, encoding="utf-8-sig")

    print(f"Converted successfully: {output_file}")

except Exception as e:
    print(f"Error: {e}")