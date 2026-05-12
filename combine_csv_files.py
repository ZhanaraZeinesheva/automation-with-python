import pandas as pd
import os
import math

input_folder = "csv_files"
output_file = "combined_files.xlsx"

MAX_ROWS = 1048575

with pd.ExcelWriter(output_file, engine="openpyxl") as writer:

    for file in os.listdir(input_folder):

        if file.endswith(".csv"):

            file_path = os.path.join(input_folder, file)

            try:
                df = pd.read_csv(file_path)

                total_rows = len(df)

                # Base sheet name
                base_sheet_name = os.path.splitext(file)[0][:25]

                # Small enough → one sheet
                if total_rows <= MAX_ROWS:

                    df.to_excel(
                        writer,
                        sheet_name=base_sheet_name,
                        index=False
                    )

                    print(f"Added sheet: {base_sheet_name}")

                # Too large → split into chunks
                else:

                    num_chunks = math.ceil(total_rows / MAX_ROWS)

                    for i in range(num_chunks):

                        start = i * MAX_ROWS
                        end = start + MAX_ROWS

                        chunk = df.iloc[start:end]

                        sheet_name = f"{base_sheet_name}_{i+1}"

                        chunk.to_excel(
                            writer,
                            sheet_name=sheet_name[:31],
                            index=False
                        )

                        print(f"Added sheet: {sheet_name}")

            except Exception as e:
                print(f"Error processing {file}: {e}")

print(f"\nCombined workbook created: {output_file}")