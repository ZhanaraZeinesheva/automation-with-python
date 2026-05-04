import pandas as pd

input_file = "hotel_bookings.csv"
output_file = "clean_hotel_bookings.csv"

df = pd.read_csv(input_file)

# -------------------------------
# 1. Clean column names
# -------------------------------
df.columns = (
    df.columns
    .astype(str)
    .str.strip()
    .str.replace(r"\s+", "_", regex=True)
    .str.lower()
)

# -------------------------------
# 2. Clean text columns
# -------------------------------
text_cols = df.select_dtypes(include=["object", "string"]).columns

for col in text_cols:
    df[col] = (
        df[col]
        .astype("string")
        .str.strip()                      # remove leading/trailing spaces
        .str.replace(r"\s+", " ", regex=True)  # remove extra internal spaces
        .str.lower()                     # change to lowercase (edit if needed)
    )

# -------------------------------
# 3. Remove duplicate rows
# -------------------------------
df = df.drop_duplicates()

# -------------------------------
# 4. Handle missing values
# -------------------------------

# Option A: Fill missing values
#for col in df.columns:
    #if pd.api.types.is_numeric_dtype(df[col]):
        #df[col] = df[col].fillna(0)          # numeric → 0
    #else:
        #df[col] = df[col].fillna("unknown")  # text → "unknown"

# Option B: Drop rows with any missing values
df = df.dropna()

# Option C: Drop columns with any missing values
# df = df.dropna(axis=1)

# -------------------------------
# 5. Save cleaned dataset
# -------------------------------
df.to_csv(output_file, index=False)

print("Cleaning complete. Saved to:", output_file)




