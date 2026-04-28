import pandas as pd
import re

input_file = "dirty_dataset.csv"
output_file = "clean_dataset.csv"

df = pd.read_csv(input_file, encoding="latin1")

# Clean column names
df.columns = (
    df.columns
    .astype(str)
    .str.strip()
    .str.lower()
    .str.replace(r"\s+", "_", regex=True)
    .str.replace(r"[^a-z0-9_]", "", regex=True)
)

# Remove footnotes like [1], [a], [4][b]
def remove_footnotes(value):
    if isinstance(value, str):
        return re.sub(r"\[[^\]]*\]", "", value).strip()
    return value

df = df.map(remove_footnotes)

# General text cleanup
def clean_text(value):
    if isinstance(value, str):
        value = value.strip()

        replacements = {
            "BeyoncÃƒÂ©": "Beyonce",
            "BeyoncÃ©": "Beyonce",
            "BeyoncÃƒÂ": "Beyonce",
            "BeyoncÃ": "Beyonce",
            "Ã¢Â€Â“": "-",
            "â€“": "-",
            "â€”": "-",
            "Â": "",
        }

        for bad, good in replacements.items():
            value = value.replace(bad, good)

        return value.strip()
    return value

df = df.map(clean_text)

# Force clean artist column
if "artist" in df.columns:
    df["artist"] = (
        df["artist"]
        .astype(str)
        .str.replace(r"Beyonc.*", "Beyonce", regex=True)
        .str.strip()
    )

# Clean tour_title column: remove weird trailing encoding characters
if "tour_title" in df.columns:
    df["tour_title"] = (
        df["tour_title"]
        .astype(str)
        .str.replace(r"Ã.*", "", regex=True)
        .str.replace(r"â.*", "", regex=True)
        .str.replace(r"Â.*", "", regex=True)
        .str.replace("*", "", regex=False)
        .str.strip()
    )

# Clean years column
if "years" in df.columns:
    # Extract all 4-digit years from the messy text
    extracted_years = df["years"].astype(str).str.findall(r"\d{4}")

    # Rebuild years column as 2023-2024 or 2023
    df["years"] = extracted_years.apply(
        lambda years: f"{years[0]}-{years[1]}" if len(years) >= 2 else years[0] if len(years) == 1 else ""
    )

    # Create start_year and end_year
    df["start_year"] = extracted_years.apply(
        lambda years: int(years[0]) if len(years) >= 1 else None
    )

    df["end_year"] = extracted_years.apply(
        lambda years: int(years[1]) if len(years) >= 2 else int(years[0]) if len(years) == 1 else None
    )


# Clean rank column
if "rank" in df.columns:
    df["rank"] = (
        df["rank"]
        .astype(str)
        .str.extract(r"(\d+)")
        .astype(float)
    )

# Clean money columns
money_columns = [
    col for col in df.columns
    if "gross" in col or "revenue" in col or "earning" in col
]

for col in money_columns:
    df[col] = (
        df[col]
        .astype(str)
        .str.replace(r"[$,]", "", regex=True)
        .str.extract(r"(\d+\.?\d*)")
        .astype(float)
    )

# Convert other numeric-looking columns
for col in df.columns:
    if col not in money_columns and df[col].dtype == "object":
        cleaned = df[col].astype(str).str.replace(",", "", regex=False)
        numeric_version = pd.to_numeric(cleaned, errors="coerce")

        if numeric_version.notna().sum() > len(df) * 0.6:
            df[col] = numeric_version

# Remove reference column if it exists
for col in df.columns:
    if col in ["ref", "refs", "reference", "references"]:
        df = df.drop(columns=[col])

# Remove empty rows and duplicates
df = df.dropna(how="all")
df = df.drop_duplicates()

df.to_csv(output_file, index=False)

print(f"Cleaned file saved as: {output_file}")