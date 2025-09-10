import pandas as pd
from pathlib import Path

def unify_csv(
    input_folder: Path = Path("data/raw/players"),
    output_file: Path = Path("data/interim/player_unified.csv")
) -> None:
    """
    Reads all CSV files from input_folder,
    aligns their columns, and saves a single unified CSV in output_file.

    """

    # List all CSV files
    csv_files = [f for f in input_folder.glob("*.csv")]

    if not csv_files:
        print("⚠️ No CSV files found, check the input folder path.")
        return

    dfs = []
    all_columns = set()

    # Step 1: Read and collect all columns
    for file in csv_files:
        try:
            df = pd.read_csv(file, on_bad_lines="skip")  # skip broken lines
            print(f"✅ Read {file.name} with {df.shape[0]} rows and {df.shape[1]} cols")
            df["source_file"] = file.name
            dfs.append(df)
            all_columns.update(df.columns)
        except Exception as e:
            print(f"❌ Error reading {file.name}: {e}")

    if not dfs:
        print(" No valid CSVs could be read.")
        return

    all_columns = list(all_columns)

    # Step 2: Normalize DataFrames
    dfs_normalized = []
    for df in dfs:
        for col in all_columns:
            if col not in df.columns:
                df[col] = None
        dfs_normalized.append(df[all_columns])

    # Step 3: Concatenate
    df_final = pd.concat(dfs_normalized, ignore_index=True)

    # Step 4: Ensure interim folder exists
    output_file.parent.mkdir(parents=True, exist_ok=True)

    # Step 5: Save unified CSV
    df_final.to_csv(output_file, index=False, encoding="utf-8")

    print(f"✅ Unification completed! File saved at {output_file.resolve()}")
    print(f" Total rows: {len(df_final)} | Total columns: {len(all_columns)}")


if __name__ == "__main__":
    unify_csv()
