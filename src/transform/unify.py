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

    print("[INFO] Starting CSV unification process")
    print(f"[INFO] Looking for CSV files in: {input_folder.resolve()}")

    # List all CSV files
    csv_files = [f for f in input_folder.glob("*.csv")]
    print(f"[INFO] Found {len(csv_files)} CSV files")

    if not csv_files:
        print("[WARNING] No CSV files found, process aborted.")
        return

    dfs = []
    all_columns = set()
    error_count = 0

    # Step 1: Read and collect all columns
    for file in csv_files:
        try:
            df = pd.read_csv(file, on_bad_lines="skip")  # skip broken lines
            print(f"[INFO] Read {file.name}: {df.shape[0]} rows, {df.shape[1]} cols")
            df["source_file"] = file.name
            dfs.append(df)
            all_columns.update(df.columns)
        except Exception as e:
            print(f"[ERROR] Could not read {file.name}: {e}")
            error_count += 1

    if error_count > 0:
        print(f"[WARNING] {error_count} file(s) could not be read.")

    if not dfs:
        print("[ERROR] No valid CSVs could be processed, process aborted.")
        return

    all_columns = list(all_columns)

    # Step 2: Normalize DataFrames
    print("[INFO] Normalizing columns and concatenating data...")
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
    print(f"[INFO] Saving unified CSV to: {output_file.resolve()}")
    df_final.to_csv(output_file, index=False, encoding="utf-8")

    print("[SUCCESS] Unification completed")
    print(f"[SUMMARY] Rows: {len(df_final)} | Columns: {len(all_columns)}")


if __name__ == "__main__":
    unify_csv()
