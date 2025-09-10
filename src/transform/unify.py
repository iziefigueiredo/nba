import pandas as pd
from pathlib import Path

class CSVUnifier:
    """
    A utility class to unify multiple player CSV files into a single dataset.
    """

    def __init__(self,
                 input_folder: Path = Path("data/raw/players"),
                 output_file: Path = Path("data/interim/player_unified.csv")):
        self.input_folder = Path(input_folder)
        self.output_file = Path(output_file)
        self.csv_files = []
        self.dfs = []
        self.all_columns = set()
        self.error_count = 0

    def discover_files(self) -> None:
        print(f"[INFO] Looking for CSV files in: {self.input_folder.resolve()}")
        self.csv_files = list(self.input_folder.glob("*.csv"))
        print(f"[INFO] Found {len(self.csv_files)} CSV files")

    def read_csvs(self) -> None:
        for file in self.csv_files:
            try:
                df = pd.read_csv(file, on_bad_lines="skip")
                print(f"[INFO] Read {file.name}: {df.shape[0]} rows, {df.shape[1]} cols")
                df["source_file"] = file.name
                self.dfs.append(df)
                self.all_columns.update(df.columns)
            except Exception as e:
                print(f"[ERROR] Could not read {file.name}: {e}")
                self.error_count += 1

        if self.error_count > 0:
            print(f"[WARNING] {self.error_count} file(s) could not be read.")

    def normalize_columns(self, df: pd.DataFrame) -> pd.DataFrame:
        for col in self.all_columns:
            if col not in df.columns:
                df[col] = None
        return df[list(self.all_columns)]

    def unify(self) -> pd.DataFrame:
        if not self.csv_files:
            print("[ERROR] No CSV files to process.")
            return pd.DataFrame()

        if not self.dfs:
            print("[ERROR] No valid CSV data loaded.")
            return pd.DataFrame()

        print("[INFO] Normalizing columns and concatenating data...")
        dfs_normalized = [self.normalize_columns(df) for df in self.dfs]
        return pd.concat(dfs_normalized, ignore_index=True)

    def save(self, df: pd.DataFrame) -> None:
        if df.empty:
            print("[ERROR] Nothing to save, DataFrame is empty.")
            return

        self.output_file.parent.mkdir(parents=True, exist_ok=True)
        df.to_csv(self.output_file, index=False, encoding="utf-8")

        print("[SUCCESS] Unification completed")
        print(f"[SUMMARY] File: {self.output_file.resolve()}")
        print(f"[SUMMARY] Rows: {len(df)} | Columns: {len(df.columns)}")

    def run(self) -> None:
        print("[INFO] Starting CSV unification process")
        self.discover_files()
        self.read_csvs()
        df_final = self.unify()
        self.save(df_final)


if __name__ == "__main__":
    unifier = CSVUnifier()
    unifier.run()
