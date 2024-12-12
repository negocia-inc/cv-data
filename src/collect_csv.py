import argparse
from pathlib import Path

import dask.dataframe as dd
import pandas as pd


def create_parser() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Preprocess csv.")
    parser.add_argument(
        "csv_dir",
        type=str,
        help="dir path containing multiple csv to be concatenated",
    )
    parser.add_argument(
        "save_path",
        type=str,
        help="save csv.gz path",
    )
    args = parser.parse_args()
    return args


def main(csv_dir, save_path):
    csv_paths = Path(csv_dir) / "*.csv.gz"
    df = dd.read_csv(csv_paths, dtype={"id": "str", "costs": "float64"})
    df.to_csv(save_path, single_file=True, index=False, compression="gzip")


if __name__ == "__main__":
    args = create_parser()
    main(args.csv_dir, args.save_path)
