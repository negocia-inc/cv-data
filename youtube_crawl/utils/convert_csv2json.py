import argparse
import json

import pandas as pd


def create_parser() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Preprocess csv.")
    parser.add_argument(
        "csv_path",
        type=str,
        help="path to csv file with ids",
    )
    parser.add_argument(
        "--save_path", type=str, help="save josn path", default="data/video_ids.json"
    )
    args = parser.parse_args()
    return args


def convert_csv2json(csv_file, save_path):
    df = pd.read_csv(csv_file)
    ids = df["id"].to_list()
    with open(save_path, "w") as f:
        json.dump(ids, f)


def main(csv_path, save_path):
    convert_csv2json(csv_path, save_path)


if __name__ == "__main__":
    args = create_parser()
    main(args.csv_path, args.save_path)
