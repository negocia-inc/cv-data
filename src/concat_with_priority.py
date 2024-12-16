import argparse

import pandas as pd


def create_parser() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Preprocess csv.")
    parser.add_argument(
        "high_priority_csv",
        type=str,
        help="path to csv with data which should be prioritised in case of duplicates",
    )
    parser.add_argument("other_csv", type=str, help="path to other csv")
    parser.add_argument(
        "save_path",
        type=str,
        help="save csv.gz path",
    )
    parser.add_argument(
        "--gender",
        action=argparse.BooleanOptionalAction,
        help="in case of gender csvs",
    )
    parser.add_argument(
        "--age",
        action=argparse.BooleanOptionalAction,
        help="in case of age csvs",
    )
    parser.add_argument(
        "--no_ad_group_id",
        action=argparse.BooleanOptionalAction,
        help="in case of campaign level CSVs",
    )
    args = parser.parse_args()
    return args


def main(args):
    df_a = pd.read_csv(args.high_priority_csv, dtype={"id": "str"})
    df_b = pd.read_csv(args.other_csv, dtype={"id": "str"})

    df_a_drop = df_a.drop_duplicates()
    df_b_drop = df_b.drop_duplicates()

    if len(df_a) != len(df_a_drop):
        raise UserWarning(
            f"There were duplicates {abs(len(df_a) - len(df_a_drop))} in {args.high_priority_csv}. Please check the query for this data"
        )
    if len(df_b) != len(df_b_drop):
        raise UserWarning(
            f"There were duplicates {abs(len(df_a) - len(df_a_drop))} in {args.other_csv}. Please check the query for this data"
        )

    concatenated = pd.concat([df_a_drop, df_b_drop])
    # Drop duplicates, keeping the first occurrence (from df_a)
    duplicate_subset = ["campaign_id", "ad_group_id", "id", "year", "month", "day"]
    if args.age:
        duplicate_subset = ["campaign_id", "ad_group_id", "date", "age_range_type"]
    if args.gender:
        duplicate_subset = ["campaign_id", "ad_group_id", "date", "gender_type"]
    if args.no_ad_group_id:
        duplicate_subset.remove("ad_group_id")
    result = concatenated.drop_duplicates(subset=duplicate_subset, keep="first")
    result.to_csv(args.save_path, index=False, compression="gzip")


if __name__ == "__main__":
    args = create_parser()
    main(args)
