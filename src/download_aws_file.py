import argparse
from concurrent.futures import ThreadPoolExecutor
from functools import partial
from pathlib import Path

import pandas as pd
from boto3.session import Session
from botocore.exceptions import ClientError
from loguru import logger

IREP_BUCKET_NAME = "ida-ad-datalake-prod"
max_workers = 5


def create_parser() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="download creative from csv.")
    parser.add_argument(
        "csv_path",
        type=str,
        help="csv.gz file path.",
    )
    parser.add_argument(
        "save_dir",
        type=str,
        help="Image directory.",
    )

    parser.add_argument(
        "--irep_profile",
        type=str,
        default="default",
        help="aws irep profile name.",
    )

    parser.add_argument(
        "--negocia_profile",
        type=str,
        default="default",
        help="aws negocia profile name.",
    )

    parser.add_argument(
        "--negocia_s3_uri",
        type=str,
        default=None,
        help="negocia s3 creative dir",
    )

    args = parser.parse_args()
    return args


def get_s3_client(profile_name):
    session = Session(profile_name=profile_name)
    client = session.client("s3")
    return client


def _list_all_objects(s3_client, bucket, prefix, **kwargs):
    kwargs.update(Bucket=bucket, Prefix=prefix)

    def _list_objects(continuation_token=None):
        if continuation_token is None:
            response = s3_client.list_objects_v2(**kwargs)
        else:
            response = s3_client.list_objects_v2(
                ContinuationToken=continuation_token, **kwargs
            )
        return response

    responses = []
    response = _list_objects()
    responses.append(response)
    while response["IsTruncated"]:
        continuation_token = response["NextContinuationToken"]
        logger.info(f"{continuation_token=}")
        response = _list_objects(continuation_token)
        responses.append(response)
    return responses


def _get_negocai_s3_file_list(negocia_profile, negocia_s3_uri):
    negocia_s3_client = get_s3_client(negocia_profile)

    negocia_s3_uri = Path(negocia_s3_uri)
    negocia_bucket_name = negocia_s3_uri.parts[1]
    negocia_s3_dir = "/".join(negocia_s3_uri.parts[2:])

    negocia_response_list = _list_all_objects(
        negocia_s3_client, negocia_bucket_name, negocia_s3_dir
    )
    df_negocia = pd.concat([pd.DataFrame(r["Contents"]) for r in negocia_response_list])
    return df_negocia.Key.apply(lambda x: Path(x).stem).to_list()


def download_files_from_s3(s3_client, save_dir, s3_key):
    save_dir = Path(save_dir)
    save_path = save_dir / Path(s3_key).name
    if not save_path.exists():
        try:
            s3_client.download_file(IREP_BUCKET_NAME, s3_key, save_path)
        except ClientError as e:
            logger.info(e)
    else:
        logger.info(f"File already exists: {save_path}")


def main():
    args = create_parser()
    performance_df = pd.read_csv(args.csv_path)
    if args.negocia_s3_uri:
        target_keys = _get_negocai_s3_file_list(
            args.negocia_profile, args.negocia_s3_uri
        )
        performance_df = performance_df[
            ~performance_df.id.astype(str).isin(target_keys)
        ]
    download_list = list(performance_df.path.unique())
    logger.info(f"dowonload file : {len(download_list)}")
    irep_s3 = get_s3_client(args.irep_profile)
    irep_download = partial(download_files_from_s3, irep_s3, args.save_dir)
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        executor.map(irep_download, download_list)


if __name__ == "__main__":
    main()