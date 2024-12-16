import argparse
import concurrent.futures
from pathlib import Path

import pandas as pd
from boto3.session import Session
from google.cloud import storage
from loguru import logger

max_workers = 5
GCS_PROJECT = "ida-prd"


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
        help="creative directory.",
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
    """
    指定されたプロファイル名を使用してS3クライアントを取得します。

    Args:
        profile_name (str): AWSプロファイル名。

    Returns:
        boto3.client: S3クライアントオブジェクト。
    """
    session = Session(profile_name=profile_name)
    client = session.client("s3")
    return client


def _list_all_objects(s3_client, bucket, prefix, **kwargs):
    """
    指定されたバケットとプレフィックスに一致する全てのオブジェクトをリストします。

    Args:
        s3_client (boto3.client): S3クライアントオブジェクト。
        bucket (str): バケット名。
        prefix (str): オブジェクトのプレフィックス。
        **kwargs: その他のオプション引数。

    Returns:
        list: 全てのオブジェクトを含むレスポンスのリスト。
    """
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


def _get_negocia_s3_file_list(negocia_profile, negocia_s3_uri):
    negocia_s3_client = get_s3_client(negocia_profile)

    negocia_s3_uri = Path(negocia_s3_uri)
    negocia_bucket_name = negocia_s3_uri.parts[1]
    negocia_s3_dir = "/".join(negocia_s3_uri.parts[2:])

    negocia_response_list = _list_all_objects(
        negocia_s3_client, negocia_bucket_name, negocia_s3_dir
    )
    df_negocia = pd.concat([pd.DataFrame(r["Contents"]) for r in negocia_response_list])
    return df_negocia.Key.apply(lambda x: Path(x).stem).to_list()


def download_file_requester_pays(
    bucket_name, project_id, source_blob_name, destination_file_name, storage_client
):
    bucket = storage_client.bucket(bucket_name, user_project=project_id)
    blob = bucket.blob(source_blob_name)
    blob.download_to_filename(destination_file_name)

    print(
        "Blob {} downloaded to {} using a requester-pays request.".format(
            source_blob_name, destination_file_name
        )
    )


def download_worker(args):
    bucket_name, project_id, source_blob_name, destination_file_name, storage_client = (
        args
    )
    # ダウンロード処理はdownload_file_requester_pays関数をそのまま利用
    download_file_requester_pays(
        bucket_name, project_id, source_blob_name, destination_file_name, storage_client
    )


def main():
    args = create_parser()
    save_dir = Path(args.save_dir)
    performance_df = pd.read_csv(args.csv_path)
    logger.info(f"download file : {len(performance_df.path.unique())}")
    if args.negocia_s3_uri:
        target_keys = _get_negocia_s3_file_list(
            args.negocia_profile, args.negocia_s3_uri
        )
        performance_df = performance_df[
            ~performance_df.id.astype(str).isin(target_keys)
        ]
    performance_df = performance_df[["id", "path"]].drop_duplicates()
    download_dict = dict(zip(performance_df["id"], performance_df["path"]))
    logger.info(f"download file without s3: {len(download_dict)}")
    storage_client = storage.Client()

    with concurrent.futures.ThreadPoolExecutor(max_workers=5) as executor:
        futures = []
        for creative_id, file_path in download_dict.items():
            parts = file_path.split("/")
            bucket_name = parts[2]
            file_name = "/".join(parts[3:])
            file_suffix = Path(file_name).suffix
            save_path = save_dir / f"{creative_id}{file_suffix}"
            if save_path.exists():
                continue
            futures.append(
                executor.submit(
                    download_worker,
                    (bucket_name, GCS_PROJECT, file_name, save_path, storage_client),
                )
            )

        concurrent.futures.wait(futures)
    logger.info("all download finish")


if __name__ == "__main__":
    main()
