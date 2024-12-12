import os
import tempfile
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

import pandas as pd
from loguru import logger

from aws_client import negocia_s3_client, pingpong_s3_client

max_wait = 5
negocia_bucket = "image-scorer-ml-poc"
negocia_s3_dir = "image_data/pingpong/crdx-data-001"
pingpong_bucket = "crdx-data-001"
pingpong_s3_dir = "prd"


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


def _get_target_pingpong_keys():
    negocia_response_list = _list_all_objects(
        negocia_s3_client, negocia_bucket, negocia_s3_dir
    )
    pingpong_response_list = _list_all_objects(
        pingpong_s3_client, pingpong_bucket, pingpong_s3_dir
    )
    df_pingpong = pd.concat(
        [pd.DataFrame(r["Contents"]) for r in pingpong_response_list]
    )
    df_negocia = pd.concat([pd.DataFrame(r["Contents"]) for r in negocia_response_list])
    df_negocia = df_negocia.rename(columns={"Key": "negocia_key"})
    df_negocia["pingpong_key"] = df_negocia.negocia_key.apply(
        lambda s: s.removeprefix(f"{negocia_s3_dir}/")
    )
    df_pingpong = df_pingpong.rename(columns={"Key": "pingpong_key"})
    df_merged = pd.merge(
        df_negocia,
        df_pingpong,
        on="pingpong_key",
        suffixes=("_negocia", "_pingpong"),
        how="outer",
    )
    df_target = df_merged.pipe(lambda df: df[df.Size_pingpong != df.Size_negocia])
    target_pingpong_keys = list(df_target.pingpong_key)
    return target_pingpong_keys


def _transfer_objects(max_workers):
    target_pingpong_keys = _get_target_pingpong_keys()
    logger.info(f"{len(target_pingpong_keys)=}")
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        executor.map(_transfer_object, target_pingpong_keys)


def _transfer_object(pingpong_key):
    negocia_key = os.path.join(negocia_s3_dir, pingpong_key)
    with tempfile.TemporaryDirectory() as d:
        filename = Path(pingpong_key).name
        filepath = str(Path(d) / filename)
        pingpong_s3_client.download_file(pingpong_bucket, pingpong_key, filepath)
        logger.info(f"download {pingpong_bucket=} {pingpong_key=}")
        negocia_s3_client.upload_file(filepath, negocia_bucket, negocia_key)
        logger.info(f"upload {negocia_bucket=} {negocia_key=}")


def execute():
    _transfer_objects()
