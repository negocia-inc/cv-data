import os
import tempfile
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

import fire
import pandas as pd
from aws_client import negocia_s3_client, pingpong_s3_client
from loguru import logger
from util import _list_all_objects

negocia_bucket = "image-scorer-ml-poc"
negocia_s3_dir = "image_data/pingpong/crdx-data-001/prd/raw_human_cleansed"
pingpong_bucket = "crdx-prd-data-001"
# クレンジングデータの保存先変前のdir
pingpong_raw_s3_dir = "prd/raw"
# クレンジングデータの保存先変後のdir
pingpong_cleansing_s3_dir = "prd/raw_human_cleansed"


def _get_target_pingpong_keys(csv_path):
    csv_df = pd.read_csv(csv_path)
    already_cleansnig_list = list(csv_df["creative_id"].astype(str) + ".psd")

    # get s3 object list
    negocia_response_list = _list_all_objects(
        negocia_s3_client, negocia_bucket, negocia_s3_dir
    )
    pingpong_cleansing_response_list = _list_all_objects(
        pingpong_s3_client, pingpong_bucket, pingpong_cleansing_s3_dir
    )

    # list to dataframe
    df_cleansing_pingpong = pd.concat(
        [pd.DataFrame(r["Contents"]) for r in pingpong_cleansing_response_list]
    )

    df_negocia = pd.concat([pd.DataFrame(r["Contents"]) for r in negocia_response_list])

    df_cleansing_pingpong = df_cleansing_pingpong.rename(
        columns={"Key": "pingpong_key"}
    )
    df_cleansing_pingpong["file_id"] = df_cleansing_pingpong.pingpong_key.apply(
        lambda s: s.removeprefix(f"{pingpong_cleansing_s3_dir}/")
    )

    df_negocia = df_negocia.rename(columns={"Key": "negocia_key"})
    df_negocia["file_id"] = df_negocia.negocia_key.apply(
        lambda s: s.removeprefix(f"{negocia_s3_dir}/")
    )

    # merge処理
    df_cleansing_merged = pd.merge(
        df_negocia,
        df_cleansing_pingpong,
        on="file_id",
        suffixes=("_negocia", "_pingpong"),
        how="right",
    ).dropna(subset=["pingpong_key"])

    # csvによって取得するファイルを絞る
    df_cleansing_merged = df_cleansing_merged[
        df_cleansing_merged["file_id"].isin(already_cleansnig_list)
    ]

    # 同じファイルサイズの場合は除外
    df_cleansing_target = df_cleansing_merged.pipe(
        lambda df: df[df.Size_pingpong != df.Size_negocia]
    )

    df_cleansing_target.to_csv("transfer.csv", index=False)

    # ダウンロードするファイルのpathリストを作成
    target_cleansing_pingpong_keys = list(df_cleansing_target.pingpong_key)

    return target_cleansing_pingpong_keys


def _transfer_object(pingpong_key):
    filename = Path(pingpong_key).name
    negocia_key = os.path.join(negocia_s3_dir, filename)
    with tempfile.TemporaryDirectory() as d:
        filepath = str(Path(d) / filename)
        pingpong_s3_client.download_file(pingpong_bucket, pingpong_key, filepath)
        logger.info(f"download {pingpong_bucket=} {pingpong_key=}")
        negocia_s3_client.upload_file(filepath, negocia_bucket, negocia_key)
        logger.info(f"upload {negocia_bucket=} {negocia_key=}")


def _select_transfer_objects(csv_path, max_workers):
    target_pingpong_keys = _get_target_pingpong_keys(csv_path)
    logger.info(f"{len(target_pingpong_keys)=}")
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        executor.map(_transfer_object, target_pingpong_keys)


def select_transfer(csv_path, max_workers=16):
    logger.info(f"start job: {max_workers=}")
    _select_transfer_objects(csv_path, max_workers)
    logger.info(f"finished job: {max_workers=}")


if __name__ == "__main__":
    fire.Fire()
