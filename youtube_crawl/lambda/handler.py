import os
import shutil

import boto3
import yt_dlp
from botocore.exceptions import ClientError

s3_client = boto3.client("s3")


def download_cookies_from_s3(bucket_name, s3_key, cookies_path):
    s3_client.download_file(bucket_name, s3_key, cookies_path)


def download_video(video_id, download_path, cookies_path):
    video_url = f"https://www.youtube.com/watch?v={video_id}"
    ydl_opts = {
        "format": "bestvideo[ext=mp4]+bestaudio[ext=m4a]/best[ext=mp4]/best",
        "merge_output_format": "mp4",
        "outtmpl": f"{download_path}/{video_id}.mp4",
        "cachedir": False,
        "cookiefile": cookies_path,
    }
    with yt_dlp.YoutubeDL(ydl_opts) as ydl:
        info_dict = ydl.extract_info(video_url, download=True)
        return ydl.prepare_filename(info_dict)


def exists_video_s3(bucket_name, key):
    try:
        s3_client.head_object(Bucket=bucket_name, Key=key)
        print(f"Object {key} exists in S3")
        return True
    except ClientError as e:
        if e.response["Error"]["Code"] == "404":
            return False  # Object does not exist
        raise e


def get_s3_key(video_id):
    return f"youtube/crawl_2024-02-22/{video_id}.mp4"


def upload_to_s3(video_id, file_path, bucket_name):
    s3_key = get_s3_key(video_id)
    boto3.client("s3").upload_file(file_path, bucket_name, s3_key)
    os.remove(file_path)


def clean_tmpdir():
    files_in_tmp = os.listdir("/tmp")
    for file in files_in_tmp:
        file_path = os.path.join("/tmp", file)
        try:
            if os.path.isfile(file_path):
                os.unlink(file_path)
            elif os.path.isdir(file_path):
                shutil.rmtree(file_path)
        except Exception as e:
            print(e)


def lambda_handler(event, context):
    video_id = event["video_id"]
    bucket_name = event["bucket_name"]
    cookies_key = event["cookies_key"]

    # Temporary paths
    download_path = "/tmp"
    cookies_path = "/tmp/cookies.txt"

    try:
        if exists_video_s3(bucket_name, get_s3_key(video_id)):
            return {"status": "success", "message": "Video already exists in S3"}
        download_cookies_from_s3(bucket_name, cookies_key, cookies_path)
        file_path = download_video(video_id, download_path, cookies_path)
        upload_to_s3(video_id, file_path, bucket_name)
        clean_tmpdir()
        return {
            "status": "success",
            "message": f"Video {video_id} downloaded and uploaded to S3",
        }
    except Exception as e:
        clean_tmpdir()
        return {"status": "error", "message": str(e)}
