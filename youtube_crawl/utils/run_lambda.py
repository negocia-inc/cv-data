import argparse
import asyncio
import json
import os
import random
from functools import partial

import boto3
import tqdm.asyncio

DATA_DIR = os.path.join(os.path.dirname(os.path.dirname(__file__)), "data")
semaphore = asyncio.Semaphore(3)  # Limit the number of concurrent invocations
lambda_client = boto3.client("lambda")


async def invoke_lambda_function(function_name, payload):
    try:
        async with semaphore:
            response = await asyncio.get_event_loop().run_in_executor(
                None,
                partial(
                    lambda_client.invoke,
                    FunctionName=function_name,
                    InvocationType="RequestResponse",
                    Payload=json.dumps(payload).encode("utf-8"),
                ),
            )
        response_payload = json.loads(response["Payload"].read().decode("utf-8"))
        return payload["video_id"], response_payload
    except Exception as e:
        return payload["video_id"], {"status": "error", "message": str(e)}


async def main(args):
    function_name = args.function_name
    video_ids_path = os.path.join(args.data_dir, args.video_ids_file)
    succeeded_video_ids_path = os.path.join(
        args.data_dir, args.succeeded_video_ids_file
    )
    failed_video_ids_path = os.path.join(args.data_dir, args.failed_video_ids_file)
    deleted_video_ids_path = os.path.join(args.data_dir, args.deleted_video_ids_file)

    with open(video_ids_path, "r") as f:
        video_ids = json.load(f)
    random.shuffle(video_ids)
    try:
        with open(succeeded_video_ids_path, "r") as f:
            succeeded_video_ids = json.load(f)
    except FileNotFoundError:
        succeeded_video_ids = []
    succeeded_video_ids = set(succeeded_video_ids)
    try:
        with open(deleted_video_ids_path, "r") as f:
            deleted_video_ids = json.load(f)
    except FileNotFoundError:
        deleted_video_ids = []
    deleted_video_ids = set(deleted_video_ids)

    payloads = [
        dict(
            video_id=video_id, bucket_name="video-ad", cookies_key="youtube/cookies.txt"
        )
        for video_id in video_ids
        if video_id not in succeeded_video_ids and video_id not in deleted_video_ids
    ]

    # Create a list to hold the tasks
    tasks = [invoke_lambda_function(function_name, payload) for payload in payloads]

    # Run tasks concurrently and wait for them to finish
    results = await tqdm.asyncio.tqdm.gather(*tasks)

    succeeded_video_ids = list(succeeded_video_ids)
    failed_video_ids = []
    deleted_video_ids = list(deleted_video_ids)

    for video_id, result in results:
        if result["status"] == "success":
            succeeded_video_ids.append(video_id)
        else:
            if "This video is private" in result["message"]:
                deleted_video_ids.append(video_id)
            elif result["message"].strip().endswith("Video unavailable"):
                deleted_video_ids.append(video_id)
            else:
                failed_video_ids.append(video_id)
            print(f"Failed to download video {video_id}: {result['message']}")

    with open(succeeded_video_ids_path, "w") as f:
        json.dump(list(sorted(succeeded_video_ids)), f, indent=2)
    with open(failed_video_ids_path, "w") as f:
        json.dump(list(sorted(failed_video_ids)), f, indent=2)
    with open(deleted_video_ids_path, "w") as f:
        json.dump(list(sorted(deleted_video_ids)), f, indent=2)


# Run the main function
if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--function-name",
        type=str,
        default="arn:aws:lambda:ap-northeast-1:359540490723:function:ytdl",
    )
    parser.add_argument("--data_dir", type=str, default=DATA_DIR)
    parser.add_argument("--video-ids-file", type=str, default="video_ids.json")
    parser.add_argument(
        "--succeeded-video-ids-file", type=str, default="succeeded_video_ids.json"
    )
    parser.add_argument(
        "--failed-video-ids-file", type=str, default="failed_video_ids.json"
    )
    parser.add_argument(
        "--deleted-video-ids-file", type=str, default="deleted_video_ids.json"
    )
    args = parser.parse_args()
    asyncio.run(main(args))
