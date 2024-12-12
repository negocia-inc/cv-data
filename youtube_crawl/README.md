# YouTube Crawl Lambda

This directory contains scripts and configurations for a Lambda function that downloads YouTube videos and uploads them to an S3 bucket.

## Directory Structure

- `build_and_push.sh`: Script to build the Docker image for the Lambda function and push it to Amazon ECR.
- `upload_cookies.sh`: Script to upload cookies to S3.
- `lambda/`: Directory containing the Lambda function code and Dockerfile.
- `utils/`: Directory containing utility scripts.

## Usage

### Build and Push Docker Image for Lambda Function

To build and push the Docker image for the Lambda function, run the following command:

```bash
./build_and_push.sh
```

Then, deploy the new image to the Lambda function using the AWS Management Console.
The Lambda function is [here](https://ap-northeast-1.console.aws.amazon.com/lambda/home?region=ap-northeast-1#/functions/ytdl?newFunction=true&tab=image).

### Get and Upload Cookies to S3 for YouTube Crawl

Warning: Using this cookie for mass downloads may result in a ban for the YouTube account used to obtain the cookie. Therefore, to avoid being banned, the number of concurrent executions is limited to three.

To get and upload cookies to S3 for YouTube crawl, run the following command:

```bash
poetry python utils/get_cookies.py
# Sign in to YouTube in the browser that opens then press Enter in the terminal
```

Then, run the following command to upload the cookies to S3:

```bash
./upload_cookies.sh
```

### Trigger Lambda Function to Download YouTube Videos

Before triggering the Lambda function, make sure that the cookies are uploaded to S3 and the Lambda function is deployed with the latest Docker image.

Get video_id from [query](../query/youtube_crawl/video_id.sql) in csv and convert to json of id in ```poetry run python utils/convert_csv2json.py <csv_path>```.

Besides, `./data/video_ids.json` should be updated with the video IDs to download.

To trigger the Lambda function, run the following command:

```bash
poetry run python utils/run_lambda.py
```

Then, videos will be uploaded to the S3 bucket specified in the Lambda function handler.