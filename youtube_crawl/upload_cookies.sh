#!/bin/bash
set -euo pipefail

aws s3 cp ./data/cookies.txt s3://video-ad/youtube/cookies.txt
