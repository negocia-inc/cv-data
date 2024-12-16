#!/bin/bash
set -eux -o pipefail

ECR_ROOT=359540490723.dkr.ecr.ap-northeast-1.amazonaws.com
REPO=ytdl

aws ecr get-login-password --region ap-northeast-1 | docker login --username AWS --password-stdin $ECR_ROOT

pushd lambda
    docker build -t $ECR_ROOT/$REPO:latest .
    docker push $ECR_ROOT/$REPO:latest
popd
