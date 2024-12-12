# Stage 1: Build stage
FROM --platform=linux/arm64 public.ecr.aws/lambda/python:3.12 AS build

# Install necessary packages and download ffmpeg
RUN microdnf install -y tar gzip xz && \
    curl -O https://johnvansickle.com/ffmpeg/releases/ffmpeg-release-arm64-static.tar.xz && \
    tar -xJf ffmpeg-release-arm64-static.tar.xz && \
    mv ffmpeg-*/ffmpeg /usr/local/bin/ && \
    mv ffmpeg-*/ffprobe /usr/local/bin/ && \
    rm -rf ffmpeg-* && \
    microdnf clean all

# Install yt-dlp and boto3
RUN pip install --no-cache-dir yt-dlp boto3

# Stage 2: Final stage
FROM --platform=linux/arm64 public.ecr.aws/lambda/python:3.12

# Copy ffmpeg and ffprobe from the build stage
COPY --from=build /usr/local/bin/ffmpeg /usr/local/bin/
COPY --from=build /usr/local/bin/ffprobe /usr/local/bin/

# Copy installed Python packages from the build stage
COPY --from=build /var/lang/lib/python3.12/site-packages /var/lang/lib/python3.12/site-packages

# Copy the handler script
COPY handler.py ${LAMBDA_TASK_ROOT}

# Set the CMD to your handler
CMD ["handler.lambda_handler"]
