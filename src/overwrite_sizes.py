import argparse
from pathlib import Path

import cv2
import pandas as pd
from loguru import logger
from PIL import Image, ImageFile

ImageFile.LOAD_TRUNCATED_IMAGES = True


def create_parser() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Overwrite height and width in CSVs with actual values from creative directory."
    )
    parser.add_argument(
        "csv_path",
        type=str,
        help="csv with wrong creative sizes",
    )
    parser.add_argument(
        "creative_dir",
        type=str,
        help="corresponding creative dir",
    )
    parser.add_argument(
        "save_path",
        type=str,
        help="save csv.gz path",
    )
    parser.add_argument(
        "creative_type",
        type=str,
        choices=["image", "video"],
        help="creative type, either 'image' or 'video'",
    )
    args = parser.parse_args()
    return args


def main(csv_path, creative_dir, save_path, creative_type):
    df = pd.read_csv(csv_path, compression="gzip")
    creative_sizes = []
    for creative_path in Path(creative_dir).glob("**/*"):
        if creative_path.stat().st_size <= 0:
            logger.info(f"{creative_path} is empty")
            continue
        if creative_type == "image":
            try:
                w, h = Image.open(creative_path).size
                creative_sizes.append((str(creative_path.stem), w, h))
            except:
                logger.info(f"{creative_path} could not be opened with PIL")
        elif creative_type == "video":
            try:
                video = cv2.VideoCapture(str(creative_path))
                if not video.isOpened():
                    logger.info(f"{creative_path} could not be opened with OpenCV")
                    continue
                w = int(video.get(cv2.CAP_PROP_FRAME_WIDTH))
                h = int(video.get(cv2.CAP_PROP_FRAME_HEIGHT))
                creative_sizes.append((str(creative_path.stem), w, h))
                video.release()
            except Exception as e:
                logger.info(f"Error processing {creative_path}: {e}")

    sizes = pd.DataFrame(creative_sizes, columns=["id", "width", "height"])
    df["id"] = df["id"].astype(str)
    if "width" in df.columns:
        df = df.drop(columns=["width"])
    if "height" in df.columns:
        df = df.drop(columns=["height"])
    df = df.merge(sizes, on="id", how="left")

    df_drop = df.dropna(subset=["width", "height"], axis=0)
    logger.info(f"Overwrote {len(df) - len(df_drop)} rows with missing creative sizes")

    df_drop.to_csv(save_path, compression="gzip", index=False)
    logger.info(f"Saved to {save_path}")


if __name__ == "__main__":
    args = create_parser()
    main(args.csv_path, args.creative_dir, args.save_path, args.creative_type)
