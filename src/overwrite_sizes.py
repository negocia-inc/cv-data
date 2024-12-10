import argparse
from pathlib import Path

import pandas as pd
from PIL import Image, ImageFile

ImageFile.LOAD_TRUNCATED_IMAGES = True


def create_parser() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Overwrite height and width in CSVs with actual values from image directory."
    )
    parser.add_argument(
        "csv_path",
        type=str,
        help="csv with wrong image sizes",
    )
    parser.add_argument(
        "image_dir",
        type=str,
        help="corresponding image dir",
    )
    args = parser.parse_args()
    return args

# TO DO 動画対応
def main(csv_path, image_dir):
    df = pd.read_csv(csv_path, compression="gzip")
    image_sizes = []
    for image_path in Path(image_dir).glob("**/*"):
        if image_path.stat().st_size <= 0:
            print(f"{image_path} is empty")
            continue
        try:
            w, h = Image.open(image_path).size
            image_sizes.append((str(image_path.stem), w, h))
        except:
            print(f"{image_path} could not be opened with PIL")
    sizes = pd.DataFrame(image_sizes, columns=["id", "width", "height"])
    df["id"] = df["id"].astype(str)
    df.update(sizes)
    print(df.columns)

    df.to_csv("test.csv.gz", compression="gzip", index = False)


if __name__ == "__main__":
    args = create_parser()
    main(args.csv_path, args.image_dir)