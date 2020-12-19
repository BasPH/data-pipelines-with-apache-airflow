import logging
from pathlib import Path
import tempfile
from urllib.request import urlretrieve
import zipfile

import pandas as pd

logging.basicConfig(
    format="[%(asctime)-15s] %(levelname)s - %(message)s", level=logging.INFO
)


def main():
    logging.info("Fetching ratings...")
    ratings = fetch_ratings("http://files.grouplens.org/datasets/movielens/ml-20m.zip")

    logging.info("Exporting ratings by year/month...")
    write_partitioned(ratings, Path("data") / "ratings")


def fetch_ratings(url):
    """Fetches ml-20m ratings from the given URL."""

    with tempfile.TemporaryDirectory() as tmp_dir:
        tmp_path = Path(tmp_dir, "download.zip")
        logging.info(f"Downloading zip file from {url}")
        urlretrieve(url, tmp_path)

        with zipfile.ZipFile(tmp_path) as zip_:
            logging.info(f"Downloaded zip file with contents: {zip_.namelist()}")

            logging.info("Reading ml-20m/ratings.csv from zip file")
            with zip_.open("ml-20m/ratings.csv") as file_:
                ratings = pd.read_csv(file_)

    return ratings


def write_partitioned(ratings, output_dir):
    """Writes ratings partitioned by year/month."""

    dates = pd.to_datetime(ratings["timestamp"], unit="s")

    for (year, month), grp in ratings.groupby([dates.dt.year, dates.dt.month]):
        output_path = output_dir / str(year) / f"{month:02d}.csv"
        output_path.parent.mkdir(parents=True, exist_ok=True)
        grp.to_csv(output_path, index=False)


if __name__ == "__main__":
    main()
