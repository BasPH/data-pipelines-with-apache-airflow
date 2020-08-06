from pathlib import Path

import pandas as pd

DATA_DIR = Path("/data")


def fetch_ratings(year, month):
    """Fetches ratings for a given month/year."""

    try:
        ratings = pd.read_csv(DATA_DIR / "ratings" / str(year) / f"{month:02d}.csv")
    except FileNotFoundError:
        raise ValueError(f"No ratings were found for {year}/{month}")

    return ratings
