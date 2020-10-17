from pathlib import Path

import pandas as pd

DATA_DIR = Path("/data")


def fetch_ratings(year, month):
    """Fetches ratings for a given month/year."""

    try:
        ratings = pd.read_csv(DATA_DIR / "ratings" / str(year) / f"{month:02d}.csv")
    except FileNotFoundError:
        ratings = pd.DataFrame.from_records(
            [], columns=["userId", "movieId", "rating", "timestamp"]
        )

    return ratings
