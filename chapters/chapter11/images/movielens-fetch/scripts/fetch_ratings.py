#!/usr/bin/env python

import json
from pathlib import Path

import click
import requests


@click.command()
@click.option("--start_date", type=click.DateTime(formats=["%Y-%m-%d"]), required=True)
@click.option("--end_date", type=click.DateTime(formats=["%Y-%m-%d"]), required=True)
@click.option("--output_path", type=click.Path(dir_okay=False), required=True)
@click.option("--host", type=str, default="http://movielens:5000")
@click.option("--user", type=str, envvar="MOVIELENS_USER", required=True)
@click.option("--password", type=str, envvar="MOVIELENS_PASSWORD", required=True)
@click.option("--batch_size", type=int, default=100)
def main(start_date, end_date, output_path, host, user, password, batch_size):
    output_path = Path(output_path)

    # Setup session.
    session = requests.Session()
    session.auth = (user, password)

    # Fetch ratings.
    print("Fetching ratings from %s (user: %s)" % (host, user))

    ratings = list(
        _get_ratings(
            session=session,
            host=host,
            start_date=start_date,
            end_date=end_date,
            batch_size=batch_size,
        )
    )
    print("Retrieved %d ratings!" % len(ratings))

    # Write output.
    output_dir = output_path.parent
    output_dir.mkdir(parents=True, exist_ok=True)

    print("Writing to %s" % output_path)
    with output_path.open("w") as file_:
        json.dump(ratings, file_)


def _get_ratings(session, host, start_date, end_date, batch_size=100):
    yield from _get_with_pagination(
        session=session,
        url=host + "/ratings",
        params={
            "start_date": start_date.strftime("%Y-%m-%d"),
            "end_date": end_date.strftime("%Y-%m-%d"),
        },
        batch_size=batch_size,
    )


def _get_with_pagination(session, url, params, batch_size=100):
    """
    Fetches records using a get request with given url/params,
    taking pagination into account.
    """

    offset = 0
    total = None
    while total is None or offset < total:
        response = session.get(
            url, params={**params, **{"offset": offset, "limit": batch_size}}
        )
        response.raise_for_status()
        response_json = response.json()

        yield from response_json["result"]

        offset += batch_size
        total = response_json["total"]


if __name__ == "__main__":
    main()
