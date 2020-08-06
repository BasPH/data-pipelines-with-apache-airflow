#! /usr/bin/env python

import click
import requests


@click.command()
@click.argument("city", type=str)
@click.option(
    "--output_path",
    default=None,
    type=click.Path(dir_okay=False, writable=True),
    help="Optional file to write output to.",
)
def fetch_weather(city, output_path):
    """CLI application for fetching weather forecasts from wttr.in."""

    response = requests.get(f"https://v2.wttr.in/{city}")
    response.raise_for_status()

    if output_path:
        with open(output_path, "wb") as file_:
            file_.write(response.content)
    else:
        print(response.content.decode())


if __name__ == "__main__":
    fetch_weather()
