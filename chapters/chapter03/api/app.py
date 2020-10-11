from datetime import date, datetime, timedelta
import time

from numpy import random
import pandas as pd
from faker import Faker

from flask import Flask, jsonify, request


def _generate_events(end_date):
    """Generates a fake dataset with events for 30 days before end date."""

    events = pd.concat(
        [
            _generate_events_for_day(date=end_date - timedelta(days=(30 - i)))
            for i in range(30)
        ],
        axis=0,
    )

    return events


def _generate_events_for_day(date):
    """Generates events for a given day."""

    # Use date as seed.
    seed = int(time.mktime(date.timetuple()))

    Faker.seed(seed)
    random_state = random.RandomState(seed)

    # Determine how many users and how many events we will have.
    n_users = random_state.randint(low=50, high=100)
    n_events = random_state.randint(low=200, high=2000)

    # Generate a bunch of users.
    fake = Faker()
    users = [fake.ipv4() for _ in range(n_users)]

    return pd.DataFrame(
        {
            "user": random_state.choice(users, size=n_events, replace=True),
            "date": pd.to_datetime(date),
        }
    )


app = Flask(__name__)
app.config["events"] = _generate_events(end_date=date(year=2019, month=1, day=5))


@app.route("/events")
def events():
    start_date = _str_to_datetime(request.args.get("start_date", None))
    end_date = _str_to_datetime(request.args.get("end_date", None))

    events = app.config.get("events")

    if start_date is not None:
        events = events.loc[events["date"] >= start_date]

    if end_date is not None:
        events = events.loc[events["date"] < end_date]

    return jsonify(events.to_dict(orient="records"))


def _str_to_datetime(value):
    if value is None:
        return None
    return datetime.strptime(value, "%Y-%m-%d")


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000)
