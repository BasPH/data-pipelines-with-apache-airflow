import os
from datetime import date

import psycopg2
import psycopg2.extras
from flask import Flask, jsonify, Response
from flask_httpauth import HTTPBasicAuth
from werkzeug.security import generate_password_hash, check_password_hash

app = Flask(__name__)
auth = HTTPBasicAuth()

users = {"citibike": generate_password_hash("cycling")}


@auth.verify_password
def verify_password(username, password):
    if username in users and check_password_hash(users.get(username), password):
        return username


@app.route("/")
@auth.login_required
def index():
    return "Welcome to Citi Bike!"


@app.route("/recent/<period>", defaults={"amount": 1}, methods=["GET"])
@app.route("/recent/<period>/<amount>", methods=["GET"])
@auth.login_required
def get_recent_rides(period: str, amount: int):
    """
    Return
    :param period: either "minute", "hour", or "day"
    :param amount: (optional) the number of periods
    :return:
    """
    if period not in ("minute", "hour", "day"):
        return Response("Period can only be 'minute', 'hour', or 'day'!", status=422)

    conn = psycopg2.connect(
        database=os.environ["POSTGRES_DATABASE"],
        user=os.environ["POSTGRES_USERNAME"],
        host=os.environ["POSTGRES_HOST"],
        password=os.environ["POSTGRES_PASSWORD"],
    )
    cursor = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
    year_offset = date.today().year - int(os.environ["DATA_YEAR"])
    cursor.execute(
        f"""SELECT
tripduration,
starttime + INTERVAL '{year_offset} YEARS' AS starttime,
stoptime + INTERVAL '{year_offset} YEARS' AS stoptime,
start_station_id,
start_station_name,
start_station_latitude,
start_station_longitude,
end_station_id,
end_station_name,
end_station_latitude,
end_station_longitude
from tripdata
WHERE stoptime + interval '{year_offset} YEARS' <= NOW()
AND stoptime + interval '{year_offset} YEARS' >= NOW() - interval '{amount} {period}s';"""
    )
    data = cursor.fetchall()
    return jsonify(data)


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000)
