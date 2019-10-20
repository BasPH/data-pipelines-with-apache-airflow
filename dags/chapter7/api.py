import pandas as pd
from flask import Flask, g

app = Flask(__name__)
app.config["ratings"] = pd.read_csv("/ratings.csv")


@app.route("/")
def hello():
    return "Hello world!"


@app.route("/test")
def test():
    return str(app.config.get("ratings").shape)


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000)
