from flask import Flask, Response, request

app = Flask(__name__)
secret_token = "supersecret"


@app.route("/", methods=["GET"])
def index():
    token = request.headers.get("token")
    if token == secret_token:
        return Response(response="Welcome!", status=200)
    else:
        return Response(response="Who's this?", status=401)


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000, debug=True)
