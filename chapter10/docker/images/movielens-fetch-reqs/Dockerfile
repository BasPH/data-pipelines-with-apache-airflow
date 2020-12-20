FROM python:3.8-slim

COPY requirements.txt /tmp/requirements.txt
RUN python -m pip install -r /tmp/requirements.txt

COPY scripts/fetch_ratings.py /usr/local/bin/fetch-ratings
RUN chmod +x /usr/local/bin/fetch-ratings

ENV PATH="/usr/local/bin:${PATH}"
