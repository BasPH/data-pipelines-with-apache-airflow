FROM python:3.8-slim

RUN python -m pip install click==7.1.1 requests==2.23.0

COPY scripts/fetch_ratings.py /usr/local/bin/fetch-ratings
RUN chmod +x /usr/local/bin/fetch-ratings

ENV PATH="/usr/local/bin:${PATH}"
