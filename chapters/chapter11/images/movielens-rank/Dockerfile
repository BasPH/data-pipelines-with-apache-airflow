FROM python:3.8-slim

RUN pip install click==7.1.1 pandas==1.0.3

COPY scripts/rank_movies.py /usr/local/bin/rank-movies
RUN chmod +x /usr/local/bin/rank-movies

ENV PATH="/usr/local/bin:${PATH}"
