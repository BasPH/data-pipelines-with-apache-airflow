FROM python:3.8-slim

RUN apt-get update && \
    apt-get install -y --no-install-recommends wget unzip && \
    wget http://files.grouplens.org/datasets/movielens/ml-20m.zip && \
    unzip ml-20m.zip && \
    mv /ml-20m/ratings.csv / && \
    rm ml-20m.zip && \
    rm -r /ml-20m && \
    rm -rf /var/lib/apt/lists/*

COPY requirements.txt /tmp/requirements.txt
RUN pip install -r /tmp/requirements.txt && rm -f /tmp/requirements.txt

COPY app.py /

EXPOSE 5000

ENTRYPOINT ["python"]
CMD ["/app.py"]
