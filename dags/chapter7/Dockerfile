FROM python:3.8-slim

RUN pip install Flask==1.1.1 pandas==0.25.2 && \
    apt-get update && \
    apt-get install -y wget unzip && \
    wget http://files.grouplens.org/datasets/movielens/ml-20m.zip && \
    unzip ml-20m.zip && \
    mv /ml-20m/ratings.csv / && \
    rm ml-20m.zip && \
    rm -r /ml-20m && \
    rm -rf /var/lib/apt/lists/*

COPY api.py /

EXPOSE 5000

ENTRYPOINT ["python"]
CMD ["api.py"]
