FROM apache/airflow:1.10.11-python3.7

USER root

# pyarrow required for writing to_parquet() with Pandas
# minio required for communicating with MinIO
# geopandas & pygeos for mapping lat lon coordinates to NYC taxi zone ids
RUN pip install \
    pyarrow==0.17.1 \
    minio==5.0.10 \
    geopandas==0.8.0 \
    pygeos==0.7.1

USER airflow
