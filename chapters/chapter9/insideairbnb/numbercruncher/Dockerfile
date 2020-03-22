FROM continuumio/miniconda3:4.7.12

RUN pip install pandas==1.0.1 minio==5.0.7

COPY crunchdata.py /root/crunchdata.py

ENTRYPOINT ["python"]
CMD ["/root/crunchdata.py"]
