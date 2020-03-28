FROM python:3.8-slim

COPY requirements.txt /tmp/requirements.txt
RUN pip install -r /tmp/requirements.txt

COPY scripts /etc/movielens/scripts
RUN chmod +x /etc/movielens/scripts/*

ENV PATH="/etc/movielens/scripts:${PATH}"

# RUN useradd -ms /bin/bash movielens
# USER movielens
