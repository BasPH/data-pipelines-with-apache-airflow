ARG AIRFLOW_BASE_IMAGE="apache/airflow:2.0.0-python3.8"
FROM ${AIRFLOW_BASE_IMAGE}

RUN pip install --user --no-cache-dir \
    apache-airflow-providers-docker==1.0.0

# Note we need to run Airflow as root in this case, as Airflow needs to
# have sufficient priviledges to access /var/run/docker.sock.
#
# In principle, it would be better to (1) create a docker group,
# (2) add the airflow user to that group and (3) chmod
# /var/run/docker.sock so that the docker group has access. However,
# this is tricky to do properly in this kind of docker-in-docker setup.
USER root

# To make sure that the root user can find the packages that were
# installed as the airflow user, we amend the Python path to include
# the airflow user's site-packages.
ENV PYTHONPATH=/home/airflow/.local/lib/python3.8/site-packages
