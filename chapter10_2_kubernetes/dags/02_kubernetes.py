import datetime as dt
import os

from kubernetes.client import models as k8s

from airflow import DAG
from airflow.providers.cncf.kubernetes.operators.kubernetes_pod import (
    KubernetesPodOperator,
)

with DAG(
    dag_id="02_kubernetes",
    description="Fetches ratings from the Movielens API using kubernetes.",
    start_date=dt.datetime(2019, 1, 1),
    end_date=dt.datetime(2019, 1, 3),
    schedule_interval="@daily",
    default_args={"depends_on_past": True},
) as dag:

    volume_claim = k8s.V1PersistentVolumeClaimVolumeSource(claim_name="data-volume")
    volume = k8s.V1Volume(name="data-volume", persistent_volume_claim=volume_claim)

    volume_mount = k8s.V1VolumeMount(
        name="data-volume", mount_path="/data", sub_path=None, read_only=False
    )

    fetch_ratings = KubernetesPodOperator(
        task_id="fetch_ratings",
        # Airflow 2.0.0a2 has a bug that results in the pod operator not applying
        # the image pull policy. By default, the k8s SDK uses a policy of always
        # pulling the image when using the latest tag, but only pulling an image if
        # it's not present (what we want) when using a different tag. For now, we
        # use this behaviour to get our desired image policy behaviour.
        #
        # TODO: Remove this workaround when the bug is fixed.
        #       See https://github.com/apache/airflow/issues/11998.
        #
        image="manning-airflow/movielens-fetch:k8s",
        cmds=["fetch-ratings"],
        arguments=[
            "--start_date",
            "{{ds}}",
            "--end_date",
            "{{next_ds}}",
            "--output_path",
            "/data/ratings/{{ds}}.json",
            "--user",
            os.environ["MOVIELENS_USER"],
            "--password",
            os.environ["MOVIELENS_PASSWORD"],
            "--host",
            os.environ["MOVIELENS_HOST"],
        ],
        namespace="airflow",
        name="fetch-ratings",
        cluster_context="docker-desktop",
        in_cluster=False,
        volumes=[volume],
        volume_mounts=[volume_mount],
        image_pull_policy="Never",
        is_delete_operator_pod=True,
    )

    rank_movies = KubernetesPodOperator(
        task_id="rank_movies",
        image="manning-airflow/movielens-rank:k8s",
        cmds=["rank-movies"],
        arguments=[
            "--input_path",
            "/data/ratings/{{ds}}.json",
            "--output_path",
            "/data/rankings/{{ds}}.csv",
        ],
        namespace="airflow",
        name="rank-movies",
        cluster_context="docker-desktop",
        in_cluster=False,
        volumes=[volume],
        volume_mounts=[volume_mount],
        image_pull_policy="Never",
        is_delete_operator_pod=True,
    )

    fetch_ratings >> rank_movies
