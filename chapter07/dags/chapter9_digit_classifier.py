import gzip
import io
import pickle

import airflow.utils.dates
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.amazon.aws.operators.s3_copy_object import S3CopyObjectOperator
from airflow.providers.amazon.aws.operators.sagemaker_endpoint import (
    SageMakerEndpointOperator,
)
from airflow.providers.amazon.aws.operators.sagemaker_training import (
    SageMakerTrainingOperator,
)
from sagemaker.amazon.common import write_numpy_to_dense_tensor

dag = DAG(
    dag_id="chapter9_aws_handwritten_digit_classifier",
    schedule_interval=None,
    start_date=airflow.utils.dates.days_ago(3),
)

download_mnist_data = S3CopyObjectOperator(
    task_id="download_mnist_data",
    source_bucket_name="sagemaker-sample-data-eu-west-1",
    source_bucket_key="algorithms/kmeans/mnist/mnist.pkl.gz",
    dest_bucket_name="your-bucket",
    dest_bucket_key="mnist.pkl.gz",
    dag=dag,
)


def _extract_mnist_data():
    s3hook = S3Hook()

    # Download S3 dataset into memory
    mnist_buffer = io.BytesIO()
    mnist_obj = s3hook.get_key(bucket_name="your-bucket", key="mnist.pkl.gz")
    mnist_obj.download_fileobj(mnist_buffer)

    # Unpack gzip file, extract dataset, convert to dense tensor, upload back to S3
    mnist_buffer.seek(0)
    with gzip.GzipFile(fileobj=mnist_buffer, mode="rb") as f:
        train_set, _, _ = pickle.loads(f.read(), encoding="latin1")
        output_buffer = io.BytesIO()
        write_numpy_to_dense_tensor(
            file=output_buffer, array=train_set[0], labels=train_set[1]
        )
        output_buffer.seek(0)
        s3hook.load_file_obj(
            output_buffer, key="mnist_data", bucket_name="your-bucket", replace=True
        )


extract_mnist_data = PythonOperator(
    task_id="extract_mnist_data", python_callable=_extract_mnist_data, dag=dag
)

sagemaker_train_model = SageMakerTrainingOperator(
    task_id="sagemaker_train_model",
    config={
        "TrainingJobName": "mnistclassifier-{{ execution_date.strftime('%Y-%m-%d-%H-%M-%S') }}",
        "AlgorithmSpecification": {
            "TrainingImage": "438346466558.dkr.ecr.eu-west-1.amazonaws.com/kmeans:1",
            "TrainingInputMode": "File",
        },
        "HyperParameters": {"k": "10", "feature_dim": "784"},
        "InputDataConfig": [
            {
                "ChannelName": "train",
                "DataSource": {
                    "S3DataSource": {
                        "S3DataType": "S3Prefix",
                        "S3Uri": "s3://your-bucket/mnist_data",
                        "S3DataDistributionType": "FullyReplicated",
                    }
                },
            }
        ],
        "OutputDataConfig": {"S3OutputPath": "s3://your-bucket/mnistclassifier-output"},
        "ResourceConfig": {
            "InstanceType": "ml.c4.xlarge",
            "InstanceCount": 1,
            "VolumeSizeInGB": 10,
        },
        "RoleArn": (
            "arn:aws:iam::297623009465:role/service-role/"
            "AmazonSageMaker-ExecutionRole-20180905T153196"
        ),
        "StoppingCondition": {"MaxRuntimeInSeconds": 24 * 60 * 60},
    },
    wait_for_completion=True,
    print_log=True,
    check_interval=10,
    dag=dag,
)

sagemaker_deploy_model = SageMakerEndpointOperator(
    task_id="sagemaker_deploy_model",
    operation="update",
    wait_for_completion=True,
    config={
        "Model": {
            "ModelName": "mnistclassifier-{{ execution_date.strftime('%Y-%m-%d-%H-%M-%S') }}",
            "PrimaryContainer": {
                "Image": "438346466558.dkr.ecr.eu-west-1.amazonaws.com/kmeans:1",
                "ModelDataUrl": (
                    "s3://your-bucket/mnistclassifier-output/mnistclassifier"
                    "-{{ execution_date.strftime('%Y-%m-%d-%H-%M-%S') }}/"
                    "output/model.tar.gz"
                ),  # this will link the model and the training job
            },
            "ExecutionRoleArn": (
                "arn:aws:iam::297623009465:role/service-role/"
                "AmazonSageMaker-ExecutionRole-20180905T153196"
            ),
        },
        "EndpointConfig": {
            "EndpointConfigName": "mnistclassifier-{{ execution_date.strftime('%Y-%m-%d-%H-%M-%S') }}",
            "ProductionVariants": [
                {
                    "InitialInstanceCount": 1,
                    "InstanceType": "ml.t2.medium",
                    "ModelName": "mnistclassifier",
                    "VariantName": "AllTraffic",
                }
            ],
        },
        "Endpoint": {
            "EndpointConfigName": "mnistclassifier-{{ execution_date.strftime('%Y-%m-%d-%H-%M-%S') }}",
            "EndpointName": "mnistclassifier",
        },
    },
    dag=dag,
)

download_mnist_data >> extract_mnist_data >> sagemaker_train_model >> sagemaker_deploy_model
