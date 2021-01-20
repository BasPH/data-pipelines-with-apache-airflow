import time

from airflow.providers.amazon.aws.hooks.base_aws import AwsBaseHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults


class GlueTriggerCrawlerOperator(BaseOperator):
    """
    Operator that triggers a crawler run in AWS Glue.

    Parameters
    ----------
    aws_conn_id
        Connection to use for connecting to AWS. Should have the appropriate
        permissions (Glue:StartCrawler and Glue:GetCrawler) in AWS.
    crawler_name
        Name of the crawler to trigger.
    region_name
        Name of the AWS region in which the crawler is located.
    kwargs
        Any kwargs are passed to the BaseOperator.
    """

    @apply_defaults
    def __init__(
        self, aws_conn_id: str, crawler_name: str, region_name: str = None, **kwargs
    ):
        super().__init__(**kwargs)
        self._aws_conn_id = aws_conn_id
        self._crawler_name = crawler_name
        self._region_name = region_name

    def execute(self, context):
        hook = AwsBaseHook(
            self._aws_conn_id, client_type="glue", region_name=self._region_name
        )
        glue_client = hook.get_conn()

        self.log.info("Triggering crawler")
        response = glue_client.start_crawler(Name=self._crawler_name)

        if response["ResponseMetadata"]["HTTPStatusCode"] != 200:
            raise RuntimeError(
                "An error occurred while triggering the crawler: %r" % response
            )

        self.log.info("Waiting for crawler to finish")
        while True:
            time.sleep(1)

            crawler = glue_client.get_crawler(Name=self._crawler_name)
            crawler_state = crawler["Crawler"]["State"]

            if crawler_state == "READY":
                self.log.info("Crawler finished running")
                break
