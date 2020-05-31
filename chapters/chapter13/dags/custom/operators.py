import time

from airflow.contrib.hooks.aws_hook import AwsHook
from airflow.models import BaseOperator
from airflow.utils import apply_defaults


class GlueTriggerCrawlerOperator(BaseOperator):
    @apply_defaults
    def __init__(self, aws_conn_id, crawler_name, region_name, wait=False, **kwargs):
        super().__init__(**kwargs)
        self._aws_conn_id = aws_conn_id
        self._crawler_name = crawler_name
        self._region_name = region_name
        self._wait = wait

    def execute(self, context):
        hook = AwsHook(self._aws_conn_id)
        glue_client = hook.get_client_type("glue", region_name=self._region_name)

        self.log.info("Triggering crawler")
        response = glue_client.start_crawler(Name="ratings-crawler")

        if response["ResponseMetadata"]["HTTPStatusCode"] != 200:
            raise RuntimeError(
                "An error occurred while triggering the crawler: %r" % response
            )

        if self._wait:
            self.log.info("Waiting for crawler to finish")
            while True:
                time.sleep(1)

                crawler = glue_client.get_crawler(Name=self._crawler_name)
                crawler_state = crawler["Crawler"]["State"]

                if crawler_state == "READY":
                    self.log.info("Crawler finished running")
                    break
