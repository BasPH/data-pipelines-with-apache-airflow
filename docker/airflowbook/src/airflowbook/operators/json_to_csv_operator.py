import csv
import json

from airflow.models import BaseOperator


class JsonToCsvOperator(BaseOperator):
    def __init__(self, input_path, output_path, **kwargs):
        super().__init__(**kwargs)
        self._input_path = input_path
        self._output_path = output_path

    def execute(self, context):
        # Read input CSV
        with open(self._input_path, "r") as json_file:
            data = json.load(json_file)

        # Get columns
        columns = {key for row in data for key in row.keys()}

        # Write output JSON
        with open(self._output_path, mode="w") as csv_file:
            writer = csv.DictWriter(csv_file, fieldnames=columns)
            writer.writeheader()
            writer.writerows(data)
