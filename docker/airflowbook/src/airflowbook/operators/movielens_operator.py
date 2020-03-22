import json
from collections import Counter, defaultdict

from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator

from airflowbook.hooks.movielens_hook import MovielensHook


class MovielensDownloadOperator(BaseOperator):
    template_fields = ("_start_date", "_end_date", "_output_path")

    def __init__(self, conn_id, start_date, end_date, output_path, **kwargs):
        super().__init__(**kwargs)
        self._conn_id = conn_id
        self._start_date = start_date
        self._end_date = end_date
        self._output_path = output_path

    def execute(self, context):
        with MovielensHook(self._conn_id) as hook:
            ratings = list(
                hook.get_ratings(start_date=self._start_date, end_date=self._end_date)
            )

        with open(self._output_path, "w") as f:
            f.write(json.dumps(ratings))


class MovielensPopularityOperator(BaseOperator):
    def __init__(self, conn_id, start_date, end_date, min_ratings=4, top_n=5, **kwargs):
        super().__init__(**kwargs)
        self._conn_id = conn_id
        self._start_date = start_date
        self._end_date = end_date
        self._min_ratings = min_ratings
        self._top_n = top_n

    def execute(self, context):
        with MovielensHook(self._conn_id) as hook:
            ratings = hook.get_ratings(
                start_date=self._start_date, end_date=self._end_date
            )

            rating_sums = defaultdict(Counter)
            for rating in ratings:
                rating_sums[rating["movieId"]].update(count=1, rating=rating["rating"])

            averages = {
                movie_id: (
                    rating_counter["rating"] / rating_counter["count"],
                    rating_counter["count"],
                )
                for movie_id, rating_counter in rating_sums.items()
                if rating_counter["count"] >= self._min_ratings
            }
            return sorted(averages.items(), key=lambda x: x[1], reverse=True)[
                : self._top_n
            ]


class MovielensToPostgresOperator(BaseOperator):
    template_fields = ("_start_date", "_end_date", "_insert_query")

    def __init__(
        self,
        movielens_conn_id,
        start_date,
        end_date,
        postgres_conn_id,
        insert_query,
        **kwargs
    ):
        super().__init__(**kwargs)
        self._movielens_conn_id = movielens_conn_id
        self._start_date = start_date
        self._end_date = end_date
        self._postgres_conn_id = postgres_conn_id
        self._insert_query = insert_query

    def execute(self, context):
        with MovielensHook(self._movielens_conn_id) as movielens_hook:
            ratings = list(
                movielens_hook.get_ratings(
                    start_date=self._start_date, end_date=self._end_date
                )
            )

        import pdb

        pdb.set_trace()

        postgres_hook = PostgresHook(postgres_conn_id=self._postgres_conn_id)
        insert_queries = [
            self._insert_query.format(
                ",".join([str(_[1]) for _ in sorted(rating.items())])
            )
            for rating in ratings
        ]
        postgres_hook.run(insert_queries)
