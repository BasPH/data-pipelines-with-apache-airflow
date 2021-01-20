"""Module containing file system sensors."""

from airflow.sensors.base import BaseSensorOperator
from airflow.utils.decorators import apply_defaults

from .hooks import MovielensHook


class MovielensRatingsSensor(BaseSensorOperator):
    """
    Sensor that waits for the Movielens API to have ratings for a time period.

    start_date : str
        (Templated) start date of the time period to check for (inclusive).
        Expected format is YYYY-MM-DD (equal to Airflow's ds formats).
    end_date : str
        (Templated) end date of the time period to check for (exclusive).
        Expected format is YYYY-MM-DD (equal to Airflow's ds formats).
    """

    template_fields = ("_start_date", "_end_date")

    @apply_defaults
    def __init__(self, conn_id, start_date="{{ds}}", end_date="{{next_ds}}", **kwargs):
        super().__init__(**kwargs)
        self._conn_id = conn_id
        self._start_date = start_date
        self._end_date = end_date

    # pylint: disable=unused-argument,missing-docstring
    def poke(self, context):
        hook = MovielensHook(self._conn_id)

        try:
            next(
                hook.get_ratings(
                    start_date=self._start_date, end_date=self._end_date, batch_size=1
                )
            )
            # If no StopIteration is raised, the request returned at least one record.
            # This means that there are records for the given period, which we indicate
            # to Airflow by returning True.
            self.log.info(
                f"Found ratings for {self._start_date} to {self._end_date}, continuing!"
            )
            return True
        except StopIteration:
            self.log.info(
                f"Didn't find any ratings for {self._start_date} "
                f"to {self._end_date}, waiting..."
            )
            # If StopIteration is raised, we know that the request did not find
            # any records. This means that there a no ratings for the time period,
            # so we should return False.
            return False
        finally:
            # Make sure we always close our hook's session.
            hook.close()
