"""Module containing file system sensors."""

from airflow.sensors.base_sensor_operator import BaseSensorOperator
from airflow.utils.decorators import apply_defaults

from airflow_fs.hooks import LocalHook


class FileSensor(BaseSensorOperator):
    """Sensor that waits for files matching a given file pattern.

    :param str path: File path to match files to. Can be any valid
        glob pattern.
    :param FsHook hook: File system hook to use when looking for files.
    """

    template_fields = ("file_pattern",)

    @apply_defaults
    def __init__(self, path, hook=None, **kwargs):
        super(FileSensor, self).__init__(**kwargs)

        self._path = path
        self._hook = hook or LocalHook()

    # pylint: disable=unused-argument,missing-docstring
    def poke(self, context):
        with self._hook as hook:
            if hook.glob(self._path):
                return True
            return False
