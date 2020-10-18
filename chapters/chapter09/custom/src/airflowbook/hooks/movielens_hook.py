from airflow.hooks.base_hook import BaseHook


class MovielensHook(BaseHook):
    """Hook for our MovieLens API."""

    def __init__(self, conn_id, retry=3):
        super().__init__(source=None)
        self._conn_id = conn_id
        self._retry = retry

        self._session = None

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()

    def get_conn(self):
        if self._session is None:
            # Fetch config for the given connection (host, login, etc).
            conn_config = self.get_connection(self._conn_id)
            self._session = self._build_session(
                auth=(conn_config.login, conn_config.password), retry=self._retry
            )
        return self._session

    @staticmethod
    def _build_session(auth=None, headers=None, retry=None):
        """
        Helper method for building a requests Session.

        Parameters
        ----------
        auth : Tuple[str]
            Credentials (user, pass) to use for basic HTTP authentication.
        headers : Dict[str, str]
            Headers to include in every request.
        retry : Union[int, requests.packages.urllib3.util.retry.Retry]
            Configuration to use for handling retries when errors occur during
            requests. Can be an int to specify the number of retries to perform
            using default settings, or a Retry object which allows you to configure
            more details such as which status codes to retry for, etc. See the
            documentation of the Retry class for more details.
        """
        import requests
        from requests.adapters import HTTPAdapter

        session = requests.Session()

        if auth:
            session.auth = auth

        if headers:
            session.headers.update(headers)

        if retry:
            adapter = HTTPAdapter(max_retries=retry)
            session.mount("http://", adapter)
            session.mount("https://", adapter)

        return session

    def close(self):
        if self._session:
            self._session.close()
            self._session = None

    # API methods:
    def get_movies(self):
        raise NotImplementedError()

    def get_users(self):
        raise NotImplementedError()

    def get_ratings(self, start_date=None, end_date=None, batch_size=100):
        """Fetches ratings between the given start/end date."""

        yield from self._get_with_pagination(
            url="http://localhost:5000/ratings",
            params={"start_date": start_date, "end_date": end_date},
            batch_size=batch_size,
        )

    def _get_with_pagination(self, url, params, batch_size=100):
        """
        Fetches records using a get request with given url/params,
        taking pagination into account.
        """

        session = self.get_conn()

        offset = 0
        total = None
        while total is None or offset < total:
            response = session.get(
                url, params={**params, **{"offset": offset, "limit": batch_size}}
            )
            response.raise_for_status()
            response_json = response.json()

            yield from response_json["result"]

            offset += batch_size
            total = response_json["total"]
