from builtins import super
from itertools import chain

from airflow.hooks.base_hook import BaseHook


class MovielensHook(BaseHook):
    """
    Hook for the MovieLens API.

    Abstracts details of the Movielens (REST) API and provides several convenience
    methods for fetching data (e.g. ratings, users, movies) from the API. Also
    provides support for automatic retries of failed requests, transparent
    handling of pagination, authentication, etc.

    Parameters
    ----------
    conn_id : str
        ID of the connection to use to connect to the Movielens API. Connection
        is expected to include authentication details (login/password) and the
        host that is serving the API.

    """

    DEFAULT_SCHEMA = "http"
    DEFAULT_PORT = 5000

    def __init__(self, conn_id, retry=3):
        super().__init__(source=None)
        self._conn_id = conn_id
        self._retry = retry

        self._session = None
        self._host = None

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()

    def get_conn(self):
        """
        Returns the connection used by the hook for querying data.
        Should in principle not be used directly.
        """

        if self._session is None:
            # Fetch config for the given connection (host, login, etc).
            config = self.get_connection(self._conn_id)

            if not config.host:
                raise ValueError(f"No host specified in connection {self._conn_id}")

            schema = config.schema or self.DEFAULT_SCHEMA
            port = config.port or self.DEFAULT_PORT

            host = f"{schema}://{config.host}:{port}"

            # Build our session instance, which we will use for any
            # requests to the API.
            self._session = HttpSession(
                user=config.login,
                password=config.password,
                host=host
            )
        return self._session

    def close(self):
        """Closes any active session."""
        if self._session:
            self._session.close()
        self._session = None

    # API methods:

    def get_movies(self):
        """Fetches a list of movies."""
        raise NotImplementedError()

    def get_users(self):
        """Fetches a list of users."""
        raise NotImplementedError()

    def get_ratings(self, start_date=None, end_date=None, batch_size=100):
        """
        Fetches ratings between the given start/end date.

        Parameters
        ----------
        start_date : str
            Start date to start fetching ratings from (inclusive). Expected
            format is YYYY-MM-DD (equal to Airflow's ds formats).
        end_date : str
            End date to fetching ratings up to (exclusive). Expected
            format is YYYY-MM-DD (equal to Airflow's ds formats).
        batch_size : int
            Size of the batches (pages) to fetch from the API. Larger values
            mean less requests, but more data transferred per request.
        """

        yield from self._get_with_pagination(
            url="/ratings",
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


class HttpSession:
    """
    Helper class wrapping a requests session with extra functionality, including:

        - Basic auth using user/password (optional).
        - Automatic retries (optional).
        - A default host - meaning we can do requests with path URLs
            (e.g., /ratings) without having to specify the full URL (optional).

    The main benefit of this class for Airflow is the default host functionality,
    which means we can keep the entire config for the connection in one place,
    instead of requiring separate session/base_url parameters.
    """

    def __init__(self, user=None, password=None, host=None, retry=None, headers=None):
        # Make sure that we have a full URL, including schema.
        if host and not host.startswith("http"):
            host = "http://" + host

        # Remove any trailing slashes.
        host = host.rstrip("/")

        self._user = user
        self._password = password
        self._host = host
        self._retry = retry
        self._headers = headers or {}

        self._session = None

    def _get_session(self):
        if self._session is None:
            self._session = self._build_session()
        return self._session

    def _build_session(self):
        import requests
        from requests.adapters import HTTPAdapter

        session = requests.Session()

        if self._user:
            session.auth = (self._user, self._password)

        if self._headers:
            session.headers.update(self._headers)

        if self._retry:
            adapter = HTTPAdapter(max_retries=self._retry)
            session.mount("http://", adapter)
            session.mount("https://", adapter)

        return session

    def get(self, url_or_endpoint, **kwargs):
        """
        Performs a GET request.

        Parameters
        ----------
        url_or_endpoint : str
            (Partial) URL pointing the the address to be queried. If a full URL is
            given, the request is performed directly to that address. If a partial URL
            is given (e.g. /index.html), then the partial URL is supplemented
            supplemented with the host base URL (e.g., if the host URL is
            http://example.com, the actual request is sent to
            http://example.com/index.html). Note that an error is raised for partial
            URLs if the session does not have a host defined.
        **kwargs
            Any kwargs are passed to requests.Session.get.
        """

        url = self._build_url(url_or_endpoint)

        session = self._get_session()
        response = session.get(url, **kwargs)

        return response

    def _build_url(self, url_or_endpoint):
        if url_or_endpoint.startswith("http"):
            # Passed value is a full URL.
            url = url_or_endpoint
        else:
            # Build full URL using host + given end point path
            if not self._host:
                raise ValueError("URLs must start with http if no host is given!")
            url = self._host + url_or_endpoint
        return url

    def post(self, url_or_endpoint, **kwargs):
        """
        Performs a POST request.

        Parameters
        ----------
        url_or_endpoint : str
            (Partial) URL pointing the the address to be queried. If a full URL is
            given, the request is performed directly to that address. If a partial URL
            is given (e.g. /index.html), then the partial URL is supplemented
            supplemented with the host base URL (e.g., if the host URL is
            http://example.com, the actual request is sent to
            http://example.com/index.html). Note that an error is raised for partial
            URLs if the session does not have a host defined.
        **kwargs
            Any kwargs are passed to requests.Session.post.
        """

        url = self._build_url(url_or_endpoint)

        session = self._get_session()
        response = session.post(url, **kwargs)

        return response

    def close(self):
        """Closes any active session."""
        if self._session:
            self._session.close()
        self._session = None
