from typing import (
    Any,
    IO,
    MutableMapping,
    Optional,
    Text,
    Tuple,
    Union,
    List,
    cast,
    Callable,
)
import abc
import sqlite3
import email.utils
import hashlib
import time
import math

import requests
import scrapelib

from scrapelib._types import _Data, RequestsCookieJar, _HooksInput, _AuthType


class Scraper(scrapelib.Scraper):
    def __init__(self, *args, scheduler: "Scheduler" = None, **kwargs):

        if scheduler is not None:
            self.scheduler = scheduler
        else:
            raise ValueError("You must supply a scheduler")

        super().__init__(*args, **kwargs)

    def request(  # type: ignore
        self,
        method: str,
        url: Union[str, bytes, Text],
        params: Union[None, bytes, MutableMapping[Text, Text]] = None,
        data: _Data = None,
        headers: Optional[MutableMapping[Text, Text]] = None,
        cookies: Union[None, RequestsCookieJar, MutableMapping[Text, Text]] = None,
        files: Optional[MutableMapping[Text, IO[Any]]] = None,
        auth: _AuthType = None,
        timeout: Union[None, float, Tuple[float, float], Tuple[float, None]] = None,
        allow_redirects: Optional[bool] = True,
        proxies: Optional[MutableMapping[Text, Text]] = None,
        hooks: Optional[_HooksInput] = None,
        stream: Optional[bool] = None,
        verify: Union[None, bool, Text] = True,
        cert: Union[Text, Tuple[Text, Text], None] = None,
        json: Optional[Any] = None,
        retry_on_404: bool = False,
    ) -> scrapelib.CacheResponse:

        method = method.lower()
        request_key = self.key_for_request(method, url)

        should_request = self.scheduler.query(request_key)

        if should_request:

            response = super().request(
                method,
                url,
                data=data,
                params=params,
                headers=headers,
                cookies=cookies,
                files=files,
                auth=auth,
                timeout=timeout,
                allow_redirects=allow_redirects,
                proxies=proxies,
                hooks=hooks,
                stream=stream,
                verify=verify,
                cert=cert,
                json=json,
                retry_on_404=retry_on_404,
            )

            if not response.fromcache:
                self.scheduler.update(request_key, response)

        else:

            response = scrapelib.CacheResponse()
            response.status_code = 418
            response.url = cast(str, url)
            response.headers = requests.structures.CaseInsensitiveDict()
            response._content = "The scheduler said we should skip".encode()
            response.raw = object()
            response.fromcache = False

        return response


class Scheduler(abc.ABC):
    storage: "Storage"
    hasher: Callable

    @abc.abstractmethod
    def query(self, key) -> bool:
        ...

    def update(self, key, response: requests.Response) -> None:

        last_checked: Union[float, int]
        last_changed: Union[float, int]
        header_date = response.headers.get("date")
        if header_date:
            time_struct = email.utils.parsedate_tz(header_date)
            if time_struct:
                last_checked = email.utils.mktime_tz(time_struct)
            else:
                last_checked = time.time()
        else:
            last_checked = time.time()

        header_last_modified = response.headers.get("last-modified")
        if header_last_modified:
            last_changed = email.utils.mktime_tz(email.utils.parsedate_tz(header_last_modified))  # type: ignore
        else:
            last_changed = last_checked

        if self.hasher:
            content_hash = self.hasher(response)
            if not content_hash:
                return None
        else:
            h = hashlib.sha256()
            h.update(response.content)
            content_hash = h.hexdigest()

        self.storage.set(key, content_hash, last_checked, last_changed)


class DummyScheduler(Scheduler):
    def query(self, key) -> bool:

        return True

    def update(self, key, response: requests.Response) -> None:
        ...


class PoissonScheduler(Scheduler):
    def __init__(
        self,
        storage,
        threshold: float = 0.3,
        prior_weight: float = 3,
        hasher=None,
    ):

        self.storage = storage
        self.threshold = threshold
        self.hasher = hasher  # type: ignore

        intervals = self.storage.intervals()
        if intervals:
            try:
                rate = len(intervals) / sum(intervals)  # type: ignore
            except ZeroDivisionError:
                rate = 1
        else:
            rate = 1

        self.alpha = prior_weight
        self.beta = prior_weight * (1 / rate)

    def query(self, key) -> bool:

        result = self.storage.get(key)
        if result is None:
            return True

        prob_change = self._prob(*result)
        print(prob_change)

        return prob_change > self.threshold

    def _prob(self, time_since_last_check, time_unchanged):

        prob_no_change = math.exp(
            -((self.alpha + 1) / (self.beta + time_unchanged)) * time_since_last_check
        )
        return 1 - prob_no_change


class Storage(abc.ABC):
    @abc.abstractmethod
    def get(self, key: str) -> Optional[Tuple[float, float]]:
        ...

    @abc.abstractmethod
    def set(
        self, key: str, content_hash: str, last_checked: float, last_modified: float
    ) -> None:
        ...

    @abc.abstractmethod
    def intervals(self) -> List[float]:
        ...


class SqliteStorage(Storage):
    def __init__(self, path):

        self._conn = sqlite3.connect(path, isolation_level=None)
        self._build_table()

    def _build_table(self) -> None:

        self._conn.execute(
            """CREATE TABLE IF NOT EXISTS history
               (key text PRIMARY KEY,
                hash text,
                last_checked datetime,
                last_changed datetime)"""
        )

    def get(self, key: str) -> Optional[Tuple[float, float]]:

        query = self._conn.execute(
            "SELECT ? - last_checked, last_checked - last_changed FROM history where key=?",
            (
                time.time(),
                key,
            ),
        )
        return query.fetchone()

    def set(
        self, key: str, content_hash: str, last_checked: float, last_changed: float
    ) -> None:

        query = self._conn.execute("SELECT hash FROM history WHERE key = ?", (key,))
        row = query.fetchone()
        stored_hash = row[0] if row else None

        if stored_hash == content_hash:
            self._conn.execute(
                "UPDATE history SET last_checked = ? WHERE key = ?", (last_checked, key)
            )
        else:

            if stored_hash:
                self._conn.execute(
                    """UPDATE history SET hash = ?,
                                          last_checked = ?,
                                          last_changed = ?
                       WHERE key = ?""",
                    (content_hash, last_checked, last_changed, key),
                )
            else:
                self._conn.execute(
                    """INSERT INTO history VALUES (?, ?, ?, ?)""",
                    (key, content_hash, last_checked, last_changed),
                )

    def intervals(self) -> List[float]:

        query = "SELECT last_checked - last_changed FROM history"

        result = self._conn.execute(query)

        return [span for span, in result]
