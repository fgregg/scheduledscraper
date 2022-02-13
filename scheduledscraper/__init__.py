from typing import (
    Any,
    IO,
    MutableMapping,
    Optional,
    Text,
    Tuple,
    Union,
    cast,
)
import abc
import sqlite3
import email.utils
import hashlib
import time

import requests
import scrapelib

from scrapelib._types import (
    _Data,
    RequestsCookieJar,
    _HooksInput,
    _AuthType,
)


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
    storage: Storage 
    
    @abc.abstractmethod
    def query(self, key) -> bool:
        ...

    def update(self, key, response: requests.Response) -> None:

        header_date = response.headers.get("date")
        if header_date:
            last_checked = time.mktime(email.utils.parsedate(header_date))  # type: ignore
        else:
            last_checked = time.time()

        header_last_modified = response.headers.get("last-modified")
        if header_last_modified:
            last_changed = time.mktime(email.utils.parsedate(header_last_modified))  # type: ignore
        else:
            last_changed = last_checked

        h = hashlib.sha256()
        h.update(response.content)
        content_hash = h.hexdigest()

        self.storage.set(key, content_hash, last_checked, last_changed)


class DummyScheduler(Scheduler):
    def query(self, key) -> bool:

        return True

    def update(self, key, response: requests.Response) -> None:
        ...


class Storage(abc.ABC):
    @abc.abstractmethod
    def get(self, key: str) -> Optional[float]:
        ...

    @abc.abstractmethod
    def set(
        self, key: str, content_hash: str, last_checked: float, last_modified: float
    ) -> None:
        ...


class SqliteStorage(Storage):
    def __init__(self, path):

        self._conn = sqlite3.connect(path)
        self._build_table()

    def _build_table(self) -> None:

        self._conn.execute(
            """CREATE TABLE IF NOT EXISTS history
               (key text PRIMARY KEY,
                hash text,
                last_checked datetime,
                last_changed datetime)"""
        )

    def get(self, key: str) -> Optional[float]:

        query = self._conn.execute(
            "SELECT last_checked - last_changed FROM history where key=?", (key,)
        )
        (seconds_since_last_change,) = query.fetchone()
        return seconds_since_last_change

    def set(
        self, key: str, content_hash: str, last_checked: float, last_changed: float
    ) -> None:

        query = self._conn.execute("SELECT hash FROM history where key=?", (key,))
        row = query.fetchone()
        stored_hash = row[0] if row else None

        if stored_hash == content_hash:
            self._conn.execute("UPDATE history SET last_checked = ?", (last_checked,))
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
