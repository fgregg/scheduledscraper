from typing import (
    Any,
    Callable,
    Container,
    IO,
    Mapping,
    MutableMapping,
    Optional,
    Text,
    Tuple,
    Union,
    cast,
)
import abc

import requests
import scrapelib

from scrapelib._types import (
    _Data,
    PreparedRequest,
    RequestsCookieJar,
    _HooksInput,
    _AuthType,
    Response,
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
    @abc.abstractmethod
    def query(self, key) -> bool:
        ...

    @abc.abstractmethod
    def update(self, key, response: requests.Response) -> None:
        ...


class DummyScheduler(Scheduler):
    def query(self, key) -> bool:

        return True

    def update(self, key, response: requests.Response) -> None:
        ...


class Storage(abc.ABC):
    @abc.abstractmethod
    def get(self, key) -> float:
        ...

    @abc.abstractmethod
    def set(self, key, response) -> None:
        ...
