import requests
import time

import scheduledscraper


def test_setting():

    storage = scheduledscraper.SqliteStorage(":memory:")

    response = requests.get("https://httpbin.org/status/200")

    storage.set("foo", response)

    seconds_since_last_change = storage.get("foo")

    assert seconds_since_last_change == 0

    time.sleep(1)

    response = requests.get("https://httpbin.org/status/200")

    storage.set("foo", response)

    seconds_since_last_change = storage.get("foo")

    assert seconds_since_last_change > 0

    response = requests.get("http://httpbin.org/get?key1=value1")

    storage.set("foo", response)

    seconds_since_last_change = storage.get("foo")

    assert seconds_since_last_change == 0

    response = requests.get(
        "https://httpbin.org/response-headers?last-modified=Wed,%2021%20Oct%202015%2007:28:00%20GMT"
    )

    storage.set("bar", response)

    first_seconds_since_last_change = storage.get("bar")

    assert first_seconds_since_last_change > 0

    time.sleep(1)

    response = requests.get(
        "https://httpbin.org/response-headers?last-modified=Wed,%2021%20Oct%202015%2007:28:00%20GMT&cool=times"
    )

    storage.set("bar", response)

    second_seconds_since_last_change = storage.get("bar")

    assert second_seconds_since_last_change > first_seconds_since_last_change
