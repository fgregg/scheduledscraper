import requests
import time

import scheduledscraper


def test_setting():

    storage = scheduledscraper.SqliteStorage(":memory:")

    storage.set("foo", "1", 1, 1)

    seconds_since_last_change = storage.get("foo")

    assert seconds_since_last_change == 0

    storage.set("foo", "1", 2, 2)

    seconds_since_last_change = storage.get("foo")

    assert seconds_since_last_change == 1

    storage.set("foo", "2", 3, 3)

    seconds_since_last_change = storage.get("foo")

    assert seconds_since_last_change == 0

    storage.set("bar", "3", 2, 1)

    seconds_since_last_change = storage.get("bar")

    assert seconds_since_last_change == 1
