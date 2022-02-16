import scheduledscraper


def test_dummy_scheduler_query():

    scheduler = scheduledscraper.DummyScheduler()

    assert scheduler.query("foo")

    assert scheduler.update("foo", "bar") is None

    assert scheduler.query("foo")
