import scheduledscraper


def test_scraper_always(mocker):

    always_scheduler = mocker.MagicMock()
    always_scheduler.query = mocker.Mock(return_value=True)

    scraper = scheduledscraper.Scraper(scheduler=always_scheduler)

    response = scraper.get("https://httpbin.org/status/200")

    assert response.status_code == 200
    always_scheduler.update.assert_called_once()


def test_scraper_never(mocker):

    never_scheduler = mocker.MagicMock()
    never_scheduler.query = mocker.Mock(return_value=False)

    scraper = scheduledscraper.Scraper(scheduler=never_scheduler)

    response = scraper.get("https://httpbin.org/status/200")

    assert response.status_code == 418
    assert response.text == "The scheduler said we should skip"
    never_scheduler.update.assert_not_called()
