import scheduledscraper


def test_scraper_always(mocker):

    always_scheduler = mocker.Mock(return_value=True)

    scraper = scheduledscraper.Scraper(scheduler=always_scheduler)

    response = scraper.get('https://httpbin.org/status/200')

    assert response.status_code == 200
