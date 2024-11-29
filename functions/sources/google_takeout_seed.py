import json
from collections.abc import Iterable, Sequence

import dlt
from dlt.sources import DltResource
from models.google_takeout import Activity, ChromeHistory, PlaceVisit
from parsers.json_parser import GoogleTakeout
from utils.google_cloud_storage import GoogleCloudStorage


@dlt.source
def google_takeout_seed(bucket_name:str) -> Sequence[DltResource]:
    """
    Extract data from the Google Takeout seed located in Google Cloud Storage.

    Parameters
    ----------
    bucket_name : str
        The name of the bucket that the seed is located in.
    """
    DATA_PATH = "google/takeout/"  # noqa: N806
    DATETIME_FORMAT = "%Y%m%dT%H%M%SZ"  # noqa: N806
    gcs = GoogleCloudStorage()
    gt = GoogleTakeout()

    @dlt.resource(
        name="chrome_history",
        write_disposition="merge",
        primary_key=("time_usec", "title"),
        columns=ChromeHistory
    )
    def chrome_history() -> Iterable[ChromeHistory]:
        """Extract the latest chrome history data."""
        latest_seeds = gcs.get_latest_seeds(bucket_name, DATA_PATH, "Chrome/History.json", DATETIME_FORMAT)
        for seed in latest_seeds:
            content = seed.download_as_string().decode("utf-8", "replace")
            data = json.loads(content)
            data = [gt.chrome_history_parser(datum) for datum in data.get("BrowserHistory", [])]
            yield data

    @dlt.resource(
        name="activity",
        write_disposition="merge",
        primary_key=("header", "title", "time"),
        columns=Activity
    )
    def activity() -> Iterable[Activity]:
        """Extract the latest activity data."""
        latest_seeds = gcs.get_latest_seeds(bucket_name, DATA_PATH, "MyActivity.json", DATETIME_FORMAT)
        for seed in latest_seeds:
            content = seed.download_as_string().decode("utf-8", "replace")
            data = json.loads(content)
            data = [gt.activity_parser(datum) for datum in data]
            yield data

    @dlt.resource(
        name="location",
        write_disposition="merge",
        primary_key=("lat", "lng", "start_time"),
        columns=PlaceVisit
    )
    def location() -> Iterable[PlaceVisit]:
        """Extract the latest location data."""
        latest_seeds = gcs.get_latest_seeds(
            bucket_name,
            DATA_PATH,
            "Location History (Timeline)/Records.json",
            DATETIME_FORMAT
        )
        for seed in latest_seeds:
            content = seed.download_as_string().decode("utf-8", "replace")
            data = json.loads(content)
            data = [gt.location_parser(datum) for datum in data if "placeVisit" in data]
            yield data

    return chrome_history, activity, location
