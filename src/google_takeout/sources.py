import json
from collections.abc import Iterable, Sequence

import dlt
from dlt.sources import DltResource

from pypeline_functions.google_takeout.models import Activity, ChromeHistory, PlaceVisit
from pypeline_functions.google_takeout.parsers import GoogleTakeoutParser
from pypeline_functions.utils.storage import GoogleCloudStorage, LocalStorage


@dlt.source
def google_takeout_seed_gcs(bucket_name: str) -> Sequence[DltResource]:
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
    google_takeout_parser = GoogleTakeoutParser()

    @dlt.resource(
        name="chrome_history", write_disposition="merge", primary_key=("time_usec", "title"), columns=ChromeHistory
    )
    def chrome_history() -> Iterable[ChromeHistory]:
        """Extract the latest chrome history data."""
        latest_seeds = gcs.get_latest_seeds(bucket_name, DATA_PATH, "Chrome/History.json", DATETIME_FORMAT)
        for seed in latest_seeds:
            content = seed.download_as_string().decode("utf-8", "replace")
            data = json.loads(content)
            for datum in data.get("Browser History", []):
                yield google_takeout_parser.chrome_history_parser(datum)

    @dlt.resource(name="activity", write_disposition="merge", primary_key=("header", "title", "time"), columns=Activity)
    def activity() -> Iterable[Activity]:
        """Extract the latest activity data."""
        latest_seeds = gcs.get_latest_seeds(bucket_name, DATA_PATH, "MyActivity.json", DATETIME_FORMAT)
        for seed in latest_seeds:
            content = seed.download_as_string().decode("utf-8", "replace")
            data = json.loads(content)
            for datum in data:
                yield google_takeout_parser.activity_parser(datum)

    @dlt.resource(
        name="location", write_disposition="merge", primary_key=("lat", "lng", "start_time"), columns=PlaceVisit
    )
    def location() -> Iterable[PlaceVisit]:
        """Extract the latest location data."""
        latest_seeds = gcs.get_latest_seeds(
            bucket_name, DATA_PATH, "Location History (Timeline)/Records.json", DATETIME_FORMAT
        )
        for seed in latest_seeds:
            content = seed.download_as_string().decode("utf-8", "replace")
            data = json.loads(content)
            if "placeVisit" in data:
                for datum in data:
                    yield google_takeout_parser.location_parser(datum)

    return chrome_history, activity, location


def google_takeout_seed_local(seed_path: str) -> Sequence[DltResource]:
    """
    Extract data from the Google Takeout seed located locally.

    Parameters
    ----------
    seed_path : str
        The file path to the extracted Google takeout seed.
    """
    local_storage = LocalStorage()
    google_takeout_parser = GoogleTakeoutParser()

    @dlt.resource(
        name="chrome_history", write_disposition="merge", primary_key=("time_usec", "title"), columns=ChromeHistory
    )
    def chrome_history() -> Iterable[ChromeHistory]:
        """Extract the latest chrome history data."""
        latest_seeds = local_storage.get_latest_seed(seed_path, "takeout-*", "Chrome/History.json")
        for seed in latest_seeds:
            with open(seed, "r", encoding="utf-8") as file:  # noqa: UP015
                data = json.load(file)
                for datum in data.get("Browser History", []):
                    yield google_takeout_parser.chrome_history_parser(datum)

    @dlt.resource(name="activity", write_disposition="merge", primary_key=("header", "title", "time"), columns=Activity)
    def activity() -> Iterable[Activity]:
        """Extract the latest activity data."""
        latest_seeds = local_storage.get_latest_seed(seed_path, "takeout-*", "MyActivity.json")
        for seed in latest_seeds:
            with open(seed, "r", encoding="utf-8") as file:  # noqa: UP015
                data = json.load(file)
                for datum in data:
                    yield google_takeout_parser.activity_parser(datum)

    @dlt.resource(
        name="location", write_disposition="merge", primary_key=("lat", "lng", "start_time"), columns=PlaceVisit
    )
    def location() -> Iterable[PlaceVisit]:
        """Extract the latest location data."""
        latest_seeds = local_storage.get_latest_seed(seed_path, "takeout-*", "Semantic Location History\\/.*\\.json")
        for seed in latest_seeds:
            with open(seed, "r", encoding="utf-8") as file:  # noqa: UP015
                data = json.load(file)
                for datum in data.get("timelineObjects", []):
                    if "placeVisit" in datum:
                        yield google_takeout_parser.location_parser(datum)

    return chrome_history, activity, location
