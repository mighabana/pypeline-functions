import json
from collections.abc import Sequence
from datetime import datetime

import dlt
from dlt.sources import DltResource
from google.cloud.storage import Blob
from models.google_takeout import ChromeHistory, Activity, PlaceVisit
from parsers.json_parser import GoogleTakeout
from utils.google_cloud_storage import GoogleCloudStorage

@dlt.source
def google_takeout_seed(bucket_name:str) -> Sequence[DltResource]:
    """
    """

    DATA_PATH = "google/takeout/"
    gcs = GoogleCloudStorage()
    gt = GoogleTakeout()

    def _get_latest_seeds(prefix:str, file_name:str) -> Blob:
        datetime_format = "%Y%m%dT%H%M%SZ"
        subfolders = gcs.list_subfolders(bucket_name, prefix)
        timestamps = []

        for folder in subfolders:
            t = folder.removeprefix(prefix).removesuffix("/")
            timestamp = datetime.strptime(t, datetime_format) #noqa: DTZ007
            timestamps.append(timestamp)

        latest_timestamp = max(timestamps)
        latest_seed = f"{prefix}{latest_timestamp.strftime(datetime_format)}/"

        blobs = gcs.list_blobs_with_prefix(bucket_name, latest_seed)
        output = []
        for blob in blobs:
            if file_name in blob.name:
                output.append(blob)
        return output

    @dlt.resource(name="chrome_history", write_disposition="merge", primary_key=("time_usec", "title"), columns=ChromeHistory)
    def chrome_history():
        latest_seeds = _get_latest_seeds(DATA_PATH, "Chrome/History.json")
        for seed in latest_seeds:
            content = seed.download_as_string().decode("utf-8", "replace")
            data = json.loads(content)
            data = [gt.chrome_history_parser(datum) for datum in data.get("BrowserHistory", [])]
            yield data

    @dlt.resource(name="activity", write_disposition="merge", primary_key=("header", "title", "time"), columns=Activity)
    def activity():
        latest_seeds = _get_latest_seeds(DATA_PATH, "MyActivity.json")
        for seed in latest_seeds:
            content = seed.download_as_string().decode("utf-8", "replace")
            data = json.loads(content)
            data = [gt.activity_parser(datum) for datum in data]
            yield data

    @dlt.resource(name="location", write_disposition="merge", primary_key=("lat", "lng", "start_time"), columns=PlaceVisit)
    def location():
        latest_seeds = _get_latest_seeds(DATA_PATH, "Location History (Timeline)/Records.json")
        for seed in latest_seeds:
            content = seed.download_as_string().decode("utf-8", "replace")
            data = json.loads(content)
            data = [gt.location_parser(datum) for datum in data if "placeVisit" in data]
            yield data

    return chrome_history, activity, location
