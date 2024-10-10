import datetime
import json
from _strptime import TimeRE
from collections.abc import Sequence

import dlt
from dlt.sources import DltResource
from google.cloud.storage import Blob
from pydantic import BaseModel, field_validator
from utils.google_cloud_storage import GoogleCloudStorage

@dlt.source
def google_takeout_seed(bucket_name:str) -> Sequence[DltResource]:
    """
    """

    DATA_PATH = "google/takeout/"
    gcs = GoogleCloudStorage()

    def default_str(string:str):
        if string is not None:
            return string
        else:
            return ""