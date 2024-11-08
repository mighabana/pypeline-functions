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
def spotify_seed(bucket_name:str) -> Sequence[DltResource]:
    """
    """

    ACCOUNT_DATA_PATH = "spotify/account_data/"  # noqa: N806
    STREAMING_HISTORY_PATH = "spotify/streaming_history" #noqa: N806
    gcs = GoogleCloudStorage()

    def default_str(string:str):
        if string is not None:
            return string
        else:
            return ""
    class FollowData(BaseModel):
        followerCount: int
        followingUsersCount: int
        dismissingUsersCount: int

    class Identifier(BaseModel):
        identifierType: str
        identifierValue: str

        _default_str = field_validator(
            "identifierType",
            "identifierValue",
            mode="before"
        )(default_str)

    class Marquee(BaseModel):
        artistName: str
        segment: str

        _default_str = field_validator(
            "artistName",
            "segment",
            mode="before"
        )(default_str)

    class SearchQueries(BaseModel):
        platform: str
        searchTime: datetime.datetime
        searchQuery: str
        searchInteractionURIs: list

        _default_str = field_validator(
            "platform",
            "searchQuery",
            mode="before"
        )(default_str)

    class UserData(BaseModel):
        username: str | None
        email: str
        country: str
        createdFromFacebook: bool
        facebookUid: str | None
        birthdate: datetime.datetime
        gender: str
        postalCode: str | None
        mobileNumber: str | None
        mobileOperator: str | None
        mobileBrand: str | None
        creationTime: datetime.datetime

        _default_str = field_validator(
            "email",
            "country",
            "gender",
            mode="before"
        )(default_str)

    class Track(BaseModel):
        artist: str
        album: str
        track: str
        uri: str

        _default_str = field_validator(
            "artist",
            "album",
            "track",
            "uri",
            mode="before"
        )(default_str)
    class Album(BaseModel):
        artist: str
        album: str
        uri: str

        _default_str = field_validator(
            "artist",
            "album",
            "uri",
            mode="before"
        )(default_str)

    class Artist(BaseModel):
        name: str
        uri: str

        _default_str = field_validator(
                "name",
                "uri",
                mode="before"
            )(default_str)

    class Library(BaseModel):
        tracks : list[Track]
        albums: list[Album]
        shows: list # unknown type my data is blank
        episodes: list # unknown type my data is blank
        bannedTracks: list[Track] #unknown type my data is blank
        artists: list[Artist]
        bannedArtists: list[Artist]
        other: list[str]

    class StreamingHistory(BaseModel):
        ts: datetime.datetime
        username: str
        platform: str
        ms_played: int
        conn_country: str
        ip_addr_decrypted: str
        user_agent_decrypted: str
        master_metadata_track_name: str | None
        master_metadata_album_artist_name: str | None
        master_metadata_album_album_name: str | None
        spotify_track_uri: str | None
        episode_name: str | None
        episode_show_name: str | None
        spotify_episode_uri: str | None
        reason_start: str
        reason_end: str
        shuffle: bool
        skipped: int | None
        offline: bool
        offline_timestamp: int
        incognito_mode: bool

        _default_str = field_validator(
            "username",
            "platform",
            "conn_country",
            "ip_addr_decrypted",
            "user_agent_decrypted",
            "reason_start",
            "reason_end",
            mode="before"
        )(default_str)

    def _get_latest_seed(prefix:str, file_name:str) -> Blob:
        datetime_format = "%Y%m%dT%H%M%S"
        subfolders = gcs.list_subfolders(bucket_name, prefix)
        timestamps = []

        for folder in subfolders:
            t = folder.removeprefix(prefix).removesuffix("/")
            timestamp = datetime.datetime.strptime(t, datetime_format) #noqa: DTZ007
            timestamps.append(timestamp)

        latest_timestamp = max(timestamps)
        latest_seed = f"{prefix}{latest_timestamp.strftime(datetime_format)}/"

        blobs = gcs.list_blobs_with_prefix(bucket_name, latest_seed)
        for blob in blobs:
            if file_name in blob.name:
                return blob


    def _datetime_parser(dct:dict):
        # TODO: make dynamic datetime parsers with a Factory Method so we can generalize datetime format conversion.
        datetime_format="%Y-%m-%dT%H:%M:%S"
        for k, v in dct.items():
            if isinstance(v, str) and TimeRE().compile(datetime_format).search(v[:19]):
                try:
                    dct[k] = datetime.datetime.strptime(v[:19], datetime_format) # noqa: DTZ007
                except:
                    pass
        return dct

    def _datetime_parser_base(dct:dict):
        # TODO: make dynamic datetime parsers with a Factory Method so we can generalize datetime format conversion.
        datetime_format="%Y-%m-%dT%H:%M:%SZ"
        for k, v in dct.items():
            if isinstance(v, str) and TimeRE().compile(datetime_format).search(v):
                try:
                    dct[k] = datetime.datetime.strptime(v, datetime_format) # noqa: DTZ007
                except:
                    pass
        return dct

    def _date_parser(dct:dict):
        # TODO: make dynamic datetime parsers with a Factory Method so we can generalize datetime format conversion.
        datetime_format="%Y-%m-%d"
        for k, v in dct.items():
            if isinstance(v, str) and TimeRE().compile(datetime_format).search(v[:10]):
                try:
                    dct[k] = datetime.datetime.strptime(v[:10], datetime_format) # noqa: DTZ007
                except:
                    pass
        return dct

    @dlt.resource(name="follow_data", write_disposition="replace", columns=FollowData)
    def follow_data():
        latest_seed = _get_latest_seed(ACCOUNT_DATA_PATH, "Follow.json")
        content = latest_seed.download_as_string().decode("utf-8", "replace")
        data = json.loads(content)
        yield data

    @dlt.resource(name="identifier", write_disposition="replace", columns=Identifier)
    def identifier():
        latest_seed = _get_latest_seed(ACCOUNT_DATA_PATH, "Identifiers.json")
        content = latest_seed.download_as_string().decode("utf-8", "replace")
        data = json.loads(content)
        yield data

    @dlt.resource(name="marquee", write_disposition="replace", columns=Marquee)
    def marquee():
        latest_seed = _get_latest_seed(ACCOUNT_DATA_PATH, "Marquee.json")
        content = latest_seed.download_as_string().decode("utf-8", "replace")
        data = json.loads(content)
        yield data

    # TODO: fix whatever is going on here.. something with the the data types of the searchQuery data types not matching
    # @dlt.resource(name="search_query", write_disposition="merge", primary_key="searchTime", columns=SearchQueries)
    # def search_query():
    #     latest_seed = _get_latest_seed(ACCOUNT_DATA_PATH, "SearchQueries.json")
    #     content = latest_seed.download_as_string().decode("utf-8", "replace")
    #     data = json.loads(content, object_hook=_datetime_parser)
    #     yield data

    @dlt.resource(name="user_data", write_disposition="replace", columns=UserData)
    def user_data():
        latest_seed = _get_latest_seed(ACCOUNT_DATA_PATH, "Userdata.json")
        content = latest_seed.download_as_string().decode("utf-8", "replace")
        data = json.loads(content, object_hook=_date_parser)
        yield data

    @dlt.resource(name="library", write_disposition="replace", columns=Library)
    def library():
        latest_seed = _get_latest_seed(ACCOUNT_DATA_PATH, "YourLibrary.json")
        content = latest_seed.download_as_string().decode("utf-8", "replace")
        data = json.loads(content)
        yield data

    @dlt.resource(name="audio_streaming_history", write_disposition="merge", primary_key="ts", columns=StreamingHistory)
    def audio_streaming_history():
        blobs = gcs.list_blobs_with_prefix(bucket_name=bucket_name, prefix=STREAMING_HISTORY_PATH)
        streaming_history_files = [blob for blob in blobs if blob.name.endswith(".json") and "Audio" in blob.name]
        for f in streaming_history_files:
            content = f.download_as_string().decode("utf-8", "replace")
            data = json.loads(content, object_hook=_datetime_parser_base)
            yield data


    # return follow_data, identifier, marquee, user_data, library, search_query, audio_streaming_history
    return follow_data, identifier, marquee, user_data, library, audio_streaming_history
