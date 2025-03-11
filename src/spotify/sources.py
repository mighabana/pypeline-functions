import json
from collections.abc import Iterable, Sequence

import dlt
from dlt.sources import DltResource

from pypeline_functions.spotify.models import (
    FollowData,
    Identifier,
    Library,
    Marquee,
    SearchQueries,
    StreamingHistory,
    UserData,
)
from pypeline_functions.spotify.parsers import SpotifyParser
from pypeline_functions.utils.storage import GoogleCloudStorage, LocalStorage


@dlt.source
def spotify_seed_gcs(bucket_name: str) -> Sequence[DltResource]:
    """
    Extract data from the Spotify seed located in Google Cloud Storage.

    Parameters
    ----------
    bucket_name : str
        The name of the bucket that the seed is located in.
    """
    ACCOUNT_DATA_PATH = "spotify/account_data/"  # noqa: N806
    STREAMING_HISTORY_PATH = "spotify/streaming_history"  # noqa: N806
    DATETIME_FORMAT = "%Y%m%dT%H%M%S"  # noqa: N806
    gcs = GoogleCloudStorage()
    spotify_parser = SpotifyParser()

    @dlt.resource(name="follow_data", write_disposition="replace", columns=FollowData)
    def follow_data() -> Iterable[FollowData]:
        """Extract the latest follow data."""
        latest_seeds = gcs.get_latest_seeds(bucket_name, ACCOUNT_DATA_PATH, "Follow.json", DATETIME_FORMAT)
        for seed in latest_seeds:
            content = seed.download_as_string().decode("utf-8", "replace")
            data = json.loads(content)
            yield spotify_parser.follow_data_parser(data)

    @dlt.resource(name="identifier", write_disposition="replace", columns=Identifier)
    def identifier() -> Iterable[Identifier]:
        """Extract the latest identifier data."""
        latest_seeds = gcs.get_latest_seeds(bucket_name, ACCOUNT_DATA_PATH, "Identifiers.json", DATETIME_FORMAT)
        for seed in latest_seeds:
            content = seed.download_as_string().decode("utf-8", "replace")
            data = json.loads(content)
            yield spotify_parser.identifier_parser(data)

    @dlt.resource(name="marquee", write_disposition="replace", columns=Marquee)
    def marquee() -> Iterable[Marquee]:
        """Extract the latest marquee data."""
        latest_seeds = gcs.get_latest_seeds(bucket_name, ACCOUNT_DATA_PATH, "Marquee.json", DATETIME_FORMAT)
        for seed in latest_seeds:
            content = seed.download_as_string().decode("utf-8", "replace")
            data = json.loads(content)
            for datum in data:
                yield spotify_parser.marquee_parser(datum)

    @dlt.resource(
        name="search_queries",
        write_disposition="merge",
        primary_key=("search_query", "search_time"),
        columns=SearchQueries,
    )
    def search_query() -> Iterable[SearchQueries]:
        """Extract the latest search query data."""
        latest_seeds = gcs.get_latest_seeds(bucket_name, ACCOUNT_DATA_PATH, "SearchQueries.json", DATETIME_FORMAT)
        for seed in latest_seeds:
            content = seed.download_as_string().decode("utf-8", "replace")
            data = json.loads(content)
            for datum in data:
                yield spotify_parser.search_query_parser(datum)

    @dlt.resource(name="user_data", write_disposition="replace", columns=UserData)
    def user_data() -> Iterable[UserData]:
        """Extract the latest user data."""
        latest_seeds = gcs.get_latest_seeds(bucket_name, ACCOUNT_DATA_PATH, "Userdata.json", DATETIME_FORMAT)
        for seed in latest_seeds:
            content = seed.download_as_string().decode("utf-8", "replace")
            data = json.loads(content)
            yield spotify_parser.user_data_parser(data)

    @dlt.resource(name="library", write_disposition="replace", columns=Library)
    def library() -> Iterable[Library]:
        """Extract the latest library data."""
        latest_seeds = gcs.get_latest_seeds(bucket_name, ACCOUNT_DATA_PATH, "YourLibrary.json", DATETIME_FORMAT)
        for seed in latest_seeds:
            content = seed.download_as_string().decode("utf-8", "replace")
            data = json.loads(content)
            yield spotify_parser.library_parser(data)

    @dlt.resource(name="audio_streaming_history", write_disposition="merge", primary_key="ts", columns=StreamingHistory)
    def audio_streaming_history() -> Iterable[StreamingHistory]:
        """Extract the latest audio streaming history data."""
        blobs = gcs.list_blobs_with_prefix(bucket_name=bucket_name, prefix=STREAMING_HISTORY_PATH)
        streaming_history_files = [blob for blob in blobs if blob.name.endswith(".json") and "Audio" in blob.name]
        for f in streaming_history_files:
            content = f.download_as_string().decode("utf-8", "replace")
            data = json.loads(content)
            for datum in data:
                yield spotify_parser.streaming_history_parser(datum)

    return follow_data, identifier, marquee, user_data, library, search_query, audio_streaming_history


@dlt.source
def spotify_seed_local(seed_path: str) -> Sequence[DltResource]:
    """
    Extract data from the Spotify seed located locally.

    Parameters
    ----------
    seed_path : str
        The file path to the extracted Spotify seed.
    """
    local_storage = LocalStorage()
    spotify_parser = SpotifyParser()
    DATETIME_FORMAT = "%Y%m%dT%H%M%S"  # noqa: N806

    @dlt.resource(name="follow_data", write_disposition="replace", columns=FollowData)
    def follow_data() -> Iterable[FollowData]:
        """Extract the latest follow data."""
        latest_seeds = local_storage.get_latest_seed(
            seed_path, "spotify-account_data-*", "Follow.json", datetime_format=DATETIME_FORMAT
        )
        for seed in latest_seeds:
            with open(seed, "r", encoding="utf-8") as file:  # noqa: UP015
                data = json.load(file)
                yield spotify_parser.follow_data_parser(data)

    @dlt.resource(name="identifier", write_disposition="replace", columns=Identifier)
    def identifier() -> Iterable[Identifier]:
        """Extract the latest identifier data."""
        latest_seeds = local_storage.get_latest_seed(
            seed_path, "spotify-account_data-*", "Identifiers.json", datetime_format=DATETIME_FORMAT
        )
        for seed in latest_seeds:
            with open(seed, "r", encoding="utf-8") as file:  # noqa: UP015
                data = json.load(file)
                yield spotify_parser.identifier_parser(data)

    @dlt.resource(name="marquee", write_disposition="replace", columns=Marquee)
    def marquee() -> Iterable[Marquee]:
        """Extract the latest marquee data."""
        latest_seeds = local_storage.get_latest_seed(
            seed_path, "spotify-account_data-*", "Marquee.json", datetime_format=DATETIME_FORMAT
        )
        for seed in latest_seeds:
            with open(seed, "r", encoding="utf-8") as file:  # noqa: UP015
                data = json.load(file)
                for datum in data:
                    yield spotify_parser.marquee_parser(datum)

    @dlt.resource(
        name="search_queries",
        write_disposition="merge",
        primary_key=("search_query", "search_time"),
        columns=SearchQueries,
    )
    def search_query() -> Iterable[SearchQueries]:
        """Extract the latest search query data."""
        latest_seeds = local_storage.get_latest_seed(
            seed_path, "spotify-account_data-*", "SearchQueries.json", datetime_format=DATETIME_FORMAT
        )
        for seed in latest_seeds:
            with open(seed, "r", encoding="utf-8") as file:  # noqa: UP015
                data = json.load(file)
                for datum in data:
                    yield spotify_parser.search_query_parser(datum)

    @dlt.resource(name="user_data", write_disposition="replace", columns=UserData)
    def user_data() -> Iterable[UserData]:
        """Extract the latest user data."""
        latest_seeds = local_storage.get_latest_seed(
            seed_path, "spotify-account_data-*", "Userdata.json", datetime_format=DATETIME_FORMAT
        )
        for seed in latest_seeds:
            with open(seed, "r", encoding="utf-8") as file:  # noqa: UP015
                data = json.load(file)
                yield spotify_parser.user_data_parser(data)

    @dlt.resource(name="library", write_disposition="replace", columns=Library)
    def library() -> Iterable[Identifier]:
        """Extract the latest library data."""
        latest_seeds = local_storage.get_latest_seed(
            seed_path, "spotify-account_data-*", "YourLibrary.json", datetime_format=DATETIME_FORMAT
        )
        for seed in latest_seeds:
            with open(seed, "r", encoding="utf-8") as file:  # noqa: UP015
                data = json.load(file)
                yield spotify_parser.library_parser(data)

    @dlt.resource(name="audio_streaming_history", write_disposition="merge", primary_key="ts", columns=StreamingHistory)
    def audio_streaming_history() -> Iterable[StreamingHistory]:
        """Extract the latest audio streaming history data."""
        latest_seeds = local_storage.get_latest_seed(
            seed_path, "spotify-streaming_history-*", ".json", datetime_format=DATETIME_FORMAT
        )
        for seed in latest_seeds:
            with open(seed, "r", encoding="utf-8") as file:  # noqa: UP015
                data = json.load(file)
                for datum in data:
                    yield spotify_parser.streaming_history_parser(datum)

    return follow_data, identifier, marquee, user_data, library, search_query, audio_streaming_history
