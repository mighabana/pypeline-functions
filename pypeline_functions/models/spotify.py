from datetime import datetime
from typing import NamedTuple

from pydantic import BaseModel, field_validator


def default_str(string: str) -> str:
    """Coerce the default string value to a blank string."""
    if string is not None:
        return string
    else:
        return ""


class FollowData(BaseModel):
    follower_count: int
    following_users_count: int
    dismissing_users_count: int


class Identifier(BaseModel):
    identifier_type: str
    identifier_value: str

    _default_str = field_validator("identifier_type", "identifier_value", mode="before")(default_str)


class Marquee(BaseModel):
    artist_name: str
    segment: str

    _default_str = field_validator("artist_name", "segment", mode="before")(default_str)


class SearchQueries(BaseModel):
    platform: str
    search_time: datetime | None
    search_query: str
    search_interaction_URIs: list[str] | None

    _default_str = field_validator("platform", "search_query", mode="before")(default_str)


class UserData(BaseModel):
    username: str | None
    email: str
    country: str
    created_from_facebook: bool
    facebook_UID: str | None
    birthdate: datetime
    gender: str
    postal_code: str | None
    mobile_number: str | None
    mobile_operator: str | None
    mobile_brand: str | None
    creation_time: datetime

    _default_str = field_validator("email", "country", "gender", mode="before")(default_str)


class Track(NamedTuple):
    artist: str
    album: str
    track: str
    uri: str


class Album(NamedTuple):
    artist: str
    album: str
    uri: str


class Artist(NamedTuple):
    name: str
    uri: str


class Library(BaseModel):
    tracks: list[Track]
    albums: list[Album]
    shows: list | None  # unknown type my data is blank
    episodes: list | None  # unknown type my data is blank
    banned_tracks: list[Track] | None  # unknown type my data is blank
    artists: list[Artist]
    banned_artists: list[Artist] | None
    other: list[str] | None


class StreamingHistory(BaseModel):
    ts: datetime
    username: str
    platform: str
    ms_played: int | None
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
    reason_start: str | None
    reason_end: str | None
    shuffle: bool | None
    skipped: int | None
    offline: bool | None
    offline_timestamp: datetime | None
    incognito_mode: bool | None

    _default_str = field_validator(
        "username",
        "platform",
        "conn_country",
        "ip_addr_decrypted",
        "user_agent_decrypted",
        "reason_start",
        "reason_end",
        mode="before",
    )(default_str)
