from datetime import datetime
from typing import NamedTuple

from pydantic import BaseModel, field_validator


def default_str(string: str) -> str:
    """Coerce the default string value to a blank string."""
    if string is not None:
        return string
    else:
        return ""


class CandidateLocation(BaseModel):
    lat: float
    lng: float
    address: str | None
    name: str | None
    place_id: str
    semantic_type: str | None
    location_confidence: float | None  # missing in older (around 2014/15) history


class PlaceVisit(BaseModel):
    lat: float | None
    lng: float | None
    center_lat: float | None
    center_lng: float | None
    address: str | None
    name: str | None
    location_confidence: float | None
    calibrated_probability: float | None
    place_id: str | None
    start_time: datetime | None
    end_time: datetime | None
    device_tag: int | None
    candidate_locations: list[CandidateLocation]
    place_confidence: str | None
    place_visit_type: str | None
    visit_confidence: float | None
    edit_confirmation_status: str | None
    place_visit_importance: str | None


class ChromeHistory(BaseModel):
    title: str
    page_transition: str
    url: str
    time_usec: datetime

    _default_str = field_validator("title", "page_transition", "url")(default_str)


class Subtitles(NamedTuple):
    name: str
    url: str | None


class Details(NamedTuple):
    name: str


class Activity(BaseModel):
    header: str
    title: str
    time: datetime
    description: str | None
    titleUrl: str | None
    subtitles: list[Subtitles] | None
    details: list[Details] | None
    products: list[str] | None
    activityControls: list[str] | None
