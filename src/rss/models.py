from datetime import datetime

from pydantic import BaseModel, field_validator


def default_str(string: str) -> str:
    """Coerce the default string value to a blank string."""
    if string is not None:
        return string
    else:
        return ""


class RSSFeed(BaseModel):
    feed_url: str
    title: str
    link: str
    author: str
    summary: str
    thumbnail_url: str
    thumbnail_width: str
    thumbnail_height: str
    published: datetime

    _default_str = field_validator(
        "feed_url", "title", "link", "author", "summary", "thumbnail_url", "thumbnail_width", "thumbnail_height"
    )(default_str)
