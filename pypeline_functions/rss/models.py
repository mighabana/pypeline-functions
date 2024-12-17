from pydantic import BaseModel, field_validator


def default_str(string: str) -> str:
    """Coerce the default string value to a blank string."""
    if string is not None:
        return string
    else:
        return ""


class CNNMetadata(BaseModel):
    feed_url: str
    entry_id: str
    title: str
    summary: str
    link: str
    published: str

    _default_str = field_validator("feed_url", "entry_id", "title", "summary", "link", "published")(default_str)
