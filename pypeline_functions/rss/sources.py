from collections.abc import Iterable, Sequence

import dlt
from dlt.sources import DltResource
from feedparser import parse

from pypeline_functions.models.rss_feed import CNNMetadata


@dlt.source
def rss_feed(feed_url: str) -> Sequence[DltResource]:
    """
    Extract data from an RSS feed.

    Parameters
    ----------
    feed_url : str
        The URL of the RSS feed.
    primary_keys : list[str]
        List of primary keys to deduplicate entries
    keys : dict
        Type definition for the DynamicModel to be used for the RSS feed's metadata
    model_name : str
        Name of the dlt resource and DynamicModel
    """

    @dlt.resource(
        name="cnn_metadata",
        write_disposition="merge",
        primary_key=("feed_url", "entry_id", "title", "published"),
        columns=CNNMetadata,
    )
    def metadata() -> Iterable[CNNMetadata]:
        feed = parse(feed_url)
        for entry in feed.entries:
            yield {
                "feed_url": feed_url,
                "entry_id": entry.get("id", None),
                "title": entry.get("title", None),
                "summary": str(entry.get("summary", None)),
                "link": entry.get("link", None),
                "published": str(entry.get("published", None)),
            }

    return metadata
