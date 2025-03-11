from collections.abc import Iterable, Sequence
from datetime import UTC, datetime
from time import mktime

import dlt
from dlt.sources import DltResource
from feedparser import parse
from pypeline_functions.rss.models import RSSFeed


@dlt.source
def rss_feed(feed_url: str) -> Sequence[DltResource]:
    """
    Extract data from an RSS feed.

    Parameters
    ----------
    feed_url : str
        The URL of the RSS feed.
    """

    @dlt.resource(name="rss_feed", write_disposition="merge", primary_key=("feed_url", "link"))
    def rss_feed() -> Iterable[RSSFeed]:
        feed = parse(feed_url)
        for entry in feed.entries:
            thumbnail = entry.get(
                "media_thumbnail", entry.get("media_content", [{"url": "", "width": "", "height": ""}])
            )[0]
            yield {
                "feed_url": feed_url,
                "title": entry.get("title"),
                "link": entry.get("link"),
                "author": entry.get("author"),
                "summary": entry.get("summary"),
                "thumbnail_url": thumbnail.get("url"),
                "thumbnail_width": thumbnail.get("width"),
                "thumbnail_height": thumbnail.get("height"),
                "published": datetime.fromtimestamp(mktime(entry.get("published_parsed")), tz=UTC),
            }

    return rss_feed
