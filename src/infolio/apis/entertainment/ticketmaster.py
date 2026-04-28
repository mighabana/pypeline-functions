import os
from collections.abc import Generator
from datetime import UTC, datetime

import polars as pl

from infolio.transformations.schema import enforce_schema
from infolio.utils.api import ApiClient
from infolio.utils.auth_handlers import ApiKeyAuthHandler
from infolio.utils.logger import get_logger

logger = get_logger(__name__)

SCHEMAS = {
    "EVENTS": {
        "event_id": pl.Utf8,
        "name": pl.Utf8,
        "url": pl.Utf8,
        "status": pl.Utf8,
        "local_date": pl.Date,
        "local_time": pl.Utf8,
        "onsale_start_datetime": pl.Datetime,
        "onsale_end_datetime": pl.Datetime,
        "venue_id": pl.Utf8,
        "venue_name": pl.Utf8,
        "venue_city": pl.Utf8,
        "venue_country": pl.Utf8,
        "attraction_id": pl.Utf8,
        "attraction_name": pl.Utf8,
        "classification_segment": pl.Utf8,
        "classification_genre": pl.Utf8,
        "classification_subgenre": pl.Utf8,
        "price_min": pl.Float64,
        "price_max": pl.Float64,
        "price_currency": pl.Utf8,
        "ingestion_datetime": pl.Datetime,
    },
    "ATTRACTIONS": {
        "attraction_id": pl.Utf8,
        "name": pl.Utf8,
        "url": pl.Utf8,
        "classification_segment": pl.Utf8,
        "classification_genre": pl.Utf8,
        "classification_subgenre": pl.Utf8,
        "ingestion_datetime": pl.Datetime,
    },
    "CLASSIFICATIONS": {
        "segment_id": pl.Utf8,
        "segment_name": pl.Utf8,
        "genre_id": pl.Utf8,
        "genre_name": pl.Utf8,
        "subgenre_id": pl.Utf8,
        "subgenre_name": pl.Utf8,
        "ingestion_datetime": pl.Datetime,
    },
    "VENUES": {
        "venue_id": pl.Utf8,
        "name": pl.Utf8,
        "url": pl.Utf8,
        "address": pl.Utf8,
        "city": pl.Utf8,
        "state": pl.Utf8,
        "country": pl.Utf8,
        "country_code": pl.Utf8,
        "postal_code": pl.Utf8,
        "latitude": pl.Float64,
        "longitude": pl.Float64,
        "timezone": pl.Utf8,
        "ingestion_datetime": pl.Datetime,
    },
}

TM_BASE_URL = "https://app.ticketmaster.com/discovery/v2"

# Schengen country codes available in the Discovery API.
# Exported as a convenience constant for callers — not used internally.
SCHENGEN_COUNTRY_CODES = [
    "AT", "BE", "CZ", "DK", "FI", "FR", "DE",
    "IT", "NL", "NO", "PL", "ES", "SE", "CH",
]


class Ticketmaster:
    """
    A utility class for the Ticketmaster Discovery API v2.

    Provides general-purpose paginating access to the four main Discovery
    API entities: events, attractions, classifications, and venues.
    All filtering is left to the caller via optional parameters —
    the client has no opinion about geography, cadence, or watchlists.

    Parameters
    ----------
    api_key : str | None
        The Ticketmaster API key. If None, reads from the
        `API__TICKETMASTER__API_KEY` environment variable.
    """

    def __init__(self, api_key: str | None = None) -> None:
        """
        Initialize the Ticketmaster Discovery API client.

        Parameters
        ----------
        api_key : str | None
            API key for Ticketmaster. If None, reads from environment
            variable API__TICKETMASTER__API_KEY.
        """
        auth_handler = ApiKeyAuthHandler(
            key_name="apikey",
            api_key=api_key or os.getenv("API__TICKETMASTER__API_KEY"),
            inject_as="param",
        )

        self.api_client = ApiClient(
            base_url=TM_BASE_URL,
            headers={"Accept": "application/json"},
            timeout=10,
            max_retries=5,
            auth_handler=auth_handler,
        )
        self.api_client.reauthenticate()

    # ------------------------------------------------------------------
    # Events
    # ------------------------------------------------------------------

    def get_events(
        self,
        country_code: str | None = None,
        attraction_id: str | None = None,
        keyword: str | None = None,
        classification_name: str | None = None,
        city: str | None = None,
        start_date_time: str | None = None,
        end_date_time: str | None = None,
        page_size: int = 200,
    ) -> Generator[pl.DataFrame, None, None]:
        """
        Paginate through events matching the given filters.

        Yields one DataFrame per API page, allowing the caller to process
        and insert records incrementally. All parameters are optional —
        omitting them returns all queryable events globally. Deduplication
        is the caller's responsibility, handled at insert time via event_id.

        Parameters
        ----------
        country_code : str | None
            ISO 3166-1 alpha-2 country code (e.g., "ES", "FR", "DE").
        attraction_id : str | None
            Ticketmaster attraction ID to filter by a specific artist
            or performer.
        keyword : str | None
            Free-text keyword search against event and attraction names.
        classification_name : str | None
            Segment, genre, or subgenre name to filter by
            (e.g., "Music", "Rock", "Classical").
        city : str | None
            City name to filter events by location.
        start_date_time : str | None
            Lower bound for event start in ISO 8601 format
            (e.g., "2025-06-01T00:00:00Z").
        end_date_time : str | None
            Upper bound for event start in ISO 8601 format.
        page_size : int, default 200
            Results per page. 200 is the API maximum and minimises
            total call usage.

        Yields
        ------
        pl.DataFrame
            Normalised event records per page matching SCHEMAS["EVENTS"].

        Raises
        ------
        requests.HTTPError
            If the API request fails.
        """
        params: dict = {"size": page_size, "page": 0, "sort": "date,asc"}

        if country_code:
            params["countryCode"] = country_code
        if attraction_id:
            params["attractionId"] = attraction_id
        if keyword:
            params["keyword"] = keyword
        if classification_name:
            params["classificationName"] = classification_name
        if city:
            params["city"] = city
        if start_date_time:
            params["startDateTime"] = start_date_time
        if end_date_time:
            params["endDateTime"] = end_date_time

        logger.info(f"🎟️  Fetching events with params: {params}")

        yield from self._paginate(
            endpoint="events.json",
            params=params,
            embedded_key="events",
            parser=self._parse_event,
            schema=SCHEMAS["EVENTS"],
        )

    # ------------------------------------------------------------------
    # Attractions
    # ------------------------------------------------------------------

    def get_attractions(
        self,
        keyword: str | None = None,
        classification_name: str | None = None,
        page_size: int = 200,
    ) -> Generator[pl.DataFrame, None, None]:
        """
        Paginate through attractions matching the given filters.

        Used both for artist discovery and for resolving artist names
        (e.g. from Spotify) to Ticketmaster attraction IDs. All parameters
        are optional — omitting them returns all queryable attractions.

        Parameters
        ----------
        keyword : str | None
            Artist name or free-text search term.
        classification_name : str | None
            Segment, genre, or subgenre name to filter by
            (e.g., "Music", "Rock").
        page_size : int, default 200
            Results per page.

        Yields
        ------
        pl.DataFrame
            Normalised attraction records per page matching
            SCHEMAS["ATTRACTIONS"].

        Raises
        ------
        requests.HTTPError
            If the API request fails.
        """
        params: dict = {"size": page_size, "page": 0}

        if keyword:
            params["keyword"] = keyword
        if classification_name:
            params["classificationName"] = classification_name

        logger.info(f"🔍 Fetching attractions with params: {params}")

        yield from self._paginate(
            endpoint="attractions.json",
            params=params,
            embedded_key="attractions",
            parser=self._parse_attraction,
            schema=SCHEMAS["ATTRACTIONS"],
        )

    # ------------------------------------------------------------------
    # Classifications
    # ------------------------------------------------------------------

    def get_classifications(self) -> pl.DataFrame:
        """
        Retrieve the full segment/genre/subgenre classification taxonomy.

        Returns a flattened DataFrame where each row represents a unique
        segment → genre → subgenre combination. The taxonomy is stable
        and intended for infrequent reference refreshes rather than
        daily ingestion.

        Returns
        -------
        pl.DataFrame
            Flattened classification records matching
            SCHEMAS["CLASSIFICATIONS"].

        Raises
        ------
        requests.HTTPError
            If the API request fails.
        """
        logger.info("🗂️  Fetching classification taxonomy")

        response = self.api_client.get(
            "classifications.json", params={"size": 200}
        )
        data = response.json()

        segments_raw = data.get("_embedded", {}).get("classifications", [])
        records = []

        for segment_raw in segments_raw:
            segment = segment_raw.get("segment", {})
            segment_id = segment.get("id", "")
            segment_name = segment.get("name", "")
            genres = segment.get("_embedded", {}).get("genres", [])

            if not genres:
                records.append(self._classification_record(
                    segment_id, segment_name, "", "", "", ""
                ))
                continue

            for genre in genres:
                genre_id = genre.get("id", "")
                genre_name = genre.get("name", "")
                subgenres = genre.get("_embedded", {}).get("subgenres", [])

                if not subgenres:
                    records.append(self._classification_record(
                        segment_id, segment_name,
                        genre_id, genre_name, "", ""
                    ))
                    continue

                for subgenre in subgenres:
                    records.append(self._classification_record(
                        segment_id, segment_name,
                        genre_id, genre_name,
                        subgenre.get("id", ""),
                        subgenre.get("name", ""),
                    ))

        df = pl.DataFrame(records) if records else pl.DataFrame(
            {col: [] for col in SCHEMAS["CLASSIFICATIONS"]}
        )

        logger.info(f"✅ Retrieved {len(records)} classification row(s)")
        return enforce_schema(df, SCHEMAS["CLASSIFICATIONS"])

    # ------------------------------------------------------------------
    # Venues
    # ------------------------------------------------------------------

    def get_venues(
        self,
        country_code: str | None = None,
        keyword: str | None = None,
        city: str | None = None,
        page_size: int = 200,
    ) -> Generator[pl.DataFrame, None, None]:
        """
        Paginate through venues matching the given filters.

        All parameters are optional — omitting them returns all queryable
        venues globally. Typically called with at least a country_code
        for an initial reference load per country.

        Parameters
        ----------
        country_code : str | None
            ISO 3166-1 alpha-2 country code (e.g., "ES", "FR").
        keyword : str | None
            Venue name or free-text search term.
        city : str | None
            City name to filter results.
        page_size : int, default 200
            Results per page.

        Yields
        ------
        pl.DataFrame
            Normalised venue records per page matching SCHEMAS["VENUES"].

        Raises
        ------
        requests.HTTPError
            If the API request fails.
        """
        params: dict = {"size": page_size, "page": 0}

        if country_code:
            params["countryCode"] = country_code
        if keyword:
            params["keyword"] = keyword
        if city:
            params["city"] = city

        logger.info(f"🏟️  Fetching venues with params: {params}")

        yield from self._paginate(
            endpoint="venues.json",
            params=params,
            embedded_key="venues",
            parser=self._parse_venue,
            schema=SCHEMAS["VENUES"],
        )

    # ------------------------------------------------------------------
    # Pagination
    # ------------------------------------------------------------------

    def _paginate(
        self,
        endpoint: str,
        params: dict,
        embedded_key: str,
        parser: callable,
        schema: dict,
    ) -> Generator[pl.DataFrame, None, None]:
        """
        Generic paginator for Discovery API list endpoints.

        Walks all pages for a given endpoint and params combination,
        yielding one enforce_schema'd DataFrame per page. Shared by
        get_events, get_attractions, and get_venues to avoid duplicating
        pagination logic across methods.

        Parameters
        ----------
        endpoint : str
            API endpoint relative to base URL (e.g., "events.json").
        params : dict
            Query parameters including size and initial page=0.
        embedded_key : str
            Key within `_embedded` containing the result list
            (e.g., "events", "attractions", "venues").
        parser : callable
            Method to normalise a single raw API record into a dict.
        schema : dict
            Schema definition to enforce on each yielded DataFrame.

        Yields
        ------
        pl.DataFrame
            Normalised records per page with schema enforced.
        """
        total_pages = 1
        page = 0
        total_records = 0

        while page < total_pages:
            params["page"] = page
            response = self.api_client.get(endpoint, params=params)
            data = response.json()

            page_meta = data.get("page", {})
            total_pages = page_meta.get("totalPages", 1)
            total_elements = page_meta.get("totalElements", 0)

            if page == 0:
                logger.info(
                    f"📄 {total_elements} total record(s) across "
                    f"{total_pages} page(s) [{endpoint}]"
                )

            records_raw = data.get("_embedded", {}).get(embedded_key, [])
            if not records_raw:
                break

            records = [parser(r) for r in records_raw]
            total_records += len(records)

            logger.info(
                f"📦 Page {page + 1}/{total_pages}: {len(records)} record(s) "
                f"({total_records} total so far)"
            )

            yield enforce_schema(pl.DataFrame(records), schema)
            page += 1

        logger.info(
            f"✅ Pagination complete [{endpoint}]: "
            f"{total_records} total record(s)"
        )

    # ------------------------------------------------------------------
    # Private parsers
    # ------------------------------------------------------------------

    def _parse_event(self, raw: dict) -> dict:
        """
        Normalise a raw Discovery API event payload into a flat record.

        Parameters
        ----------
        raw : dict
            Raw event dict from the Discovery API.

        Returns
        -------
        dict
            Normalised event record matching SCHEMAS["EVENTS"].
        """
        dates = raw.get("dates", {})
        start = dates.get("start", {})
        sales = raw.get("sales", {}).get("public", {})
        embedded = raw.get("_embedded", {})

        venues = embedded.get("venues", [{}])
        venue = venues[0] if venues else {}

        attractions = embedded.get("attractions", [{}])
        attraction = attractions[0] if attractions else {}

        classifications = raw.get("classifications", [{}])
        classification = classifications[0] if classifications else {}
        segment = classification.get("segment", {})
        genre = classification.get("genre", {})
        subgenre = classification.get("subGenre", {})

        price_min, price_max, price_currency = self._extract_price_range(
            raw.get("priceRanges", [])
        )

        return {
            "event_id": raw.get("id", ""),
            "name": raw.get("name", ""),
            "url": raw.get("url", ""),
            "status": dates.get("status", {}).get("code", ""),
            "local_date": self._parse_date(start.get("localDate")),
            "local_time": start.get("localTime", ""),
            "onsale_start_datetime": self._parse_datetime(
                sales.get("startDateTime")
            ),
            "onsale_end_datetime": self._parse_datetime(
                sales.get("endDateTime")
            ),
            "venue_id": venue.get("id", ""),
            "venue_name": venue.get("name", ""),
            "venue_city": venue.get("city", {}).get("name", ""),
            "venue_country": venue.get("country", {}).get("name", ""),
            "attraction_id": attraction.get("id", ""),
            "attraction_name": attraction.get("name", ""),
            "classification_segment": segment.get("name", ""),
            "classification_genre": genre.get("name", ""),
            "classification_subgenre": subgenre.get("name", ""),
            "price_min": price_min,
            "price_max": price_max,
            "price_currency": price_currency,
            "ingestion_datetime": datetime.now(tz=UTC),
        }

    def _parse_attraction(self, raw: dict) -> dict:
        """
        Normalise a raw Discovery API attraction payload.

        Parameters
        ----------
        raw : dict
            Raw attraction dict from the Discovery API.

        Returns
        -------
        dict
            Normalised attraction record matching SCHEMAS["ATTRACTIONS"].
        """
        classifications = raw.get("classifications", [{}])
        classification = classifications[0] if classifications else {}
        segment = classification.get("segment", {})
        genre = classification.get("genre", {})
        subgenre = classification.get("subGenre", {})

        return {
            "attraction_id": raw.get("id", ""),
            "name": raw.get("name", ""),
            "url": raw.get("url", ""),
            "classification_segment": segment.get("name", ""),
            "classification_genre": genre.get("name", ""),
            "classification_subgenre": subgenre.get("name", ""),
            "ingestion_datetime": datetime.now(tz=UTC),
        }

    def _parse_venue(self, raw: dict) -> dict:
        """
        Normalise a raw Discovery API venue payload.

        Parameters
        ----------
        raw : dict
            Raw venue dict from the Discovery API.

        Returns
        -------
        dict
            Normalised venue record matching SCHEMAS["VENUES"].
        """
        location = raw.get("location", {})
        address = raw.get("address", {})

        return {
            "venue_id": raw.get("id", ""),
            "name": raw.get("name", ""),
            "url": raw.get("url", ""),
            "address": address.get("line1", ""),
            "city": raw.get("city", {}).get("name", ""),
            "state": raw.get("state", {}).get("name", ""),
            "country": raw.get("country", {}).get("name", ""),
            "country_code": raw.get("country", {}).get("countryCode", ""),
            "postal_code": raw.get("postalCode", ""),
            "latitude": float(location["latitude"])
                if location.get("latitude") is not None else None,
            "longitude": float(location["longitude"])
                if location.get("longitude") is not None else None,
            "timezone": raw.get("timezone", ""),
            "ingestion_datetime": datetime.now(tz=UTC),
        }

    # ------------------------------------------------------------------
    # Static helpers
    # ------------------------------------------------------------------

    @staticmethod
    def _classification_record(
        segment_id: str,
        segment_name: str,
        genre_id: str,
        genre_name: str,
        subgenre_id: str,
        subgenre_name: str,
    ) -> dict:
        """
        Build a flat classification record.

        Parameters
        ----------
        segment_id : str
        segment_name : str
        genre_id : str
        genre_name : str
        subgenre_id : str
        subgenre_name : str

        Returns
        -------
        dict
            Flat classification record matching SCHEMAS["CLASSIFICATIONS"].
        """
        return {
            "segment_id": segment_id,
            "segment_name": segment_name,
            "genre_id": genre_id,
            "genre_name": genre_name,
            "subgenre_id": subgenre_id,
            "subgenre_name": subgenre_name,
            "ingestion_datetime": datetime.now(tz=UTC),
        }

    @staticmethod
    def _extract_price_range(
        price_ranges: list[dict],
    ) -> tuple[float | None, float | None, str | None]:
        """
        Extract min price, max price, and currency from a priceRanges list.

        Parameters
        ----------
        price_ranges : list[dict]
            The `priceRanges` field from a Ticketmaster API payload.

        Returns
        -------
        tuple[float | None, float | None, str | None]
            (price_min, price_max, price_currency)
        """
        if not price_ranges:
            return None, None, None
        pr = price_ranges[0]
        price_min = float(pr["min"]) if pr.get("min") is not None else None
        price_max = float(pr["max"]) if pr.get("max") is not None else None
        return price_min, price_max, pr.get("currency")

    @staticmethod
    def _parse_date(value: str | None):
        """
        Parse a YYYY-MM-DD string to a date object.

        Parameters
        ----------
        value : str | None
            Date string from the API.

        Returns
        -------
        datetime.date | None
        """
        if not value:
            return None
        return datetime.strptime(value, "%Y-%m-%d").date()

    @staticmethod
    def _parse_datetime(value: str | None):
        """
        Parse an ISO 8601 datetime string to a timezone-aware datetime.

        Parameters
        ----------
        value : str | None
            Datetime string from the API (e.g., "2025-06-01T18:00:00Z").

        Returns
        -------
        datetime | None
        """
        if not value:
            return None
        return datetime.fromisoformat(value.replace("Z", "+00:00"))
