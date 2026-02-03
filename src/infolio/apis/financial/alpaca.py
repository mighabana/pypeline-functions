import os
from collections.abc import Generator
from datetime import UTC, date, datetime, timedelta

import polars as pl

from infolio.transformations.schema import enforce_schema
from infolio.utils.api import ApiClient
from infolio.utils.logger import get_logger

logger = get_logger(__name__)

SCHEMAS = {
    "STOCK_BARS": {
        "symbol": pl.Utf8,
        "timestamp": pl.Datetime,
        "bar_date": pl.Date,
        "open": pl.Float64,
        "high": pl.Float64,
        "low": pl.Float64,
        "close": pl.Float64,
        "volume": pl.Int64,
        "trade_count": pl.Int64,
        "vwap": pl.Float64,
        "ingestion_datetime": pl.Datetime,
    },
    "STOCK_SNAPSHOT": {
        "symbol": pl.Utf8,
        "latest_trade_price": pl.Float64,
        "latest_trade_size": pl.Int64,
        "latest_trade_timestamp": pl.Datetime,
        "latest_quote_ask_price": pl.Float64,
        "latest_quote_bid_price": pl.Float64,
        "latest_quote_timestamp": pl.Datetime,
        "prev_daily_close": pl.Float64,
        "ingestion_datetime": pl.Datetime,
    },
}


class Alpaca:
    """
    A utility class for the Alpaca Market Data API.

    Provides access to real-time and historical stock market data including bars (OHLCV),
    trades, quotes, and snapshots. Data is sourced from IEX (free) or SIP (paid).

    Parameters
    ----------
    api_key : str | None
        The Alpaca API key ID. If None, will use the `API__ALPACA__API_KEY`
        environment variable.
    secret_key : str | None
        The Alpaca API secret key. If None, will use the `API__ALPACA__SECRET_KEY`
        environment variable.
    feed : str, default "iex"
        Data feed to use. Options:
        - "iex": Free tier - IEX exchange data only
        - "sip": Paid tier - All exchanges (requires subscription)
    """

    def __init__(
        self,
        api_key: str | None = None,
        secret_key: str | None = None,
        feed: str = "iex",
    ) -> None:
        """
        Initialize the Alpaca API client.

        Parameters
        ----------
        api_key : str | None
            API key for Alpaca. If None, reads from API__ALPACA__API_KEY.
        secret_key : str | None
            API secret key for Alpaca. If None, reads from API__ALPACA__SECRET_KEY.
        feed : str
            Data feed - "iex" (free) or "sip" (paid).
        """
        self.api_key = api_key or os.getenv("API__ALPACA__API_KEY")
        self.secret_key = secret_key or os.getenv("API__ALPACA__SECRET_KEY")
        self.feed = feed

        self.api_client = ApiClient(
            base_url="https://data.alpaca.markets/v2",
            headers={
                "Accept": "application/json",
                "APCA-API-KEY-ID": self.api_key,
                "APCA-API-SECRET-KEY": self.secret_key,
            },
            timeout=30,
            max_retries=5,
        )

    def get_latest_bars(
        self, symbols: list[str], feed: str | None = None
    ) -> pl.DataFrame:
        """
        Retrieve the latest minute bar for stock symbols.

        Fetches the most recent minute-aggregated OHLCV data for the specified symbols.
        This is useful for getting current pricing data.

        Parameters
        ----------
        symbols : list[str]
            List of stock ticker symbols (e.g., ["AAPL", "TSLA", "MSFT"]).
        feed : str | None
            Override the default feed. If None, uses instance feed setting.

        Returns
        -------
        pl.DataFrame
            DataFrame with columns: symbol, timestamp, bar_date, open, high, low,
            close, volume, trade_count, vwap, ingestion_datetime.

        Raises
        ------
        requests.HTTPError
            If the API request fails.

        Notes
        -----
        Free tier only returns IEX exchange data. For full market data, use feed="sip"
        with a paid subscription.
        """
        endpoint = "stocks/bars/latest"
        params = {
            "symbols": ",".join(symbols),
            "feed": feed or self.feed,
        }

        logger.info(f"📊 Fetching latest bars for {len(symbols)} symbols")

        response = self.api_client.get(endpoint, params=params)
        data = response.json()

        bars_data = data.get("bars", {})

        records = []
        for symbol, bar in bars_data.items():
            timestamp = datetime.fromisoformat(bar["t"].replace("Z", "+00:00"))

            records.append(
                {
                    "symbol": symbol,
                    "timestamp": timestamp,
                    "bar_date": timestamp.date(),
                    "open": float(bar["o"]),
                    "high": float(bar["h"]),
                    "low": float(bar["l"]),
                    "close": float(bar["c"]),
                    "volume": int(bar["v"]),
                    "trade_count": int(bar["n"]),
                    "vwap": float(bar["vw"]),
                    "ingestion_datetime": datetime.now(tz=UTC),
                }
            )

        df = pl.DataFrame(records)
        logger.info(f"✅ Retrieved {len(records)} latest bars")

        return enforce_schema(df, SCHEMAS["STOCK_BARS"])

    def get_historical_bars(
        self,
        symbols: list[str],
        start_date: str | date,
        end_date: str | date | None = None,
        timeframe: str = "1Day",
        feed: str | None = None,
        limit: int | None = None,
    ) -> pl.DataFrame:
        """
        Retrieve historical bar data for stock symbols.

        Fetches OHLCV (Open, High, Low, Close, Volume) bar data for specified date range.
        Supports multiple timeframes from minutes to days.

        Parameters
        ----------
        symbols : list[str]
            List of stock ticker symbols.
        start_date : str | date
            Start date for historical data (format: "YYYY-MM-DD" or date object).
        end_date : str | date | None
            End date for historical data. If None, uses today.
        timeframe : str, default "1Day"
            Bar aggregation timeframe. Options:
            - Minutes: "1Min", "5Min", "15Min", "30Min"
            - Hours: "1Hour", "4Hour"
            - Days: "1Day"
        feed : str | None
            Override the default feed.
        limit : int | None
            Maximum number of bars to return. If None, returns all available bars.

        Returns
        -------
        pl.DataFrame
            DataFrame with unified STOCK_BARS schema.

        Raises
        ------
        requests.HTTPError
            If the API request fails.

        Notes
        -----
        The API uses pagination for large datasets. This method automatically handles
        pagination up to the specified limit or all available data.
        """
        # Convert dates to ISO format
        if isinstance(start_date, date):
            start_str = start_date.isoformat()
        else:
            start_str = start_date

        if end_date is None:
            end_str = date.now(tz=UTC).isoformat()
        elif isinstance(end_date, date):
            end_str = end_date.isoformat()
        else:
            end_str = end_date

        endpoint = "stocks/bars"
        params = {
            "symbols": ",".join(symbols),
            "start": start_str,
            "end": end_str,
            "timeframe": timeframe,
            "feed": feed or self.feed,
        }

        if limit:
            params["limit"] = limit

        logger.info(
            f"📈 Fetching historical bars: {len(symbols)} symbols, "
            f"{start_str} to {end_str}, {timeframe}"
        )

        all_records = []
        next_page_token = None
        page_count = 0

        while True:
            if next_page_token:
                params["page_token"] = next_page_token

            response = self.api_client.get(endpoint, params=params)
            data = response.json()

            bars_data = data.get("bars", {})
            page_count += 1

            # Process bars for each symbol
            for symbol, bars in bars_data.items():
                for bar in bars:
                    timestamp = datetime.fromisoformat(bar["t"].replace("Z", "+00:00"))

                    all_records.append(
                        {
                            "symbol": symbol,
                            "timestamp": timestamp,
                            "bar_date": timestamp.date(),
                            "open": float(bar["o"]),
                            "high": float(bar["h"]),
                            "low": float(bar["l"]),
                            "close": float(bar["c"]),
                            "volume": int(bar["v"]),
                            "trade_count": int(bar["n"]),
                            "vwap": float(bar["vw"]),
                            "ingestion_datetime": datetime.now(tz=UTC),
                        }
                    )

            # Check for pagination
            next_page_token = data.get("next_page_token")
            if not next_page_token:
                break

            logger.info(f"📄 Page {page_count}: {len(all_records)} bars so far...")

        df = pl.DataFrame(all_records)
        logger.info(
            f"✅ Retrieved {len(all_records)} bars across {page_count} page(s)"
        )

        return enforce_schema(df, SCHEMAS["STOCK_BARS"])

    def get_timeseries_bars(
        self,
        symbols: list[str],
        start_date: str | date,
        end_date: str | date,
        timeframe: str = "1Day",
        feed: str | None = None,
        batch_days: int = 30,
    ) -> Generator[pl.DataFrame, None, None]:
        """
        Retrieve historical bars in batches for efficient backfilling.

        Fetches historical bar data in date-range batches, yielding DataFrames for
        incremental processing. Useful for backfilling large date ranges.

        Parameters
        ----------
        symbols : list[str]
            List of stock ticker symbols.
        start_date : str | date
            Start date for backfill.
        end_date : str | date
            End date for backfill.
        timeframe : str, default "1Day"
            Bar aggregation timeframe.
        feed : str | None
            Override the default feed.
        batch_days : int, default 30
            Number of days to fetch per batch.

        Yields
        ------
        pl.DataFrame
            DataFrames containing bars for each batch.

        Notes
        -----
        This generator-based approach is memory-efficient for large backfills.
        """
        # Convert dates
        if isinstance(start_date, str):
            start_date = datetime.strptime(start_date, "%Y-%m-%d").astimezone(UTC).date()
        if isinstance(end_date, str):
            end_date = datetime.strptime(end_date, "%Y-%m-%d").astimezone(UTC).date()

        total_days = (end_date - start_date).days + 1
        logger.info(
            f"📊 Fetching timeseries data for {len(symbols)} symbols "
            f"({total_days} days in {batch_days}-day batches)"
        )

        current_start = start_date
        batch_count = 0

        while current_start <= end_date:
            current_end = min(current_start + timedelta(days=batch_days - 1), end_date)

            try:
                df = self.get_historical_bars(
                    symbols=symbols,
                    start_date=current_start,
                    end_date=current_end,
                    timeframe=timeframe,
                    feed=feed,
                )

                if df.height > 0:
                    batch_count += 1
                    logger.info(
                        f"📦 Batch #{batch_count}: {current_start} to {current_end}, "
                        f"{df.height} bars"
                    )
                    yield df

            except Exception as e:
                logger.error(
                    f"❌ Failed to fetch batch {current_start} to {current_end}: {e}"
                )

            current_start = current_end + timedelta(days=1)

        logger.info(f"✅ Completed timeseries fetch: {batch_count} batches")

    def get_snapshot(self, symbols: list[str], feed: str | None = None) -> pl.DataFrame:
        """
        Retrieve snapshot data for stock symbols.

        Fetches the latest trade, quote, minute bar, daily bar, and previous daily bar
        in a single API call. This is the most efficient way to get current market data.

        Parameters
        ----------
        symbols : list[str]
            List of stock ticker symbols.
        feed : str | None
            Override the default feed.

        Returns
        -------
        pl.DataFrame
            DataFrame with columns: symbol, latest_trade_price, latest_trade_size,
            latest_trade_timestamp, latest_quote_ask_price, latest_quote_bid_price,
            latest_quote_timestamp, prev_daily_close, ingestion_datetime.

        Raises
        ------
        requests.HTTPError
            If the API request fails.

        Notes
        -----
        Snapshots provide a comprehensive view of current market state in a single call,
        making them ideal for dashboards and monitoring applications.
        """
        endpoint = "stocks/snapshots"
        params = {
            "symbols": ",".join(symbols),
            "feed": feed or self.feed,
        }

        logger.info(f"📸 Fetching snapshots for {len(symbols)} symbols")

        response = self.api_client.get(endpoint, params=params)
        data = response.json()

        snapshots = data.get("snapshots", {}) or data

        records = []
        for symbol, snapshot in snapshots.items():
            latest_trade = snapshot.get("latestTrade", {})
            latest_quote = snapshot.get("latestQuote", {})
            prev_daily_bar = snapshot.get("prevDailyBar", {})

            # Parse timestamps
            trade_ts = None
            if latest_trade.get("t"):
                trade_ts = datetime.fromisoformat(
                    latest_trade["t"].replace("Z", "+00:00")
                )

            quote_ts = None
            if latest_quote.get("t"):
                quote_ts = datetime.fromisoformat(
                    latest_quote["t"].replace("Z", "+00:00")
                )

            records.append(
                {
                    "symbol": symbol,
                    "latest_trade_price": float(latest_trade.get("p", 0)),
                    "latest_trade_size": int(latest_trade.get("s", 0)),
                    "latest_trade_timestamp": trade_ts,
                    "latest_quote_ask_price": float(latest_quote.get("ap", 0)),
                    "latest_quote_bid_price": float(latest_quote.get("bp", 0)),
                    "latest_quote_timestamp": quote_ts,
                    "prev_daily_close": float(prev_daily_bar.get("c", 0)),
                    "ingestion_datetime": datetime.now(tz=UTC),
                }
            )

        df = pl.DataFrame(records)
        logger.info(f"✅ Retrieved {len(records)} snapshots")

        return enforce_schema(df, SCHEMAS["STOCK_SNAPSHOT"])

    def get_bar_for_symbol_on_date(
        self, symbol: str, date: str | date, timeframe: str = "1Day"
    ) -> float | None:
        """
        Retrieve the closing price for a specific symbol on a specific date.

        Convenience method to get a single price value.

        Parameters
        ----------
        symbol : str
            Stock ticker symbol.
        date : str | date
            Date to retrieve price for.
        timeframe : str
            Bar timeframe (typically "1Day" for daily close).

        Returns
        -------
        float | None
            Closing price, or None if no data available.
        """
        df = self.get_historical_bars(
            symbols=[symbol],
            start_date=date,
            end_date=date,
            timeframe=timeframe,
        )

        if df.height == 0:
            return None

        return df.select("close").row(0)[0]
