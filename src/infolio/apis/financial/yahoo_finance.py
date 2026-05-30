from __future__ import annotations

from collections.abc import Generator
from datetime import UTC, date, datetime
import polars as pl
import yfinance as yf

from infolio.transformations.schema import enforce_schema
from infolio.utils.logger import get_logger

logger = get_logger(__name__)


class YahooFinance:
    """
    A utility class for Yahoo Finance data extraction.

    Provides access to stock prices, company information, dividends, splits,
    financial statements, earnings history, and analyst ratings for global
    stock markets. Uses the yfinance library which accesses Yahoo's publicly
    available financial data.

    Notes
    -----
    - Data is free but intended for personal use only per Yahoo's TOS
    - No API key required
    - Supports bulk downloads with multithreading
    - All public methods return Generator[pl.DataFrame, None, None]
    """

    # ── Schemas ────────────────────────────────────────────────────────────────

    PRICE_SCHEMA = pl.Schema({
        "ticker": pl.Utf8,
        "snapshot_timestamp": pl.Datetime("us"),
        "current_price": pl.Float64,
        "previous_close": pl.Float64,
        "open": pl.Float64,
        "day_low": pl.Float64,
        "day_high": pl.Float64,
        "volume": pl.Int64,
        "average_volume": pl.Int64,
        "average_volume_10day": pl.Int64,
        "bid": pl.Float64,
        "ask": pl.Float64,
        "bid_size": pl.Int64,
        "ask_size": pl.Int64,
        "pre_market_price": pl.Float64,
        "post_market_price": pl.Float64,
        "pre_market_change": pl.Float64,
        "post_market_change": pl.Float64,
        "ingestion_datetime": pl.Datetime("us"),
    })

    FINANCIALS_SCHEMA = pl.Schema({
        "ticker": pl.Utf8,
        "snapshot_timestamp": pl.Datetime("us"),
        "market_cap": pl.Float64,
        "trailing_pe": pl.Float64,
        "forward_pe": pl.Float64,
        "peg_ratio": pl.Float64,
        "price_to_book": pl.Float64,
        "price_to_sales": pl.Float64,
        "enterprise_value": pl.Float64,
        "enterprise_to_revenue": pl.Float64,
        "enterprise_to_ebitda": pl.Float64,
        "profit_margins": pl.Float64,
        "gross_margins": pl.Float64,
        "operating_margins": pl.Float64,
        "ebitda_margins": pl.Float64,
        "return_on_assets": pl.Float64,
        "return_on_equity": pl.Float64,
        "revenue_growth": pl.Float64,
        "earnings_growth": pl.Float64,
        "earnings_quarterly_growth": pl.Float64,
        "trailing_eps": pl.Float64,
        "forward_eps": pl.Float64,
        "revenue_per_share": pl.Float64,
        "book_value": pl.Float64,
        "total_cash_per_share": pl.Float64,
        "dividend_rate": pl.Float64,
        "dividend_yield": pl.Float64,
        "payout_ratio": pl.Float64,
        "five_year_avg_dividend_yield": pl.Float64,
        "dividend_date": pl.Date,
        "ex_dividend_date": pl.Date,
        "total_cash": pl.Float64,
        "total_debt": pl.Float64,
        "debt_to_equity": pl.Float64,
        "current_ratio": pl.Float64,
        "quick_ratio": pl.Float64,
        "free_cashflow": pl.Float64,
        "operating_cashflow": pl.Float64,
        "total_revenue": pl.Float64,
        "gross_profits": pl.Float64,
        "ebitda": pl.Float64,
        "net_income": pl.Float64,
        "fifty_two_week_low": pl.Float64,
        "fifty_two_week_high": pl.Float64,
        "fifty_two_week_change": pl.Float64,
        "fifty_day_average": pl.Float64,
        "two_hundred_day_average": pl.Float64,
        "ingestion_datetime": pl.Datetime("us"),
    })

    SENTIMENT_SCHEMA = pl.Schema({
        "ticker": pl.Utf8,
        "snapshot_timestamp": pl.Datetime("us"),
        "target_high_price": pl.Float64,
        "target_low_price": pl.Float64,
        "target_mean_price": pl.Float64,
        "target_median_price": pl.Float64,
        "recommendation_mean": pl.Float64,
        "recommendation_key": pl.Utf8,
        "number_of_analyst_opinions": pl.Int64,
        "short_ratio": pl.Float64,
        "shares_short": pl.Int64,
        "shares_short_prior_month": pl.Int64,
        "shares_percent_shares_out": pl.Float64,
        "held_percent_insiders": pl.Float64,
        "held_percent_institutions": pl.Float64,
        "shares_outstanding": pl.Int64,
        "float_shares": pl.Int64,
        "beta": pl.Float64,
        "beta_3year": pl.Float64,
        "ingestion_datetime": pl.Datetime("us"),
    })

    COMPANY_SCHEMA = pl.Schema({
        "ticker": pl.Utf8,
        "effective_date": pl.Date,
        "end_date": pl.Date,
        "is_current": pl.Boolean,
        "symbol": pl.Utf8,
        "short_name": pl.Utf8,
        "long_name": pl.Utf8,
        "sector": pl.Utf8,
        "industry": pl.Utf8,
        "industry_key": pl.Utf8,
        "sector_key": pl.Utf8,
        "country": pl.Utf8,
        "state": pl.Utf8,
        "city": pl.Utf8,
        "address": pl.Utf8,
        "zip": pl.Utf8,
        "phone": pl.Utf8,
        "website": pl.Utf8,
        "business_summary": pl.Utf8,
        "full_time_employees": pl.Int64,
        "exchange": pl.Utf8,
        "currency": pl.Utf8,
        "quote_type": pl.Utf8,
        "timezone": pl.Utf8,
        "isin": pl.Utf8,
        "uuid": pl.Utf8,
        "first_trade_date": pl.Date,
        "ingestion_datetime": pl.Datetime("us"),
        "data_hash": pl.Utf8,
    })

    COMPANY_CHANGES_SCHEMA = pl.Schema({
        "ticker": pl.Utf8,
        "change_date": pl.Date,
        "field_name": pl.Utf8,
        "old_value": pl.Utf8,
        "new_value": pl.Utf8,
        "ingestion_datetime": pl.Datetime("us"),
    })

    HISTORICAL_PRICES_SCHEMA = pl.Schema({
        "ticker": pl.Utf8,
        "date": pl.Date,
        "open": pl.Float64,
        "high": pl.Float64,
        "low": pl.Float64,
        "close": pl.Float64,
        "adj_close": pl.Float64,
        "volume": pl.Int64,
        "ingestion_datetime": pl.Datetime("us"),
    })

    DIVIDENDS_SCHEMA = pl.Schema({
        "ticker": pl.Utf8,
        "date": pl.Date,
        "dividend": pl.Float64,
        "ingestion_datetime": pl.Datetime("us"),
    })

    SPLITS_SCHEMA = pl.Schema({
        "ticker": pl.Utf8,
        "date": pl.Date,
        "split_ratio": pl.Float64,
        "ingestion_datetime": pl.Datetime("us"),
    })

    INCOME_STMT_SCHEMA = pl.Schema({
        "ticker": pl.Utf8,
        "period_type": pl.Utf8,
        "period_date": pl.Date,
        "total_revenue": pl.Float64,
        "cost_of_revenue": pl.Float64,
        "gross_profit": pl.Float64,
        "research_and_development": pl.Float64,
        "selling_general_administrative": pl.Float64,
        "operating_income": pl.Float64,
        "interest_expense": pl.Float64,
        "pretax_income": pl.Float64,
        "tax_provision": pl.Float64,
        "net_income": pl.Float64,
        "ebitda": pl.Float64,
        "basic_eps": pl.Float64,
        "diluted_eps": pl.Float64,
        "basic_shares_outstanding": pl.Float64,
        "diluted_shares_outstanding": pl.Float64,
        "ingestion_datetime": pl.Datetime("us"),
    })

    BALANCE_SHEET_SCHEMA = pl.Schema({
        "ticker": pl.Utf8,
        "period_type": pl.Utf8,
        "period_date": pl.Date,
        "total_assets": pl.Float64,
        "current_assets": pl.Float64,
        "cash_and_equivalents": pl.Float64,
        "short_term_investments": pl.Float64,
        "accounts_receivable": pl.Float64,
        "inventory": pl.Float64,
        "total_non_current_assets": pl.Float64,
        "net_ppe": pl.Float64,
        "goodwill": pl.Float64,
        "intangible_assets": pl.Float64,
        "total_liabilities": pl.Float64,
        "current_liabilities": pl.Float64,
        "accounts_payable": pl.Float64,
        "total_non_current_liabilities": pl.Float64,
        "long_term_debt": pl.Float64,
        "stockholders_equity": pl.Float64,
        "retained_earnings": pl.Float64,
        "working_capital": pl.Float64,
        "total_debt": pl.Float64,
        "net_debt": pl.Float64,
        "ingestion_datetime": pl.Datetime("us"),
    })

    CASH_FLOW_SCHEMA = pl.Schema({
        "ticker": pl.Utf8,
        "period_type": pl.Utf8,
        "period_date": pl.Date,
        "operating_cash_flow": pl.Float64,
        "net_income": pl.Float64,
        "depreciation_amortization": pl.Float64,
        "stock_based_compensation": pl.Float64,
        "change_in_working_capital": pl.Float64,
        "capital_expenditures": pl.Float64,
        "investing_cash_flow": pl.Float64,
        "financing_cash_flow": pl.Float64,
        "free_cash_flow": pl.Float64,
        "end_cash_position": pl.Float64,
        "ingestion_datetime": pl.Datetime("us"),
    })

    EARNINGS_HISTORY_SCHEMA = pl.Schema({
        "ticker": pl.Utf8,
        "report_date": pl.Date,
        "eps_estimate": pl.Float64,
        "eps_actual": pl.Float64,
        "eps_difference": pl.Float64,
        "surprise_percent": pl.Float64,
        "ingestion_datetime": pl.Datetime("us"),
    })

    ANALYST_RATINGS_SCHEMA = pl.Schema({
        "ticker": pl.Utf8,
        "rating_date": pl.Date,
        "firm": pl.Utf8,
        "from_grade": pl.Utf8,
        "to_grade": pl.Utf8,
        "action": pl.Utf8,
        "ingestion_datetime": pl.Datetime("us"),
    })

    # ── Inner class: shared lazy batch source for latest-info streams ──────────

    class _LatestInfoSource:
        """
        Lazily fetches and caches batches from _iter_latest_info_batches so that
        get_price_snapshots, get_financials, and get_market_sentiment can all share
        a single API walk without triple-fetching.

        Each stream method tracks its own index and calls _advance_to() to fetch
        only as many batches as needed. Already-fetched batches are held in memory
        for subsequent stream consumers.
        """

        def __init__(self, gen: Generator) -> None:
            self._gen = gen
            self._batches: list[tuple[pl.DataFrame, pl.DataFrame, pl.DataFrame]] = []
            self._done = False

        def _advance_to(self, index: int) -> None:
            while not self._done and len(self._batches) <= index:
                try:
                    self._batches.append(next(self._gen))
                except StopIteration:
                    self._done = True

        def prices_stream(self) -> Generator[pl.DataFrame, None, None]:
            i = 0
            while True:
                self._advance_to(i)
                if i >= len(self._batches):
                    return
                yield self._batches[i][0]
                i += 1

        def financials_stream(self) -> Generator[pl.DataFrame, None, None]:
            i = 0
            while True:
                self._advance_to(i)
                if i >= len(self._batches):
                    return
                yield self._batches[i][1]
                i += 1

        def sentiment_stream(self) -> Generator[pl.DataFrame, None, None]:
            i = 0
            while True:
                self._advance_to(i)
                if i >= len(self._batches):
                    return
                yield self._batches[i][2]
                i += 1

    # ── Init ──────────────────────────────────────────────────────────────────

    def __init__(self) -> None:
        """Initialize the Yahoo Finance client."""
        self._latest_info_cache: tuple[tuple, _LatestInfoSource] | None = None
        logger.info("🔧 Initialized Yahoo Finance client")

    # ── Public API ─────────────────────────────────────────────────────────────

    def get_price_snapshots(
        self,
        tickers: list[str],
        use_threads: bool = True,
        batch_size: int = 100,
    ) -> Generator[pl.DataFrame, None, None]:
        """
        Retrieve latest price snapshots for a list of tickers.

        Yields one DataFrame per batch containing current price data:
        bid/ask, day range, volume, pre/post market prices.

        Shares the underlying API walk with get_financials() and
        get_market_sentiment() when called with the same arguments on the
        same instance, avoiding redundant requests.

        Parameters
        ----------
        tickers : list[str]
            List of ticker symbols.
        use_threads : bool, default True
            Enable multithreading for faster processing.
        batch_size : int, default 100
            Number of tickers to process per batch.

        Yields
        ------
        pl.DataFrame
            DataFrame with schema PRICE_SCHEMA for each batch.
        """
        source = self._get_latest_info_source(tickers, batch_size, use_threads)
        yield from source.prices_stream()

    def get_financials(
        self,
        tickers: list[str],
        use_threads: bool = True,
        batch_size: int = 100,
    ) -> Generator[pl.DataFrame, None, None]:
        """
        Retrieve latest financial snapshot metrics for a list of tickers.

        Yields one DataFrame per batch containing valuation multiples,
        profitability ratios, balance sheet summary, and growth metrics
        derived from the Yahoo Finance .info endpoint.

        Shares the underlying API walk with get_price_snapshots() and
        get_market_sentiment() when called with the same arguments on the
        same instance, avoiding redundant requests.

        Parameters
        ----------
        tickers : list[str]
            List of ticker symbols.
        use_threads : bool, default True
            Enable multithreading for faster processing.
        batch_size : int, default 100
            Number of tickers to process per batch.

        Yields
        ------
        pl.DataFrame
            DataFrame with schema FINANCIALS_SCHEMA for each batch.
        """
        source = self._get_latest_info_source(tickers, batch_size, use_threads)
        yield from source.financials_stream()

    def get_market_sentiment(
        self,
        tickers: list[str],
        use_threads: bool = True,
        batch_size: int = 100,
    ) -> Generator[pl.DataFrame, None, None]:
        """
        Retrieve latest market sentiment data for a list of tickers.

        Yields one DataFrame per batch containing analyst price targets,
        recommendation consensus, short interest, and ownership metrics.

        Shares the underlying API walk with get_price_snapshots() and
        get_financials() when called with the same arguments on the same
        instance, avoiding redundant requests.

        Parameters
        ----------
        tickers : list[str]
            List of ticker symbols.
        use_threads : bool, default True
            Enable multithreading for faster processing.
        batch_size : int, default 100
            Number of tickers to process per batch.

        Yields
        ------
        pl.DataFrame
            DataFrame with schema SENTIMENT_SCHEMA for each batch.
        """
        source = self._get_latest_info_source(tickers, batch_size, use_threads)
        yield from source.sentiment_stream()

    def get_company_static(
        self,
        tickers: list[str],
        use_threads: bool = True,
        batch_size: int = 100,
    ) -> Generator[pl.DataFrame, None, None]:
        """
        Retrieve company static information in SCD Type 2 format.

        Yields one DataFrame per batch containing company profile data that
        rarely changes: name, sector, industry, location, business description,
        employee count, exchange info. Includes effective_date, end_date,
        is_current, and data_hash fields for change tracking.

        Parameters
        ----------
        tickers : list[str]
            List of ticker symbols.
        use_threads : bool, default True
            Enable multithreading.
        batch_size : int, default 100
            Number of tickers per batch.

        Yields
        ------
        pl.DataFrame
            DataFrame with schema COMPANY_SCHEMA for each batch.
        """
        logger.info(f"🏢 Fetching company static data for {len(tickers)} tickers")
        total_batches = (len(tickers) + batch_size - 1) // batch_size

        for batch_num, batch in enumerate(self._chunk_list(tickers, batch_size), 1):
            logger.info(f"  Processing batch {batch_num}/{total_batches} ({len(batch)} tickers)")
            batch_frames = []
            failed = []

            if use_threads:
                from concurrent.futures import ThreadPoolExecutor, as_completed

                with ThreadPoolExecutor(max_workers=10) as executor:
                    futures = {
                        executor.submit(self._fetch_single_company_static, ticker): ticker
                        for ticker in batch
                    }
                    for future in as_completed(futures):
                        ticker = futures[future]
                        try:
                            result = future.result()
                            if result.height > 0:
                                batch_frames.append(result)
                        except Exception as e:
                            logger.warning(f"  Failed {ticker}: {e}")
                            failed.append(ticker)
            else:
                for ticker in batch:
                    result = self._fetch_single_company_static(ticker)
                    if result.height > 0:
                        batch_frames.append(result)
                    else:
                        failed.append(ticker)

            if failed:
                logger.info(f"  🔄 Retrying {len(failed)} failed tickers")
                for ticker in failed:
                    result = self._fetch_single_company_static(ticker)
                    if result.height > 0:
                        batch_frames.append(result)

            if batch_frames:
                batch_df = pl.concat(batch_frames)
                logger.info(f"  ✅ Batch {batch_num} complete: {batch_df.height} records")
                yield batch_df

        logger.info(f"✅ Completed company static fetch: {total_batches} batches")

    def detect_static_changes(
        self,
        current_data: pl.DataFrame,
        previous_data: pl.DataFrame,
    ) -> Generator[pl.DataFrame, None, None]:
        """
        Detect field-level changes between current and previous company static data.

        Compares two company_static DataFrames and yields a single DataFrame
        of field-level changes in COMPANY_CHANGES_SCHEMA format. Yields nothing
        if no changes are detected or previous_data is empty (first load).

        Parameters
        ----------
        current_data : pl.DataFrame
            Current company static data (from get_company_static).
        previous_data : pl.DataFrame
            Previous company static data (from database).

        Yields
        ------
        pl.DataFrame
            DataFrame with schema COMPANY_CHANGES_SCHEMA. Only yielded when
            at least one change is detected.
        """
        if previous_data.height == 0:
            logger.info("No previous data — first load, no changes to detect")
            return

        changes = []
        change_date = datetime.now(UTC).replace(tzinfo=None).date()

        fields_to_monitor = [
            "symbol", "short_name", "long_name", "sector", "industry",
            "industry_key", "sector_key", "country", "state", "city",
            "address", "zip", "phone", "website", "business_summary",
            "full_time_employees", "exchange", "currency", "quote_type",
            "timezone", "isin",
        ]

        for ticker in current_data["ticker"].unique():
            current_row = current_data.filter(pl.col("ticker") == ticker)
            previous_row = previous_data.filter(pl.col("ticker") == ticker)

            if previous_row.height == 0:
                continue

            for field in fields_to_monitor:
                try:
                    current_val = current_row[field][0]
                    previous_val = previous_row[field][0]
                    if current_val != previous_val:
                        changes.append({
                            "ticker": ticker,
                            "change_date": change_date,
                            "field_name": field,
                            "old_value": str(previous_val) if previous_val is not None else None,
                            "new_value": str(current_val) if current_val is not None else None,
                            "ingestion_datetime": datetime.now(UTC).replace(tzinfo=None),
                        })
                except Exception as e:
                    logger.warning(f"Error comparing {field} for {ticker}: {e}")

        if changes:
            logger.info(f"🔍 Detected {len(changes)} field changes")
            df = pl.DataFrame(changes)
            yield enforce_schema(df, self.COMPANY_CHANGES_SCHEMA)
        else:
            logger.info("✅ No changes detected")

    def get_timeseries_prices(
        self,
        tickers: list[str],
        start_date: str | date,
        end_date: str | date,
        interval: str = "1d",
        batch_size: int = 100,
    ) -> Generator[pl.DataFrame, None, None]:
        """
        Retrieve historical OHLCV price data in batches for large ticker lists.

        Downloads adjusted and unadjusted close prices, open, high, low, and
        volume for a date range. Useful for backtesting, technical analysis,
        volatility modeling, and cross-asset correlation in ClickHouse.

        Parameters
        ----------
        tickers : list[str]
            List of ticker symbols.
        start_date : str | date
            Start date (YYYY-MM-DD or date object).
        end_date : str | date
            End date (YYYY-MM-DD or date object).
        interval : str, default "1d"
            Data interval. Valid: 1d, 5d, 1wk, 1mo, 3mo.
        batch_size : int, default 100
            Number of tickers to process per batch.

        Yields
        ------
        pl.DataFrame
            DataFrame with schema HISTORICAL_PRICES_SCHEMA for each batch.
        """
        total_tickers = len(tickers)
        total_batches = (total_tickers + batch_size - 1) // batch_size
        logger.info(
            f"📦 Fetching time series prices for {total_tickers} tickers "
            f"in batches of {batch_size}"
        )

        for batch_num, batch in enumerate(self._chunk_list(tickers, batch_size), 1):
            logger.info(
                f"📊 Processing batch {batch_num}/{total_batches} "
                f"({len(batch)} tickers)"
            )
            try:
                df = self._fetch_historical_prices_batch(
                    tickers=batch,
                    start_date=start_date,
                    end_date=end_date,
                    interval=interval,
                    use_threads=True,
                )
                if df.height > 0:
                    logger.info(f"✅ Batch {batch_num}: {df.height:,} records")
                    yield df
            except Exception as e:
                logger.error(f"❌ Failed to process batch {batch_num}: {e}")
                continue

        logger.info(f"✅ Completed time series fetch: {total_batches} batches")

    def get_dividends(
        self,
        tickers: list[str],
        batch_size: int = 50,
    ) -> Generator[pl.DataFrame, None, None]:
        """
        Retrieve full dividend history for a list of tickers.

        Yields one DataFrame per batch containing all historical dividend
        payments. Useful for computing total return (price + dividends),
        dividend growth rates, and payout consistency screening in ClickHouse.

        Parameters
        ----------
        tickers : list[str]
            List of ticker symbols.
        batch_size : int, default 50
            Number of tickers to process per batch.

        Yields
        ------
        pl.DataFrame
            DataFrame with schema DIVIDENDS_SCHEMA for each batch.
        """
        total_batches = (len(tickers) + batch_size - 1) // batch_size
        logger.info(f"💰 Fetching dividend history for {len(tickers)} tickers")

        for batch_num, batch in enumerate(self._chunk_list(tickers, batch_size), 1):
            records = []
            for ticker_symbol in batch:
                try:
                    ticker = yf.Ticker(ticker_symbol)
                    dividends = ticker.dividends
                    if dividends.empty:
                        logger.debug(f"ℹ️ No dividends for {ticker_symbol}")
                        continue
                    for div_date, dividend in dividends.items():
                        records.append({
                            "ticker": ticker_symbol,
                            "date": div_date.date(),
                            "dividend": float(dividend),
                            "ingestion_datetime": datetime.now(UTC).replace(tzinfo=None),
                        })
                except Exception as e:
                    logger.error(f"❌ Failed to fetch dividends for {ticker_symbol}: {e}")

            if records:
                df = enforce_schema(pl.DataFrame(records), self.DIVIDENDS_SCHEMA)
                logger.info(f"  ✅ Batch {batch_num}/{total_batches}: {df.height} records")
                yield df

        logger.info("✅ Completed dividend fetch")

    def get_splits(
        self,
        tickers: list[str],
        batch_size: int = 50,
    ) -> Generator[pl.DataFrame, None, None]:
        """
        Retrieve full stock split history for a list of tickers.

        Yields one DataFrame per batch. Split history is needed to adjust
        raw OHLCV prices for splits and to detect structural price level
        changes that are not real market moves.

        Parameters
        ----------
        tickers : list[str]
            List of ticker symbols.
        batch_size : int, default 50
            Number of tickers to process per batch.

        Yields
        ------
        pl.DataFrame
            DataFrame with schema SPLITS_SCHEMA for each batch.
        """
        total_batches = (len(tickers) + batch_size - 1) // batch_size
        logger.info(f"🔀 Fetching split history for {len(tickers)} tickers")

        for batch_num, batch in enumerate(self._chunk_list(tickers, batch_size), 1):
            records = []
            for ticker_symbol in batch:
                try:
                    ticker = yf.Ticker(ticker_symbol)
                    splits = ticker.splits
                    if splits.empty:
                        logger.debug(f"ℹ️ No splits for {ticker_symbol}")
                        continue
                    for split_date, split_ratio in splits.items():
                        records.append({
                            "ticker": ticker_symbol,
                            "date": split_date.date(),
                            "split_ratio": float(split_ratio),
                            "ingestion_datetime": datetime.now(UTC).replace(tzinfo=None),
                        })
                except Exception as e:
                    logger.error(f"❌ Failed to fetch splits for {ticker_symbol}: {e}")

            if records:
                df = enforce_schema(pl.DataFrame(records), self.SPLITS_SCHEMA)
                logger.info(f"  ✅ Batch {batch_num}/{total_batches}: {df.height} records")
                yield df

        logger.info("✅ Completed split fetch")

    def get_financial_statements(
        self,
        tickers: list[str],
        freq: str = "quarterly",
    ) -> Generator[pl.DataFrame, None, None]:
        """
        Retrieve structured financial statements for a list of tickers.

        Yields three DataFrames per ticker: income statement, balance sheet,
        and cash flow statement. Data covers the last 4 fiscal years (annual)
        or last 5 quarters (quarterly).

        This provides the full reported line items behind the snapshot ratios
        in get_financials(), enabling revenue CAGR, FCF yield over time,
        debt trajectory, and earnings quality analysis in ClickHouse.

        Parameters
        ----------
        tickers : list[str]
            List of ticker symbols.
        freq : str, default "quarterly"
            Reporting frequency. Either "quarterly" or "annual".

        Yields
        ------
        pl.DataFrame
            Alternating DataFrames in this order per ticker:
            income statement (INCOME_STMT_SCHEMA),
            balance sheet (BALANCE_SHEET_SCHEMA),
            cash flow (CASH_FLOW_SCHEMA).
            Empty schemas are not yielded.
        """
        logger.info(
            f"📋 Fetching {freq} financial statements for {len(tickers)} tickers"
        )

        for ticker_symbol in tickers:
            try:
                ticker = yf.Ticker(ticker_symbol)
                ingestion_time = datetime.now(UTC).replace(tzinfo=None)

                income_df = self._parse_income_stmt(ticker, ticker_symbol, freq, ingestion_time)
                balance_df = self._parse_balance_sheet(ticker, ticker_symbol, freq, ingestion_time)
                cashflow_df = self._parse_cash_flow(ticker, ticker_symbol, freq, ingestion_time)

                if income_df.height > 0:
                    yield income_df
                if balance_df.height > 0:
                    yield balance_df
                if cashflow_df.height > 0:
                    yield cashflow_df

            except Exception as e:
                logger.error(f"❌ Failed to fetch financial statements for {ticker_symbol}: {e}")

        logger.info("✅ Completed financial statements fetch")

    def get_earnings_history(
        self,
        tickers: list[str],
        batch_size: int = 50,
    ) -> Generator[pl.DataFrame, None, None]:
        """
        Retrieve historical EPS estimates vs actuals for a list of tickers.

        Yields one DataFrame per batch containing each earnings report date,
        the consensus EPS estimate, the actual reported EPS, the difference,
        and the surprise percentage.

        EPS surprise and revision momentum are empirically validated return
        predictors. This table enables surprise history screening and
        correlation of beats/misses with subsequent price moves in ClickHouse.

        Parameters
        ----------
        tickers : list[str]
            List of ticker symbols.
        batch_size : int, default 50
            Number of tickers to process per batch.

        Yields
        ------
        pl.DataFrame
            DataFrame with schema EARNINGS_HISTORY_SCHEMA for each batch.
        """
        total_batches = (len(tickers) + batch_size - 1) // batch_size
        logger.info(f"📅 Fetching earnings history for {len(tickers)} tickers")

        for batch_num, batch in enumerate(self._chunk_list(tickers, batch_size), 1):
            records = []
            for ticker_symbol in batch:
                try:
                    ticker = yf.Ticker(ticker_symbol)
                    history = ticker.earnings_history

                    if history is None or history.empty:
                        logger.debug(f"ℹ️ No earnings history for {ticker_symbol}")
                        continue

                    ingestion_time = datetime.now(UTC).replace(tzinfo=None)
                    for idx, row in history.iterrows():
                        report_date = idx.date() if hasattr(idx, "date") else None
                        records.append({
                            "ticker": ticker_symbol,
                            "report_date": report_date,
                            "eps_estimate": float(row["epsEstimate"]) if row.get("epsEstimate") is not None else None,
                            "eps_actual": float(row["epsActual"]) if row.get("epsActual") is not None else None,
                            "eps_difference": float(row["epsDifference"]) if row.get("epsDifference") is not None else None,
                            "surprise_percent": float(row["surprisePercent"]) if row.get("surprisePercent") is not None else None,
                            "ingestion_datetime": ingestion_time,
                        })

                except Exception as e:
                    logger.error(f"❌ Failed to fetch earnings history for {ticker_symbol}: {e}")

            if records:
                df = enforce_schema(pl.DataFrame(records), self.EARNINGS_HISTORY_SCHEMA)
                logger.info(f"  ✅ Batch {batch_num}/{total_batches}: {df.height} records")
                yield df

        logger.info("✅ Completed earnings history fetch")

    def get_analyst_ratings(
        self,
        tickers: list[str],
        batch_size: int = 50,
    ) -> Generator[pl.DataFrame, None, None]:
        """
        Retrieve analyst upgrade/downgrade history for a list of tickers.

        Yields one DataFrame per batch containing the full history of rating
        changes from named analyst firms: date, firm, from-grade, to-grade,
        and action type.

        Richer than the snapshot recommendation_key in get_market_sentiment(),
        this table enables tracking whether specific firm upgrades/downgrades
        precede price moves and monitoring analyst consensus drift over time.

        Parameters
        ----------
        tickers : list[str]
            List of ticker symbols.
        batch_size : int, default 50
            Number of tickers to process per batch.

        Yields
        ------
        pl.DataFrame
            DataFrame with schema ANALYST_RATINGS_SCHEMA for each batch.
        """
        total_batches = (len(tickers) + batch_size - 1) // batch_size
        logger.info(f"📊 Fetching analyst ratings for {len(tickers)} tickers")

        for batch_num, batch in enumerate(self._chunk_list(tickers, batch_size), 1):
            records = []
            for ticker_symbol in batch:
                try:
                    ticker = yf.Ticker(ticker_symbol)
                    upgrades = ticker.upgrades_downgrades

                    if upgrades is None or upgrades.empty:
                        logger.debug(f"ℹ️ No analyst ratings for {ticker_symbol}")
                        continue

                    ingestion_time = datetime.now(UTC).replace(tzinfo=None)
                    for idx, row in upgrades.iterrows():
                        rating_date = idx.date() if hasattr(idx, "date") else None
                        records.append({
                            "ticker": ticker_symbol,
                            "rating_date": rating_date,
                            "firm": row.get("Firm"),
                            "from_grade": row.get("FromGrade"),
                            "to_grade": row.get("ToGrade"),
                            "action": row.get("Action"),
                            "ingestion_datetime": ingestion_time,
                        })

                except Exception as e:
                    logger.error(f"❌ Failed to fetch analyst ratings for {ticker_symbol}: {e}")

            if records:
                df = enforce_schema(pl.DataFrame(records), self.ANALYST_RATINGS_SCHEMA)
                logger.info(f"  ✅ Batch {batch_num}/{total_batches}: {df.height} records")
                yield df

        logger.info("✅ Completed analyst ratings fetch")

    # ── Private: latest-info shared source ────────────────────────────────────

    def _get_latest_info_source(
        self,
        tickers: list[str],
        batch_size: int,
        use_threads: bool,
    ) -> _LatestInfoSource:
        """Return a shared _LatestInfoSource for the given call parameters.

        Reuses the cached source when called with identical arguments so that
        get_price_snapshots, get_financials, and get_market_sentiment share one
        API walk. Creates a fresh source when parameters differ.
        """
        cache_key = (tuple(tickers), batch_size, use_threads)
        if self._latest_info_cache is None or self._latest_info_cache[0] != cache_key:
            self._latest_info_cache = (
                cache_key,
                self._LatestInfoSource(
                    self._iter_latest_info_batches(tickers, batch_size, use_threads)
                ),
            )
        return self._latest_info_cache[1]

    def _iter_latest_info_batches(
        self,
        tickers: list[str],
        batch_size: int,
        use_threads: bool,
    ) -> Generator[tuple[pl.DataFrame, pl.DataFrame, pl.DataFrame], None, None]:
        """Yield (prices_df, financials_df, sentiment_df) tuples one per batch."""
        total_batches = (len(tickers) + batch_size - 1) // batch_size
        logger.info(
            f"📊 Fetching all latest info for {len(tickers)} tickers "
            f"(batch_size={batch_size})"
        )

        for batch_num, batch in enumerate(self._chunk_list(tickers, batch_size), 1):
            logger.info(
                f"  Processing batch {batch_num}/{total_batches} ({len(batch)} tickers)"
            )
            prices, financials, sentiment, failed = self._fetch_batch_all_info(
                batch, use_threads
            )
            logger.info(
                f"  ✅ Batch {batch_num} complete: "
                f"{prices.height} prices, "
                f"{financials.height} financials, "
                f"{sentiment.height} sentiment, "
                f"{len(failed)} failed"
            )
            yield prices, financials, sentiment

        logger.info(f"✅ Completed all {total_batches} batches")

    # ── Private: batch/single fetchers ────────────────────────────────────────

    def _fetch_batch_all_info(
        self, tickers: list[str], use_threads: bool
    ) -> tuple[pl.DataFrame, pl.DataFrame, pl.DataFrame, list]:
        """
        Fetch all info for a batch of tickers from a single .info call per ticker.

        Returns
        -------
        tuple[pl.DataFrame, pl.DataFrame, pl.DataFrame, list]
            (price_snapshots, financials_timeseries, market_sentiment_timeseries,
            failed_tickers)
        """
        price_records = []
        financial_records = []
        sentiment_records = []
        failed_tickers = []

        if use_threads:
            from concurrent.futures import ThreadPoolExecutor, as_completed

            with ThreadPoolExecutor(max_workers=10) as executor:
                futures = {
                    executor.submit(self._fetch_single_all_info, ticker): ticker
                    for ticker in tickers
                }
                for future in as_completed(futures):
                    ticker = futures[future]
                    try:
                        result = future.result()
                        if result:
                            price_records.append(result["price"])
                            financial_records.append(result["financial"])
                            sentiment_records.append(result["sentiment"])
                    except Exception as e:
                        logger.warning(f"  Failed {ticker}: {e}")
                        failed_tickers.append(ticker)
        else:
            for ticker in tickers:
                try:
                    result = self._fetch_single_all_info(ticker)
                    if result:
                        price_records.append(result["price"])
                        financial_records.append(result["financial"])
                        sentiment_records.append(result["sentiment"])
                except Exception as e:
                    logger.warning(f"  Failed {ticker}: {e}")
                    failed_tickers.append(ticker)

        if failed_tickers:
            logger.info(f"  🔄 Retrying {len(failed_tickers)} failed tickers")
            for ticker in failed_tickers:
                try:
                    result = self._fetch_single_all_info(ticker)
                    if result:
                        price_records.append(result["price"])
                        financial_records.append(result["financial"])
                        sentiment_records.append(result["sentiment"])
                except Exception as e:
                    logger.error(f"  ❌ Retry failed for {ticker}: {e}")

        prices_df = enforce_schema(
            pl.DataFrame(price_records) if price_records else pl.DataFrame(),
            self.PRICE_SCHEMA,
        )
        financials_df = enforce_schema(
            pl.DataFrame(financial_records) if financial_records else pl.DataFrame(),
            self.FINANCIALS_SCHEMA,
        )
        sentiment_df = enforce_schema(
            pl.DataFrame(sentiment_records) if sentiment_records else pl.DataFrame(),
            self.SENTIMENT_SCHEMA,
        )
        failed_ticker_list = sorted(
            set(tickers) - {r["ticker"] for r in price_records}
        )

        return prices_df, financials_df, sentiment_df, failed_ticker_list

    def _fetch_single_all_info(
        self,
        ticker_symbol: str,
        retry_count: int = 0,
        max_retries: int = 3,
    ) -> dict | None:
        """
        Fetch all info for a single ticker from one .info call.

        Returns a dict with 'price', 'financial', 'sentiment' record dicts,
        or None if the fetch failed after retries.
        """
        import time
        time.sleep(0.1)

        try:
            ticker = yf.Ticker(ticker_symbol)
            info = ticker.info

            if not info.get("regularMarketPrice") and not info.get("currentPrice"):
                logger.warning(
                    f"  ⚠️ No valid data for {ticker_symbol} — possibly delisted or invalid"
                )
                return None

            try:
                fast_info = ticker.fast_info
                current_price = fast_info.get("lastPrice")
                previous_close = fast_info.get("previousClose")
                open_price = fast_info.get("open")
                day_low = fast_info.get("dayLow")
                day_high = fast_info.get("dayHigh")
                volume = fast_info.get("volume")
            except Exception:
                current_price = info.get("currentPrice") or info.get("regularMarketPrice")
                previous_close = info.get("previousClose")
                open_price = info.get("open") or info.get("regularMarketOpen")
                day_low = info.get("dayLow") or info.get("regularMarketDayLow")
                day_high = info.get("dayHigh") or info.get("regularMarketDayHigh")
                volume = info.get("volume") or info.get("regularMarketVolume")

            timestamp = info.get("regularMarketTime")
            snapshot_timestamp = (
                datetime.fromtimestamp(timestamp, tz=UTC).replace(tzinfo=None)
                if timestamp
                else datetime.now(UTC).replace(tzinfo=None)
            )
            ingestion_time = datetime.now(UTC).replace(tzinfo=None)

            price_record = {
                "ticker": ticker_symbol,
                "snapshot_timestamp": snapshot_timestamp,
                "current_price": float(current_price) if current_price else None,
                "previous_close": float(previous_close) if previous_close else None,
                "open": float(open_price) if open_price else None,
                "day_low": float(day_low) if day_low else None,
                "day_high": float(day_high) if day_high else None,
                "volume": int(volume) if volume else None,
                "average_volume": int(v) if (v := info.get("averageVolume")) else None,
                "average_volume_10day": int(v) if (v := info.get("averageDailyVolume10Day")) else None,
                "bid": float(v) if (v := info.get("bid")) else None,
                "ask": float(v) if (v := info.get("ask")) else None,
                "bid_size": int(v) if (v := info.get("bidSize")) else None,
                "ask_size": int(v) if (v := info.get("askSize")) else None,
                "pre_market_price": float(v) if (v := info.get("preMarketPrice")) else None,
                "post_market_price": float(v) if (v := info.get("postMarketPrice")) else None,
                "pre_market_change": float(v) if (v := info.get("preMarketChange")) else None,
                "post_market_change": float(v) if (v := info.get("postMarketChange")) else None,
                "ingestion_datetime": ingestion_time,
            }

            financial_record = {
                "ticker": ticker_symbol,
                "snapshot_timestamp": snapshot_timestamp,
                "market_cap": float(v) if (v := info.get("marketCap")) else None,
                "trailing_pe": float(v) if (v := info.get("trailingPE")) else None,
                "forward_pe": float(v) if (v := info.get("forwardPE")) else None,
                "peg_ratio": float(v) if (v := info.get("pegRatio") or info.get("trailingPegRatio")) else None,
                "price_to_book": float(v) if (v := info.get("priceToBook")) else None,
                "price_to_sales": float(v) if (v := info.get("priceToSalesTrailing12Months")) else None,
                "enterprise_value": float(v) if (v := info.get("enterpriseValue")) else None,
                "enterprise_to_revenue": float(v) if (v := info.get("enterpriseToRevenue")) else None,
                "enterprise_to_ebitda": float(v) if (v := info.get("enterpriseToEbitda")) else None,
                "profit_margins": float(v) if (v := info.get("profitMargins")) else None,
                "gross_margins": float(v) if (v := info.get("grossMargins")) else None,
                "operating_margins": float(v) if (v := info.get("operatingMargins")) else None,
                "ebitda_margins": float(v) if (v := info.get("ebitdaMargins")) else None,
                "return_on_assets": float(v) if (v := info.get("returnOnAssets")) else None,
                "return_on_equity": float(v) if (v := info.get("returnOnEquity")) else None,
                "revenue_growth": float(v) if (v := info.get("revenueGrowth")) else None,
                "earnings_growth": float(v) if (v := info.get("earningsGrowth")) else None,
                "earnings_quarterly_growth": float(v) if (v := info.get("earningsQuarterlyGrowth")) else None,
                "trailing_eps": float(v) if (v := info.get("trailingEps")) else None,
                "forward_eps": float(v) if (v := info.get("forwardEps")) else None,
                "revenue_per_share": float(v) if (v := info.get("revenuePerShare")) else None,
                "book_value": float(v) if (v := info.get("bookValue")) else None,
                "total_cash_per_share": float(v) if (v := info.get("totalCashPerShare")) else None,
                "dividend_rate": float(v) if (v := info.get("dividendRate")) else None,
                "dividend_yield": float(v) if (v := info.get("dividendYield")) else None,
                "payout_ratio": float(v) if (v := info.get("payoutRatio")) else None,
                "five_year_avg_dividend_yield": float(v) if (v := info.get("fiveYearAvgDividendYield")) else None,
                "dividend_date": datetime.fromtimestamp(v, tz=UTC).date() if (v := info.get("dividendDate")) else None,
                "ex_dividend_date": datetime.fromtimestamp(v, tz=UTC).date() if (v := info.get("exDividendDate")) else None,
                "total_cash": float(v) if (v := info.get("totalCash")) else None,
                "total_debt": float(v) if (v := info.get("totalDebt")) else None,
                "debt_to_equity": float(v) if (v := info.get("debtToEquity")) else None,
                "current_ratio": float(v) if (v := info.get("currentRatio")) else None,
                "quick_ratio": float(v) if (v := info.get("quickRatio")) else None,
                "free_cashflow": float(v) if (v := info.get("freeCashflow")) else None,
                "operating_cashflow": float(v) if (v := info.get("operatingCashflow")) else None,
                "total_revenue": float(v) if (v := info.get("totalRevenue")) else None,
                "gross_profits": float(v) if (v := info.get("grossProfits")) else None,
                "ebitda": float(v) if (v := info.get("ebitda")) else None,
                "net_income": float(v) if (v := info.get("netIncomeToCommon")) else None,
                "fifty_two_week_low": float(v) if (v := info.get("fiftyTwoWeekLow")) else None,
                "fifty_two_week_high": float(v) if (v := info.get("fiftyTwoWeekHigh")) else None,
                "fifty_two_week_change": float(v) if (v := info.get("52WeekChange")) else None,
                "fifty_day_average": float(v) if (v := info.get("fiftyDayAverage")) else None,
                "two_hundred_day_average": float(v) if (v := info.get("twoHundredDayAverage")) else None,
                "ingestion_datetime": ingestion_time,
            }

            sentiment_record = {
                "ticker": ticker_symbol,
                "snapshot_timestamp": snapshot_timestamp,
                "target_high_price": float(v) if (v := info.get("targetHighPrice")) else None,
                "target_low_price": float(v) if (v := info.get("targetLowPrice")) else None,
                "target_mean_price": float(v) if (v := info.get("targetMeanPrice")) else None,
                "target_median_price": float(v) if (v := info.get("targetMedianPrice")) else None,
                "recommendation_mean": float(v) if (v := info.get("recommendationMean")) else None,
                "recommendation_key": info.get("recommendationKey"),
                "number_of_analyst_opinions": int(v) if (v := info.get("numberOfAnalystOpinions")) else None,
                "short_ratio": float(v) if (v := info.get("shortRatio")) else None,
                "shares_short": int(v) if (v := info.get("sharesShort")) else None,
                "shares_short_prior_month": int(v) if (v := info.get("sharesShortPriorMonth")) else None,
                "shares_percent_shares_out": float(v) if (v := info.get("sharesPercentSharesOut")) else None,
                "held_percent_insiders": float(v) if (v := info.get("heldPercentInsiders")) else None,
                "held_percent_institutions": float(v) if (v := info.get("heldPercentInstitutions")) else None,
                "shares_outstanding": int(v) if (v := info.get("sharesOutstanding")) else None,
                "float_shares": int(v) if (v := info.get("floatShares")) else None,
                "beta": float(v) if (v := info.get("beta")) else None,
                "beta_3year": float(v) if (v := info.get("beta3Year")) else None,
                "ingestion_datetime": ingestion_time,
            }

            return {
                "price": price_record,
                "financial": financial_record,
                "sentiment": sentiment_record,
            }

        except Exception as e:
            import time
            error_msg = str(e).lower()

            is_rate_limit = any(phrase in error_msg for phrase in [
                "too many requests", "429", "rate limit", "quota exceeded", "throttle",
            ])
            is_timeout = any(phrase in error_msg for phrase in [
                "timeout", "timed out", "connection timed out", "read timeout",
            ])

            if is_rate_limit and retry_count < max_retries:
                wait_time = 30 * (2 ** retry_count)
                logger.warning(
                    f"⏱️  Rate limit hit for {ticker_symbol}. "
                    f"Waiting {wait_time}s before retry {retry_count + 1}/{max_retries}"
                )
                time.sleep(wait_time)
                return self._fetch_single_all_info(ticker_symbol, retry_count + 1, max_retries)

            if is_timeout and retry_count < max_retries:
                wait_time = 10 * (retry_count + 1)
                logger.warning(
                    f"⏱️  Timeout for {ticker_symbol}. "
                    f"Waiting {wait_time}s before retry {retry_count + 1}/{max_retries}"
                )
                time.sleep(wait_time)
                return self._fetch_single_all_info(ticker_symbol, retry_count + 1, max_retries)

            if is_rate_limit:
                logger.error(f"❌ Rate limit exceeded for {ticker_symbol} after {max_retries} retries")
            elif is_timeout:
                logger.error(f"❌ Connection timeout for {ticker_symbol} after {max_retries} retries")
            else:
                logger.error(f"❌ Failed to fetch all info for {ticker_symbol}: {e}")

            return None

    def _fetch_single_company_static(self, ticker_symbol: str) -> pl.DataFrame:
        """Fetch company static data for a single ticker."""
        try:
            ticker = yf.Ticker(ticker_symbol)
            info = ticker.info
            effective_date = datetime.now(UTC).replace(tzinfo=None).date()

            static_data = {
                "ticker": ticker_symbol,
                "effective_date": effective_date,
                "end_date": None,
                "is_current": True,
                "symbol": info.get("symbol"),
                "short_name": info.get("shortName"),
                "long_name": info.get("longName"),
                "sector": info.get("sector"),
                "industry": info.get("industry"),
                "industry_key": info.get("industryKey"),
                "sector_key": info.get("sectorKey"),
                "country": info.get("country"),
                "state": info.get("state"),
                "city": info.get("city"),
                "address": info.get("address1"),
                "zip": info.get("zip"),
                "phone": info.get("phone"),
                "website": info.get("website"),
                "business_summary": info.get("longBusinessSummary"),
                "full_time_employees": int(v) if (v := info.get("fullTimeEmployees")) else None,
                "exchange": info.get("exchange"),
                "currency": info.get("currency"),
                "quote_type": info.get("quoteType"),
                "timezone": info.get("exchangeTimezoneName"),
                "isin": info.get("isin"),
                "uuid": info.get("uuid"),
                "first_trade_date": datetime.fromtimestamp(v, tz=UTC).date() if (v := info.get("firstTradeDateEpochUtc")) else None,
                "ingestion_datetime": datetime.now(UTC).replace(tzinfo=None),
            }
            static_data["data_hash"] = self._compute_static_hash(static_data)

            df = pl.DataFrame([static_data])
            return enforce_schema(df, self.COMPANY_SCHEMA)

        except Exception as e:
            logger.error(f"❌ Failed to fetch company static for {ticker_symbol}: {e}")
            return pl.DataFrame(schema=self.COMPANY_SCHEMA)

    def _fetch_historical_prices_batch(
        self,
        tickers: list[str],
        start_date: str | date | None = None,
        end_date: str | date | None = None,
        period: str = "1mo",
        interval: str = "1d",
        use_threads: bool = True,
    ) -> pl.DataFrame:
        """Download OHLCV data for a batch of tickers via yf.download."""
        data = yf.download(
            tickers=tickers,
            start=start_date,
            end=end_date,
            period=period if not start_date else None,
            interval=interval,
            group_by="ticker",
            auto_adjust=False,
            threads=use_threads,
            progress=False,
        )

        if len(tickers) == 1:
            ticker = tickers[0]
            if data.empty:
                logger.warning(f"⚠️ No data for {ticker}")
                return pl.DataFrame(schema=self.HISTORICAL_PRICES_SCHEMA)

            df = data.reset_index()
            df["ticker"] = ticker
            df = df.rename(columns={
                "Date": "date", "Open": "open", "High": "high", "Low": "low",
                "Close": "close", "Adj Close": "adj_close", "Volume": "volume",
            })
        else:
            if data.empty:
                logger.warning("⚠️ No data for any tickers")
                return pl.DataFrame(schema=self.HISTORICAL_PRICES_SCHEMA)

            records = []
            for ticker in tickers:
                try:
                    ticker_data = data[ticker]
                    if ticker_data.empty:
                        continue
                    ticker_df = ticker_data.reset_index()
                    ticker_df["ticker"] = ticker
                    ticker_df = ticker_df.rename(columns={
                        "Date": "date", "Open": "open", "High": "high", "Low": "low",
                        "Close": "close", "Adj Close": "adj_close", "Volume": "volume",
                    })
                    records.append(ticker_df)
                except KeyError:
                    logger.warning(f"⚠️ No data for {ticker}")
            if not records:
                return pl.DataFrame(schema=self.HISTORICAL_PRICES_SCHEMA)

            import pandas as pd
            df = pd.concat(records, ignore_index=True)

        pl_df = pl.from_pandas(df)
        pl_df = pl_df.with_columns(pl.lit(datetime.now(UTC).replace(tzinfo=None)).alias("ingestion_datetime"))
        pl_df = pl_df.select([
            "ticker", "date", "open", "high", "low", "close",
            "adj_close", "volume", "ingestion_datetime",
        ])
        return enforce_schema(pl_df, self.HISTORICAL_PRICES_SCHEMA)

    # ── Private: financial statement parsers ──────────────────────────────────

    def _parse_income_stmt(
        self,
        ticker: yf.Ticker,
        ticker_symbol: str,
        freq: str,
        ingestion_time: datetime,
    ) -> pl.DataFrame:
        """Parse income statement into INCOME_STMT_SCHEMA rows."""
        try:
            stmt = ticker.quarterly_income_stmt if freq == "quarterly" else ticker.income_stmt
            if stmt is None or stmt.empty:
                return pl.DataFrame(schema=self.INCOME_STMT_SCHEMA)

            def _get(row: str) -> float | None:
                return float(stmt.loc[row, col]) if row in stmt.index and stmt.loc[row, col] is not None else None

            records = []
            for col in stmt.columns:
                records.append({
                    "ticker": ticker_symbol,
                    "period_type": freq,
                    "period_date": col.date() if hasattr(col, "date") else None,
                    "total_revenue": _get("Total Revenue"),
                    "cost_of_revenue": _get("Cost Of Revenue"),
                    "gross_profit": _get("Gross Profit"),
                    "research_and_development": _get("Research And Development"),
                    "selling_general_administrative": _get("Selling General And Administration"),
                    "operating_income": _get("Operating Income"),
                    "interest_expense": _get("Interest Expense"),
                    "pretax_income": _get("Pretax Income"),
                    "tax_provision": _get("Tax Provision"),
                    "net_income": _get("Net Income"),
                    "ebitda": _get("EBITDA"),
                    "basic_eps": _get("Basic EPS"),
                    "diluted_eps": _get("Diluted EPS"),
                    "basic_shares_outstanding": _get("Basic Average Shares"),
                    "diluted_shares_outstanding": _get("Diluted Average Shares"),
                    "ingestion_datetime": ingestion_time,
                })

            return enforce_schema(pl.DataFrame(records), self.INCOME_STMT_SCHEMA)

        except Exception as e:
            logger.error(f"❌ Failed to parse income stmt for {ticker_symbol}: {e}")
            return pl.DataFrame(schema=self.INCOME_STMT_SCHEMA)

    def _parse_balance_sheet(
        self,
        ticker: yf.Ticker,
        ticker_symbol: str,
        freq: str,
        ingestion_time: datetime,
    ) -> pl.DataFrame:
        """Parse balance sheet into BALANCE_SHEET_SCHEMA rows."""
        try:
            stmt = ticker.quarterly_balance_sheet if freq == "quarterly" else ticker.balance_sheet
            if stmt is None or stmt.empty:
                return pl.DataFrame(schema=self.BALANCE_SHEET_SCHEMA)

            def _get(row: str) -> float | None:
                return float(stmt.loc[row, col]) if row in stmt.index and stmt.loc[row, col] is not None else None

            records = []
            for col in stmt.columns:
                records.append({
                    "ticker": ticker_symbol,
                    "period_type": freq,
                    "period_date": col.date() if hasattr(col, "date") else None,
                    "total_assets": _get("Total Assets"),
                    "current_assets": _get("Current Assets"),
                    "cash_and_equivalents": _get("Cash And Cash Equivalents"),
                    "short_term_investments": _get("Other Short Term Investments"),
                    "accounts_receivable": _get("Accounts Receivable"),
                    "inventory": _get("Inventory"),
                    "total_non_current_assets": _get("Total Non Current Assets"),
                    "net_ppe": _get("Net PPE"),
                    "goodwill": _get("Goodwill"),
                    "intangible_assets": _get("Goodwill And Other Intangible Assets"),
                    "total_liabilities": _get("Total Liabilities Net Minority Interest"),
                    "current_liabilities": _get("Current Liabilities"),
                    "accounts_payable": _get("Accounts Payable"),
                    "total_non_current_liabilities": _get("Total Non Current Liabilities Net Minority Interest"),
                    "long_term_debt": _get("Long Term Debt"),
                    "stockholders_equity": _get("Stockholders Equity"),
                    "retained_earnings": _get("Retained Earnings"),
                    "working_capital": _get("Working Capital"),
                    "total_debt": _get("Total Debt"),
                    "net_debt": _get("Net Debt"),
                    "ingestion_datetime": ingestion_time,
                })

            return enforce_schema(pl.DataFrame(records), self.BALANCE_SHEET_SCHEMA)

        except Exception as e:
            logger.error(f"❌ Failed to parse balance sheet for {ticker_symbol}: {e}")
            return pl.DataFrame(schema=self.BALANCE_SHEET_SCHEMA)

    def _parse_cash_flow(
        self,
        ticker: yf.Ticker,
        ticker_symbol: str,
        freq: str,
        ingestion_time: datetime,
    ) -> pl.DataFrame:
        """Parse cash flow statement into CASH_FLOW_SCHEMA rows."""
        try:
            stmt = ticker.quarterly_cashflow if freq == "quarterly" else ticker.cashflow
            if stmt is None or stmt.empty:
                return pl.DataFrame(schema=self.CASH_FLOW_SCHEMA)

            def _get(row: str) -> float | None:
                return float(stmt.loc[row, col]) if row in stmt.index and stmt.loc[row, col] is not None else None

            records = []
            for col in stmt.columns:
                records.append({
                    "ticker": ticker_symbol,
                    "period_type": freq,
                    "period_date": col.date() if hasattr(col, "date") else None,
                    "operating_cash_flow": _get("Operating Cash Flow"),
                    "net_income": _get("Net Income"),
                    "depreciation_amortization": _get("Depreciation And Amortization"),
                    "stock_based_compensation": _get("Stock Based Compensation"),
                    "change_in_working_capital": _get("Changes In Account Receivables"),
                    "capital_expenditures": _get("Capital Expenditure"),
                    "investing_cash_flow": _get("Investing Cash Flow"),
                    "financing_cash_flow": _get("Financing Cash Flow"),
                    "free_cash_flow": _get("Free Cash Flow"),
                    "end_cash_position": _get("End Cash Position"),
                    "ingestion_datetime": ingestion_time,
                })

            return enforce_schema(pl.DataFrame(records), self.CASH_FLOW_SCHEMA)

        except Exception as e:
            logger.error(f"❌ Failed to parse cash flow for {ticker_symbol}: {e}")
            return pl.DataFrame(schema=self.CASH_FLOW_SCHEMA)

    # ── Private: utilities ────────────────────────────────────────────────────

    @staticmethod
    def _compute_static_hash(data: dict) -> str:
        """Compute a hash of company static fields for change detection."""
        import hashlib
        import json

        relevant_fields = {
            k: v for k, v in data.items()
            if k not in ["ticker", "effective_date", "end_date", "is_current",
                         "ingestion_datetime", "data_hash"]
        }
        serialized = json.dumps(relevant_fields, sort_keys=True, default=str)
        return hashlib.md5(serialized.encode()).hexdigest()

    @staticmethod
    def _chunk_list(lst: list, chunk_size: int) -> Generator[list, None, None]:
        """Split a list into chunks of the specified size."""
        for i in range(0, len(lst), chunk_size):
            yield lst[i:i + chunk_size]
