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

    Provides access to stock prices, company information, dividends, and splits
    for global stock markets. Uses the yfinance library which accesses Yahoo's
    publicly available financial data.

    Notes
    -----
    - Data is free but intended for personal use only per Yahoo's TOS
    - No API key required
    - Supports bulk downloads with multithreading
    - Historical data available back to the stock's listing date
    """

    PRICE_SCHEMA = pl.Schema({
        "ticker": pl.Utf8,
        "snapshot_timestamp": pl.Datetime,
        # Current pricing
        "current_price": pl.Float64,
        "previous_close": pl.Float64,
        "open": pl.Float64,
        "day_low": pl.Float64,
        "day_high": pl.Float64,
        # Volume
        "volume": pl.Int64,
        "average_volume": pl.Int64,
        "average_volume_10day": pl.Int64,
        # Bid/Ask
        "bid": pl.Float64,
        "ask": pl.Float64,
        "bid_size": pl.Int64,
        "ask_size": pl.Int64,
        # Pre/Post market
        "pre_market_price": pl.Float64,
        "post_market_price": pl.Float64,
        "pre_market_change": pl.Float64,
        "post_market_change": pl.Float64,
        "ingestion_datetime": pl.Datetime,
    })

    FINANCIALS_SCHEMA = pl.Schema({
        "ticker": pl.Utf8,
        "snapshot_timestamp": pl.Datetime,
        # Market metrics
        "market_cap": pl.Float64,
        # Valuation
        "trailing_pe": pl.Float64,
        "forward_pe": pl.Float64,
        "peg_ratio": pl.Float64,
        "price_to_book": pl.Float64,
        "price_to_sales": pl.Float64,
        "enterprise_value": pl.Float64,
        "enterprise_to_revenue": pl.Float64,
        "enterprise_to_ebitda": pl.Float64,
        # Profitability
        "profit_margins": pl.Float64,
        "gross_margins": pl.Float64,
        "operating_margins": pl.Float64,
        "ebitda_margins": pl.Float64,
        "return_on_assets": pl.Float64,
        "return_on_equity": pl.Float64,
        # Growth
        "revenue_growth": pl.Float64,
        "earnings_growth": pl.Float64,
        "earnings_quarterly_growth": pl.Float64,
        # Per-share data
        "trailing_eps": pl.Float64,
        "forward_eps": pl.Float64,
        "revenue_per_share": pl.Float64,
        "book_value": pl.Float64,
        "total_cash_per_share": pl.Float64,
        # Dividends
        "dividend_rate": pl.Float64,
        "dividend_yield": pl.Float64,
        "payout_ratio": pl.Float64,
        "five_year_avg_dividend_yield": pl.Float64,
        "dividend_date": pl.Date,
        "ex_dividend_date": pl.Date,
        # Financial health
        "total_cash": pl.Float64,
        "total_debt": pl.Float64,
        "debt_to_equity": pl.Float64,
        "current_ratio": pl.Float64,
        "quick_ratio": pl.Float64,
        "free_cashflow": pl.Float64,
        "operating_cashflow": pl.Float64,
        # Revenue & Earnings
        "total_revenue": pl.Float64,
        "gross_profits": pl.Float64,
        "ebitda": pl.Float64,
        "net_income": pl.Float64,
        # 52-week range
        "fifty_two_week_low": pl.Float64,
        "fifty_two_week_high": pl.Float64,
        "fifty_two_week_change": pl.Float64,
        "fifty_day_average": pl.Float64,
        "two_hundred_day_average": pl.Float64,
        "ingestion_datetime": pl.Datetime,
    })

    SENTIMENT_SCHEMA = pl.Schema({
        "ticker": pl.Utf8,
        "snapshot_timestamp": pl.Datetime,
        # Analyst targets
        "target_high_price": pl.Float64,
        "target_low_price": pl.Float64,
        "target_mean_price": pl.Float64,
        "target_median_price": pl.Float64,
        # Recommendations
        "recommendation_mean": pl.Float64,
        "recommendation_key": pl.Utf8,
        "number_of_analyst_opinions": pl.Int64,
        # Short interest
        "short_ratio": pl.Float64,
        "shares_short": pl.Int64,
        "shares_short_prior_month": pl.Int64,
        "shares_percent_shares_out": pl.Float64,
        # Ownership
        "held_percent_insiders": pl.Float64,
        "held_percent_institutions": pl.Float64,
        # Share structure
        "shares_outstanding": pl.Int64,
        "float_shares": pl.Int64,
        # Risk
        "beta": pl.Float64,
        "beta_3year": pl.Float64,
        "ingestion_datetime": pl.Datetime,
    })

    COMPANY_SCHEMA = pl.Schema({
        "ticker": pl.Utf8,
        "effective_date": pl.Date,
        "end_date": pl.Date,
        "is_current": pl.Boolean,
        # Basic info
        "symbol": pl.Utf8,
        "short_name": pl.Utf8,
        "long_name": pl.Utf8,
        "sector": pl.Utf8,
        "industry": pl.Utf8,
        "industry_key": pl.Utf8,
        "sector_key": pl.Utf8,
        # Location
        "country": pl.Utf8,
        "state": pl.Utf8,
        "city": pl.Utf8,
        "address": pl.Utf8,
        "zip": pl.Utf8,
        "phone": pl.Utf8,
        "website": pl.Utf8,
        # Business
        "business_summary": pl.Utf8,
        "full_time_employees": pl.Int64,
        # Exchange info
        "exchange": pl.Utf8,
        "currency": pl.Utf8,
        "quote_type": pl.Utf8,
        "timezone": pl.Utf8,
        # Identifiers
        "isin": pl.Utf8,
        "uuid": pl.Utf8,
        "first_trade_date": pl.Date,
        "ingestion_datetime": pl.Datetime,
        "data_hash": pl.Utf8,
    })

    COMPANY_CHANGES_SCHEMA = pl.Schema({
        "ticker": pl.Utf8,
        "change_date": pl.Date,
        "field_name": pl.Utf8,
        "old_value": pl.Utf8,
        "new_value": pl.Utf8,
        "ingestion_datetime": pl.Datetime,
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
        "ingestion_datetime": pl.Datetime,
    })

    DIVIDENDS_SCHEMA = pl.Schema({
        "ticker": pl.Utf8,
        "date": pl.Date,
        "dividend": pl.Float64,
        "ingestion_datetime": pl.Datetime,
    })

    SPLITS_SCHEMA = pl.Schema({
        "ticker": pl.Utf8,
        "date": pl.Date,
        "split_ratio": pl.Float64,
        "ingestion_datetime": pl.Datetime,
    })

    def __init__(self) -> None:
        """Initialize the Yahoo Finance client."""
        logger.info("🔧 Initialized Yahoo Finance client")

    def get_latest_info(
        self,
        tickers: list[str],
        use_threads: bool = True,
        batch_size: int = 100,
    ) -> Generator[dict, None, None]:
        """
        Retrieve all latest data (prices, financials, sentiment) from a single API call.

        This is the most efficient method for collecting all high-frequency data. It calls
        the yfinance .info method once per ticker and extracts data for all three tables:
        price_snapshots, financials_timeseries, and market_sentiment_timeseries.

        Yields DataFrames as each batch completes, allowing for streaming to S3 or database.

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
        dict
            Dictionary with keys: 'price_snapshots', 'financials_timeseries', 
            'market_sentiment_timeseries', each containing a pl.DataFrame for the batch.
        """
        logger.info(f"📊 Fetching all latest info for {len(tickers)} tickers (batch_size={batch_size})")

        total_batches = (len(tickers) + batch_size - 1) // batch_size

        for batch_num, batch in enumerate(self._chunk_list(tickers, batch_size), 1):
            logger.info(f"  Processing batch {batch_num}/{total_batches} ({len(batch)} tickers)")

            prices, financials, sentiment, failed = self._fetch_batch_all_info(batch, use_threads)

            logger.info(
                f"  ✅ Batch {batch_num} complete: "
                f"{prices.height} prices, "
                f"{financials.height} financials, "
                f"{sentiment.height} sentiment,"
                f"{len(failed)} failed"
            )

            yield (prices, financials, sentiment, failed)

        logger.info(f"✅ Completed all {total_batches} batches")

    def _fetch_batch_all_info(
        self, tickers: list[str], use_threads: bool
    ) -> tuple[pl.DataFrame, pl.DataFrame, pl.DataFrame, list]:
        """
        Fetch all info for a batch of tickers from single .info call per ticker.

        Returns
        -------
        tuple[pl.DataFrame, pl.DataFrame, pl.DataFrame, list]
            Tuple of (price_snapshots, financials_timeseries, market_sentiment_timeseries)
            DataFrames a list of failed stock tickers.
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

        # Retry failed tickers
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

        # Convert to DataFrames and return as tuple
        prices_df = enforce_schema(
            pl.DataFrame(price_records) if price_records else pl.DataFrame(),
            self.PRICE_SCHEMA
        )
        financials_df = enforce_schema(
            pl.DataFrame(financial_records) if financial_records else pl.DataFrame(),
            self.FINANCIALS_SCHEMA
        )
        sentiment_df = enforce_schema(
            pl.DataFrame(sentiment_records) if sentiment_records else pl.DataFrame(),
            self.SENTIMENT_SCHEMA
        )

        failed_ticker_list = sorted(
            set(tickers) - {r["ticker"] for r in price_records}
        )

        return prices_df, financials_df, sentiment_df, failed_ticker_list

    def _fetch_single_all_info(self, ticker_symbol: str, retry_count: int = 0, max_retries: int = 3) -> dict | None:
        """
        Fetch all info for a single ticker from one .info call.

        Parameters
        ----------
        ticker_symbol : str
            Stock ticker symbol.
        retry_count : int, default 0
            Current retry attempt (for internal use).
        max_retries : int, default 3
            Maximum number of retries for rate limit errors.

        Returns
        -------
        dict | None
            Dictionary with 'price', 'financial', 'sentiment' record dicts,
            or None if fetch failed.
        """
        import time
        time.sleep(0.1)

        try:
            ticker = yf.Ticker(ticker_symbol)
            info = ticker.info

            if not info.get("regularMarketPrice") and not info.get("currentPrice"):
                logger.warning(f"  ⚠️ No valid data for {ticker_symbol} — possibly delisted or invalid")
                return None

            # Try fast_info for price data
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

            # Get timestamp
            timestamp = info.get("regularMarketTime")
            snapshot_timestamp = (
                datetime.fromtimestamp(timestamp, tz=UTC)
                if timestamp
                else datetime.now(tz=UTC)
            )
            ingestion_time = datetime.now(tz=UTC)

            # Build price record
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

            # Build financial record
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

            # Build sentiment record
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
            error_msg = str(e).lower()

            # Check if it's a rate limit error
            is_rate_limit = any(phrase in error_msg for phrase in [
                "too many requests",
                "429",
                "rate limit",
                "quota exceeded",
                "throttle",
            ])

            # Check if it's a timeout error
            is_timeout = any(phrase in error_msg for phrase in [
                "timeout",
                "timed out",
                "connection timed out",
                "read timeout",
            ])

            if is_rate_limit and retry_count < max_retries:
                # Exponential backoff: 2^retry_count * base_delay
                # Retry 1: 30 seconds, Retry 2: 60 seconds, Retry 3: 120 seconds
                base_delay = 30
                wait_time = base_delay * (2 ** retry_count)

                logger.warning(
                    f"⏱️  Rate limit hit for {ticker_symbol}. "
                    f"Waiting {wait_time}s before retry {retry_count + 1}/{max_retries}"
                )
                time.sleep(wait_time)

                # Recursive retry
                return self._fetch_single_all_info(ticker_symbol, retry_count + 1, max_retries)

            # Retry on timeouts with shorter wait
            if is_timeout and retry_count < max_retries:
                wait_time = 10 * (retry_count + 1)  # 10s, 20s, 30s
                logger.warning(
                    f"⏱️  Timeout for {ticker_symbol}. "
                    f"Waiting {wait_time}s before retry {retry_count + 1}/{max_retries}"
                )
                time.sleep(wait_time)

                # Recursive retry
                return self._fetch_single_all_info(ticker_symbol, retry_count + 1, max_retries)

            # Log the error
            if is_rate_limit:
                logger.error(f"❌ Rate limit exceeded for {ticker_symbol} after {max_retries} retries")
            elif is_timeout:
                logger.error(f"❌ Connection timeout for {ticker_symbol} after {max_retries} retries")
            else:
                logger.error(f"❌ Failed to fetch all info for {ticker_symbol}: {e}")

            return None

    def get_company_static(
        self,
        tickers: list[str],
        use_threads: bool = True,
        batch_size: int = 100,
    ) -> pl.DataFrame:
        """
        Retrieve company static information (SCD Type 2 format).

        Fetches company information that rarely changes: name, sector, industry,
        location, business description, employees, exchange info, etc. Returns data
        in Slowly Changing Dimension Type 2 format with effective_date, end_date,
        and is_current fields for versioning.

        Parameters
        ----------
        tickers : list[str]
            List of ticker symbols.
        use_threads : bool, default True
            Enable multithreading.
        batch_size : int, default 100
            Number of tickers per batch.

        Returns
        -------
        pl.DataFrame
            DataFrame with schema COMPANY_STATIC (SCD Type 2).

        Examples
        --------
        >>> yf = YahooFinance()
        >>> static = yf.get_company_static(["AAPL"])
        >>> # Check for changes
        >>> if yf.detect_static_changes("AAPL", previous_hash):
        ...     # Update table
        ...     pass
        """
        logger.info(f"🏢 Fetching company static data for {len(tickers)} tickers")

        if len(tickers) == 1:
            return self._fetch_single_company_static(tickers[0])

        return self._fetch_batch_company_static(tickers, use_threads, batch_size)

    def _fetch_single_company_static(self, ticker_symbol: str) -> pl.DataFrame:
        """Fetch company static data for a single ticker."""
        try:
            ticker = yf.Ticker(ticker_symbol)
            info = ticker.info

            effective_date = datetime.now(tz=UTC).date()

            # Extract static fields
            static_data = {
                "ticker": ticker_symbol,
                "effective_date": effective_date,
                "end_date": None,  # NULL for current version
                "is_current": True,
                # Basic info
                "symbol": info.get("symbol"),
                "short_name": info.get("shortName"),
                "long_name": info.get("longName"),
                "sector": info.get("sector"),
                "industry": info.get("industry"),
                "industry_key": info.get("industryKey"),
                "sector_key": info.get("sectorKey"),
                # Location
                "country": info.get("country"),
                "state": info.get("state"),
                "city": info.get("city"),
                "address": info.get("address1"),
                "zip": info.get("zip"),
                "phone": info.get("phone"),
                "website": info.get("website"),
                # Business
                "business_summary": info.get("longBusinessSummary"),
                "full_time_employees": int(v) if (v := info.get("fullTimeEmployees")) else None,
                # Exchange
                "exchange": info.get("exchange"),
                "currency": info.get("currency"),
                "quote_type": info.get("quoteType"),
                "timezone": info.get("exchangeTimezoneName"),
                # Identifiers
                "isin": info.get("isin"),
                "uuid": info.get("uuid"),
                "first_trade_date": datetime.fromtimestamp(v, tz=UTC).date() if (v := info.get("firstTradeDateEpochUtc")) else None,
                "ingestion_datetime": datetime.now(tz=UTC),
            }

            # Compute hash for change detection
            static_data["data_hash"] = self._compute_static_hash(static_data)

            df = pl.DataFrame([static_data])
            return enforce_schema(df, self.COMPANY_SCHEMA)

        except Exception as e:
            logger.error(f"❌ Failed to fetch company static for {ticker_symbol}: {e}")
            return pl.DataFrame(schema=self.COMPANY_SCHEMA)

    def _fetch_batch_company_static(
        self, tickers: list[str], use_threads: bool, batch_size: int
    ) -> pl.DataFrame:
        """Fetch company static for multiple tickers."""
        all_data = []
        failed_tickers = []

        for batch in self._chunk_list(tickers, batch_size):
            try:
                logger.info(f"  Processing batch of {len(batch)} tickers")
                batch_data = []

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
                                    batch_data.append(result)
                            except Exception as e:
                                logger.warning(f"  Failed {ticker}: {e}")
                                failed_tickers.append(ticker)
                else:
                    for ticker in batch:
                        result = self._fetch_single_company_static(ticker)
                        if result.height > 0:
                            batch_data.append(result)
                        else:
                            failed_tickers.append(ticker)

                if batch_data:
                    all_data.extend(batch_data)

            except Exception as e:
                logger.error(f"❌ Batch failed: {e}")
                failed_tickers.extend(batch)

        if failed_tickers:
            logger.info(f"🔄 Retrying {len(failed_tickers)} failed tickers")
            for ticker in failed_tickers:
                result = self._fetch_single_company_static(ticker)
                if result.height > 0:
                    all_data.append(result)

        if not all_data:
            return pl.DataFrame(schema=self.COMPANY_SCHEMA)

        combined = pl.concat(all_data)
        logger.info(f"✅ Retrieved company static for {combined.height} tickers")
        return combined

    def detect_static_changes(
        self, current_data: pl.DataFrame, previous_data: pl.DataFrame
    ) -> pl.DataFrame:
        """
        Detect changes between current and previous company static data.

        Compares two company_static DataFrames and returns a DataFrame of changes
        in the format of COMPANY_STATIC_CHANGES schema.

        Parameters
        ----------
        current_data : pl.DataFrame
            Current company static data (from get_company_static).
        previous_data : pl.DataFrame
            Previous company static data (from database).

        Returns
        -------
        pl.DataFrame
            DataFrame with schema COMPANY_STATIC_CHANGES containing field-level changes.
            Empty DataFrame if no changes detected.
        """
        if previous_data.height == 0:
            logger.info("No previous data - first load")
            return pl.DataFrame(schema=self.COMPANY_CHANGES_SCHEMA)

        changes = []
        change_date = datetime.now(tz=UTC).date()

        # Fields to monitor for changes (excluding metadata fields)
        fields_to_monitor = [
            "symbol", "short_name", "long_name", "sector", "industry",
            "industry_key", "sector_key", "country", "state", "city",
            "address", "zip", "phone", "website", "business_summary",
            "full_time_employees", "exchange", "currency", "quote_type",
            "timezone", "isin"
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

                    # Compare values (handle None/null)
                    if current_val != previous_val:
                        changes.append({
                            "ticker": ticker,
                            "change_date": change_date,
                            "field_name": field,
                            "old_value": str(previous_val) if previous_val is not None else None,
                            "new_value": str(current_val) if current_val is not None else None,
                            "ingestion_datetime": datetime.now(tz=UTC),
                        })
                except Exception as e:
                    logger.warning(f"Error comparing {field} for {ticker}: {e}")

        if changes:
            logger.info(f"🔍 Detected {len(changes)} field changes")
            df = pl.DataFrame(changes)
            return enforce_schema(df, self.COMPANY_CHANGES_SCHEMA)
        else:
            logger.info("✅ No changes detected")
            return pl.DataFrame(schema=self.COMPANY_CHANGES_SCHEMA)

    @staticmethod
    def _compute_static_hash(data: dict) -> str:
        """Compute hash of static data for change detection."""
        import hashlib
        import json

        # Extract relevant fields (exclude metadata)
        relevant_fields = {
            k: v for k, v in data.items()
            if k not in ["ticker", "effective_date", "end_date", "is_current",
                        "ingestion_datetime", "data_hash"]
        }

        # Sort and serialize
        serialized = json.dumps(relevant_fields, sort_keys=True, default=str)
        return hashlib.md5(serialized.encode()).hexdigest()

    @staticmethod
    def _chunk_list(lst: list, chunk_size: int) -> Generator[list]:
        """Split list into chunks of specified size."""
        for i in range(0, len(lst), chunk_size):
            yield lst[i:i + chunk_size]

    def get_historical_prices(
        self,
        tickers: list[str],
        start_date: str | date | None = None,
        end_date: str | date | None = None,
        period: str = "1mo",
        interval: str = "1d",
        use_threads: bool = True,
    ) -> pl.DataFrame:
        """
        Retrieve historical OHLCV price data.

        Downloads traditional OHLCV (Open, High, Low, Close, Volume) data with optional
        date range or period specification.

        Parameters
        ----------
        tickers : list[str]
            List of ticker symbols.
        start_date : str | date | None
            Start date (YYYY-MM-DD or date object). If None, uses period.
        end_date : str | date | None
            End date. If None, uses today.
        period : str, default "1mo"
            Period if start_date is None. Valid: 1d, 5d, 1mo, 3mo, 6mo, 1y, 2y, 5y, 10y, ytd, max.
        interval : str, default "1d"
            Data interval. Valid: 1m, 2m, 5m, 15m, 30m, 60m, 90m, 1h, 1d, 5d, 1wk, 1mo, 3mo.
        use_threads : bool, default True
            Enable multithreading for faster downloads.

        Returns
        -------
        pl.DataFrame
            DataFrame with schema HISTORICAL_PRICES (OHLCV format).
        """
        logger.info(
            f"📈 Fetching historical prices for {len(tickers)} tickers "
            f"(period={period if not start_date else f'{start_date} to {end_date}'}, interval={interval})"
        )

        # Download using yfinance bulk download
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

        # Handle single vs multiple tickers
        if len(tickers) == 1:
            ticker = tickers[0]
            if data.empty:
                logger.warning(f"⚠️ No data for {ticker}")
                return pl.DataFrame(schema=self.HISTORICAL_PRICES_SCHEMA)

            df = data.reset_index()
            df["ticker"] = ticker
            df = df.rename(columns={
                "Date": "date",
                "Open": "open",
                "High": "high",
                "Low": "low",
                "Close": "close",
                "Adj Close": "adj_close",
                "Volume": "volume",
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
                        "Date": "date",
                        "Open": "open",
                        "High": "high",
                        "Low": "low",
                        "Close": "close",
                        "Adj Close": "adj_close",
                        "Volume": "volume",
                    })
                    records.append(ticker_df)
                except KeyError:
                    logger.warning(f"⚠️ No data for {ticker}")
                    continue

            if not records:
                return pl.DataFrame(schema=self.HISTORICAL_PRICES_SCHEMA)

            import pandas as pd
            df = pd.concat(records, ignore_index=True)

        # Convert to polars
        pl_df = pl.from_pandas(df)
        pl_df = pl_df.with_columns(pl.lit(datetime.now(tz=UTC)).alias("ingestion_datetime"))

        # Select columns
        pl_df = pl_df.select([
            "ticker", "date", "open", "high", "low", "close",
            "adj_close", "volume", "ingestion_datetime"
        ])

        logger.info(f"✅ Retrieved {pl_df.height:,} historical records")
        return enforce_schema(pl_df, self.HISTORICAL_PRICES_SCHEMA)

    def get_dividends(self, tickers: list[str]) -> pl.DataFrame:
        """
        Retrieve dividend history for multiple tickers.

        Fetches all historical dividend payments for the specified stocks.

        Parameters
        ----------
        tickers : list[str]
            List of ticker symbols.

        Returns
        -------
        pl.DataFrame
            DataFrame with columns: ticker, date, dividend, ingestion_datetime.
        """
        logger.info(f"💰 Fetching dividend history for {len(tickers)} tickers")

        records = []
        for ticker_symbol in tickers:
            try:
                ticker = yf.Ticker(ticker_symbol)
                dividends = ticker.dividends

                if dividends.empty:
                    logger.debug(f"ℹ️ No dividends for {ticker_symbol}")
                    continue

                for date, dividend in dividends.items():
                    records.append(
                        {
                            "ticker": ticker_symbol,
                            "date": date.date(),
                            "dividend": float(dividend),
                            "ingestion_datetime": datetime.now(tz=UTC),
                        }
                    )

            except Exception as e:
                logger.error(f"❌ Failed to fetch dividends for {ticker_symbol}: {e}")
                continue

        df = pl.DataFrame(records)
        logger.info(f"✅ Retrieved {len(records)} dividend records")

        return enforce_schema(df, self.DIVIDENDS_SCHEMA)

    def get_splits(self, tickers: list[str]) -> pl.DataFrame:
        """
        Retrieve stock split history for multiple tickers.

        Fetches all historical stock splits for the specified stocks.

        Parameters
        ----------
        tickers : list[str]
            List of ticker symbols.

        Returns
        -------
        pl.DataFrame
            DataFrame with columns: ticker, date, split_ratio, ingestion_datetime.
        """
        logger.info(f"🔀 Fetching split history for {len(tickers)} tickers")

        records = []
        for ticker_symbol in tickers:
            try:
                ticker = yf.Ticker(ticker_symbol)
                splits = ticker.splits

                if splits.empty:
                    logger.debug(f"ℹ️ No splits for {ticker_symbol}")
                    continue

                for date, split_ratio in splits.items():
                    records.append(
                        {
                            "ticker": ticker_symbol,
                            "date": date.date(),
                            "split_ratio": float(split_ratio),
                            "ingestion_datetime": datetime.now(tz=UTC),
                        }
                    )

            except Exception as e:
                logger.error(f"❌ Failed to fetch splits for {ticker_symbol}: {e}")
                continue

        df = pl.DataFrame(records)
        logger.info(f"✅ Retrieved {len(records)} split records")

        return enforce_schema(df, self.SPLITS_SCHEMA)

    def get_timeseries_prices(
        self,
        tickers: list[str],
        start_date: str | date,
        end_date: str | date,
        interval: str = "1d",
        batch_size: int = 100,
    ) -> Generator[pl.DataFrame, None, None]:
        """
        Retrieve historical prices in batches for large ticker lists.

        Useful for processing large numbers of tickers without overwhelming memory
        or hitting rate limits. Yields batches of data as they're downloaded.

        Parameters
        ----------
        tickers : list[str]
            List of ticker symbols.
        start_date : str | date
            Start date for historical data.
        end_date : str | date
            End date for historical data.
        interval : str, default "1d"
            Data interval (1d, 1wk, 1mo, etc.).
        batch_size : int, default 100
            Number of tickers to process per batch.

        Yields
        ------
        pl.DataFrame
            DataFrames containing price data for each batch of tickers.
        """
        total_tickers = len(tickers)
        logger.info(
            f"📦 Fetching time series data for {total_tickers} tickers "
            f"in batches of {batch_size}"
        )

        for i in range(0, total_tickers, batch_size):
            batch_tickers = tickers[i : i + batch_size]
            batch_num = (i // batch_size) + 1
            total_batches = (total_tickers + batch_size - 1) // batch_size

            logger.info(
                f"📊 Processing batch {batch_num}/{total_batches} "
                f"({len(batch_tickers)} tickers)"
            )

            try:
                df = self.get_historical_prices(
                    tickers=batch_tickers,
                    start_date=start_date,
                    end_date=end_date,
                    interval=interval,
                    use_threads=True,
                )

                logger.info(f"✅ Batch {batch_num}: {df.height:,} records")
                yield df

            except Exception as e:
                logger.error(f"❌ Failed to process batch {batch_num}: {e}")
                continue

        logger.info(f"✅ Completed time series fetch: {total_batches} batches")
