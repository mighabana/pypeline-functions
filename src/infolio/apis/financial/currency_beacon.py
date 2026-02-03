import os
from collections.abc import Generator
from datetime import UTC, date, datetime, timedelta

import polars as pl

from infolio.transformations.schema import enforce_schema
from infolio.utils.api import ApiClient
from infolio.utils.auth_handlers import ApiKeyAuthHandler
from infolio.utils.logger import get_logger

logger = get_logger(__name__)

SCHEMAS = {
    "EXCHANGE_RATES": {
        "base_currency": pl.Utf8,
        "target_currency": pl.Utf8,
        "rate": pl.Float64,
        "rate_date": pl.Date,
        "rate_timestamp": pl.Datetime,
        "ingestion_datetime": pl.Datetime,
    },
    "CONVERSION": {
        "from_currency": pl.Utf8,
        "to_currency": pl.Utf8,
        "amount": pl.Float64,
        "converted_amount": pl.Float64,
        "rate": pl.Float64,
        "rate_timestamp": pl.Datetime,
        "ingestion_datetime": pl.Datetime,
    },
    "CURRENCIES": {
        "currency_code": pl.Utf8,
        "currency_name": pl.Utf8,
        "short_code": pl.Utf8,
        "symbol": pl.Utf8,
        "ingestion_datetime": pl.Datetime,
    },
}


class CurrencyBeacon:
    """
    A utility class for the Currency Beacon API.

    Provides access to real-time and historical exchange rate data for 200+ currencies.
    Data is sourced from major forex providers, central banks, and commercial vendors.

    Parameters
    ----------
    api_key : str | None
        The Currency Beacon API key. If None, will use the `API__CURRENCY_BEACON__API_KEY`
        environment variable.
    """

    def __init__(self, api_key: str | None = None) -> None:
        """
        Initialize the Currency Beacon API client.

        Parameters
        ----------
        api_key : str | None
            API key for Currency Beacon. If None, reads from environment variable
            API__CURRENCY_BEACON__API_KEY.
        """
        auth_handler = ApiKeyAuthHandler(
            key_name="Authorization",
            api_key=f"Bearer {api_key}" or f"Bearer {os.getenv('API__CURRENCY_BEACON__API_KEY')}",
        )

        self.api_client = ApiClient(
            base_url="https://api.currencybeacon.com/v1",
            headers={"Accept": "application/json"},
            timeout=10,
            max_retries=5,
            auth_handler=auth_handler,
        )
        self.api_client.reauthenticate()

    def get_latest_rates(
        self, base: str = "USD", symbols: list[str] | None = None
    ) -> pl.DataFrame:
        """
        Retrieve the latest exchange rates.

        Fetches real-time exchange rates for specified currencies. Rates are updated
        hourly and represent the mid-market rate between buy and sell prices.

        Parameters
        ----------
        base : str, default "USD"
            The base currency code (e.g., "USD", "EUR", "PHP").
        symbols : list[str] | None
            List of target currency codes to retrieve. If None, returns all available
            currencies (~200).

        Returns
        -------
        pl.DataFrame
            DataFrame with columns: base_currency, target_currency, rate, rate_date,
            rate_timestamp, ingestion_datetime.

        Raises
        ------
        requests.HTTPError
            If the API request fails.
        """
        endpoint = "latest"
        params = {"base": base}

        if symbols:
            params["symbols"] = ",".join(symbols)

        logger.info(f"📊 Fetching latest rates for base={base}, symbols={symbols or 'ALL'}")

        response = self.api_client.get(endpoint, params=params)
        data = response.json()

        # Extract response metadata
        response_meta = data.get("meta", {})
        api_timestamp = response_meta.get("last_updated_at")

        # Parse timestamp
        if api_timestamp:
            timestamp = datetime.fromisoformat(api_timestamp.replace("Z", "+00:00"))
        else:
            timestamp = datetime.now(tz=UTC)

        # Extract rates
        response_data = data.get("response", {})
        base_currency = response_data.get("base")
        rates = response_data.get("rates", {})

        # Build DataFrame
        records = []
        for target_currency, rate in rates.items():
            records.append(
                {
                    "base_currency": base_currency,
                    "target_currency": target_currency,
                    "rate": float(rate),
                    "rate_date": timestamp.date(),
                    "rate_timestamp": timestamp,
                    "ingestion_datetime": datetime.now(tz=UTC),
                }
            )

        df = pl.DataFrame(records)
        logger.info(f"✅ Retrieved {len(records)} exchange rates")

        return enforce_schema(df, SCHEMAS["EXCHANGE_RATES"])

    def get_historical_rates(
        self, date: str | date, base: str = "USD", symbols: list[str] | None = None
    ) -> pl.DataFrame:
        """
        Retrieve historical exchange rates for a specific date.

        Fetches end-of-day exchange rates for a past date. Historical data is available
        back to 1995.

        Parameters
        ----------
        date : str | date
            The date for which to retrieve rates. Can be a string in "YYYY-MM-DD" format
            or a datetime.date object.
        base : str, default "USD"
            The base currency code.
        symbols : list[str] | None
            List of target currency codes. If None, returns all available currencies.

        Returns
        -------
        pl.DataFrame
            DataFrame with columns: base_currency, target_currency, rate, rate_date,
            rate_timestamp, ingestion_datetime.

        Raises
        ------
        requests.HTTPError
            If the API request fails.
        ValueError
            If the date format is invalid.
        """
        endpoint = "historical"

        # Convert date to string if necessary
        if isinstance(date, datetime):
            date_str = date.date().isoformat()
        elif isinstance(date, str):
            date_str = date
        else:
            date_str = date.isoformat()

        params = {"date": date_str, "base": base}

        if symbols:
            params["symbols"] = ",".join(symbols)

        logger.info(f"📅 Fetching historical rates for date={date_str}, base={base}")

        response = self.api_client.get(endpoint, params=params)
        data = response.json()

        # Extract response data
        response_data = data.get("response", {})
        base_currency = response_data.get("base")
        response_date = response_data.get("date")
        rates = response_data.get("rates", {})

        # Parse date
        parsed_date = datetime.strptime(response_date, "%Y-%m-%d").date()

        # Historical rates don't have a specific timestamp, so we use noon UTC on the date
        rate_timestamp = datetime.combine(parsed_date, datetime.min.time()).replace(tzinfo=UTC)

        # Build DataFrame
        records = []
        for target_currency, rate in rates.items():
            records.append(
                {
                    "base_currency": base_currency,
                    "target_currency": target_currency,
                    "rate": float(rate),
                    "rate_date": parsed_date,
                    "rate_timestamp": rate_timestamp,
                    "ingestion_datetime": datetime.now(tz=UTC),
                }
            )

        df = pl.DataFrame(records)
        logger.info(f"✅ Retrieved {len(records)} historical rates for {date_str}")

        return enforce_schema(df, SCHEMAS["EXCHANGE_RATES"])

    def get_timeseries_rates(
        self,
        start_date: str | date,
        end_date: str | date,
        base: str = "USD",
        symbols: list[str] | None = None,
        batch_size: int = 7,
    ) -> Generator[pl.DataFrame, None, None]:
        """
        Retrieve exchange rates over a date range.

        Fetches historical rates for multiple dates in batches. This is useful for
        backfilling historical data or analyzing trends over time.

        Parameters
        ----------
        start_date : str | date
            The start date for the time series.
        end_date : str | date
            The end date for the time series.
        base : str, default "USD"
            The base currency code.
        symbols : list[str] | None
            List of target currency codes.
        batch_size : int, default 7
            Number of days to fetch per API call. Smaller batches reduce API load
            but increase total requests.

        Yields
        ------
        pl.DataFrame
            DataFrames containing rates for each batch of dates.

        """
        # Convert dates to date objects
        if isinstance(start_date, str):
            start_date = datetime.strptime(start_date, "%Y-%m-%d").date()
        if isinstance(end_date, str):
            end_date = datetime.strptime(end_date, "%Y-%m-%d").date()

        total_days = (end_date - start_date).days + 1
        logger.info(
            f"📈 Fetching time series data for {total_days} days "
            f"({start_date} to {end_date})"
        )

        current_date = start_date
        batch_count = 0

        while current_date <= end_date:
            batch_dfs = []
            batch_end = min(current_date + timedelta(days=batch_size - 1), end_date)

            # Fetch each day in the batch
            date_cursor = current_date
            while date_cursor <= batch_end:
                try:
                    df = self.get_historical_rates(
                        date=date_cursor, base=base, symbols=symbols
                    )
                    batch_dfs.append(df)
                except Exception as e:
                    logger.error(f"❌ Failed to fetch rates for {date_cursor}: {e}")

                date_cursor += timedelta(days=1)

            # Combine batch
            if batch_dfs:
                batch_count += 1
                combined_df = pl.concat(batch_dfs)
                logger.info(
                    f"📦 Batch #{batch_count}: {len(batch_dfs)} days, "
                    f"{combined_df.height} total rows"
                )
                yield combined_df

            current_date = batch_end + timedelta(days=1)

        logger.info(f"✅ Completed time series fetch: {batch_count} batches")

    def convert_currency(
        self,
        from_currency: str,
        to_currency: str,
        amount: float,
    ) -> pl.DataFrame:
        """
        Convert an amount from one currency to another.

        Performs real-time currency conversion using the latest exchange rates.

        Parameters
        ----------
        from_currency : str
            The source currency code (e.g., "USD").
        to_currency : str
            The target currency code (e.g., "EUR").
        amount : float
            The amount to convert.

        Returns
        -------
        pl.DataFrame
            DataFrame with columns: from_currency, to_currency, amount, converted_amount,
            rate, rate_timestamp, ingestion_datetime.

        Raises
        ------
        requests.HTTPError
            If the API request fails.
        """
        endpoint = "convert"
        params = {
            "from": from_currency,
            "to": to_currency,
            "amount": amount,
        }

        logger.info(
            f"💱 Converting {amount} {from_currency} to {to_currency}"
        )

        response = self.api_client.get(endpoint, params=params)
        data = response.json()

        # Extract response metadata
        response_meta = data.get("meta", {})
        api_timestamp = response_meta.get("last_updated_at")

        # Parse timestamp
        if api_timestamp:
            timestamp = datetime.fromisoformat(api_timestamp.replace("Z", "+00:00"))
        else:
            timestamp = datetime.now(tz=UTC)

        # Extract conversion data
        response_data = data.get("response", {})
        converted_amount = response_data.get("value")

        # Calculate rate (converted_amount / amount)
        rate = converted_amount / amount if amount != 0 else 0

        df = pl.DataFrame(
            {
                "from_currency": [from_currency],
                "to_currency": [to_currency],
                "amount": [float(amount)],
                "converted_amount": [float(converted_amount)],
                "rate": [rate],
                "rate_timestamp": [timestamp],
                "ingestion_datetime": [datetime.now(tz=UTC)],
            }
        )

        logger.info(
            f"✅ Converted {amount} {from_currency} = "
            f"{converted_amount:.4f} {to_currency} (rate: {rate:.6f})"
        )

        return enforce_schema(df, SCHEMAS["CONVERSION"])

    def get_currencies(self, currency_type: str = "fiat") -> pl.DataFrame:
        """
        Retrieve a list of all supported currencies.

        Fetches metadata for available currencies including codes, names, and symbols.

        Parameters
        ----------
        currency_type : str, default "fiat"
            The type of currencies to retrieve. Options:
            - "fiat": Traditional currencies (USD, EUR, etc.)
            - "crypto": Cryptocurrencies (BTC, ETH, etc.)

        Returns
        -------
        pl.DataFrame
            DataFrame with columns: currency_code, currency_name, short_code, symbol,
            ingestion_datetime.

        Raises
        ------
        requests.HTTPError
            If the API request fails.
        ValueError
            If an invalid currency_type is provided.
        """
        endpoint = "currencies"
        params = {"type": currency_type}

        logger.info(f"🌍 Fetching {currency_type} currency list")

        response = self.api_client.get(endpoint, params=params)
        data = response.json()

        # Extract currencies
        response_data = data.get("response", [])

        records = []
        for currency in response_data:
            records.append(
                {
                    "currency_code": currency.get("id"),
                    "currency_name": currency.get("name"),
                    "short_code": currency.get("short_code"),
                    "symbol": currency.get("symbol"),
                    "ingestion_datetime": datetime.now(tz=UTC),
                }
            )

        df = pl.DataFrame(records)
        logger.info(f"✅ Retrieved {len(records)} {currency_type} currencies")

        return enforce_schema(df, SCHEMAS["CURRENCIES"])

    def get_rate_for_pair(
        self, base: str, target: str, date: str | date | None = None
    ) -> float:
        """
        Retrieve the exchange rate for a specific currency pair.

        Convenience method to get a single exchange rate value.

        Parameters
        ----------
        base : str
            The base currency code.
        target : str
            The target currency code.
        date : str | date | None
            Optional date for historical rate. If None, returns latest rate.

        Returns
        -------
        float
            The exchange rate.
        """
        if date:
            df = self.get_historical_rates(date=date, base=base, symbols=[target])
        else:
            df = self.get_latest_rates(base=base, symbols=[target])

        if df.height == 0:
            raise ValueError(f"No rate found for {base}/{target}")

        return df.select("rate").row(0)[0]
