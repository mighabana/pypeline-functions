import argparse
from datetime import datetime, UTC

from infolio.apis.financial import YahooFinance
from infolio.connectors.cloud_storage import S3
from infolio.utils.logger import get_logger

logger = get_logger(__name__)

def fetch_latest_stock_info(tickers:list[str], bucket_name:str, path_prefix:str, batch_postfix:str) -> None:
    """"""
    yahoo_finance = YahooFinance()
    s3 = S3()

    batches = list(yahoo_finance.get_latest_info(tickers=tickers))

    timestamp = datetime.now(tz=UTC).strftime("%Y%m%dT%H%M%S")
    filename = f"{timestamp}_{batch_postfix}.parquet"

    price_gen = (prices for prices, _, _ in batches)
    financial_gen = (financials for _, financials, _ in batches)
    sentiment_gen = (sentiment for _, _, sentiment in batches)

    price_key = f"{path_prefix}/price"
    price_file = s3.upload(price_gen, bucket_name, price_key, filename=filename)
    logger.info(f"✅ Successfully uploaded [{','.join(tickers)}] price data to {price_file}")

    financial_key = f"{path_prefix}/financial"
    financial_file = s3.upload(financial_gen, bucket_name, financial_key, filename=filename)
    logger.info(f"✅ Successfully uploaded [{','.join(tickers)}] financial data to {financial_file}")

    sentiment_key = f"{path_prefix}/sentiment"
    sentiment_file = s3.upload(sentiment_gen, bucket_name, sentiment_key, filename=filename)
    logger.info(f"✅ Successfully uploaded [{','.join(tickers)}] sentiment data to {sentiment_file}")

def main() -> None: # nowa: D103
    parser = argparse.ArgumentParser(
        description = "Extract Stock information through Yahoo Finance and save to S3"
    )
    subparsers = parser.add_subparsers(dest="mode", required=True)

    # Latest Mode
    latest_parser = subparsers.add_parser(
        "latest", help="Fetch the current latest stock info."
    )
    latest_parser.add_argument(
        "--tickers",
        required=True,
        action="extend",
        nargs="+"
    )
    latest_parser.add_argument(
        "--bucket_name",
        required=True,
        help="Bucket where the stock info will be stored."
    )
    latest_parser.add_argument(
        "--path_prefix",
        required=True,
        help="Path prefix for the stock info to be extracted to."
    )
    latest_parser.add_argument(
        "--batch_postfix",
        required=True,
        help="A batch indicator added to the end of the filename."
    )

    args = parser.parse_args()

    if args.mode == "latest":
        fetch_latest_stock_info(args.tickers, args.bucket_name, args.path_prefix, args.batch_postfix)

if __name__ == "__main__":
    main()
