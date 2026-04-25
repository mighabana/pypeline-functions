import argparse
import json
import os
import tempfile
from datetime import UTC, datetime

from infolio.apis.financial import YahooFinance
from infolio.connectors.cloud_storage import S3
from infolio.utils.logger import get_logger

logger = get_logger(__name__)

AIRFLOW_XCOM_PATH = "/airflow/xcom/return.json"

def _write_xcom(data: dict) -> None:
    """Write XCom data when running in Airflow (KubernetesPodOperator sidecar), or to a temp file on local execution."""
    try:
        os.makedirs(os.path.dirname(AIRFLOW_XCOM_PATH), exist_ok=True)
        with open(AIRFLOW_XCOM_PATH, "w") as f:
            json.dump(data, f)
        logger.info(f"📋 XCom written to {AIRFLOW_XCOM_PATH}")
    except OSError:
        # Not running inside an Airflow pod — fall back to temp file for local execution
        tmp = tempfile.NamedTemporaryFile(
            mode="w",
            suffix=".json",
            prefix="xcom_",
            delete=False,
        )
        json.dump(data, tmp)
        tmp.close()
        logger.info(f"🗂  Local execution — XCom data written to {tmp.name}")
        logger.info(f"🗂  XCom contents: {data}")

def fetch_latest_stock_info(tickers:list[str], bucket_name:str, path_prefix:str, batch_postfix:str) -> None:
    """"""
    yahoo_finance = YahooFinance()
    s3 = S3()

    batches = list(yahoo_finance.get_latest_info(tickers=tickers))

    timestamp = datetime.now(tz=UTC).strftime("%Y%m%dT%H%M%S")
    filename = f"{timestamp}_{batch_postfix}.parquet"

    price_gen = (prices for prices, _, _, _ in batches)
    financial_gen = (financials for _, financials, _, _ in batches)
    sentiment_gen = (sentiment for _, _, sentiment, _ in batches)

    price_key = f"{path_prefix}/price"
    price_file = s3.upload(price_gen, bucket_name, price_key, filename=filename)
    logger.info(f"✅ Successfully uploaded [{','.join(tickers)}] price data to {price_file}")

    financial_key = f"{path_prefix}/financial"
    financial_file = s3.upload(financial_gen, bucket_name, financial_key, filename=filename)
    logger.info(f"✅ Successfully uploaded [{','.join(tickers)}] financial data to {financial_file}")

    sentiment_key = f"{path_prefix}/sentiment"
    sentiment_file = s3.upload(sentiment_gen, bucket_name, sentiment_key, filename=filename)
    logger.info(f"✅ Successfully uploaded [{','.join(tickers)}] sentiment data to {sentiment_file}")

    # Failure tracking
    failed_tickers = sorted({ticker for _, _, _, failed in batches for ticker in failed})

    if failed_tickers:
        logger.warning(f"⚠️  {len(failed_tickers)} tickers returned no data: {failed_tickers}")
    else:
        logger.info(f"✅ All {len(tickers)} tickers returned data.")

    _write_xcom({
        "batch":         batch_postfix,
        "requested":     sorted(tickers),
        "failed":        failed_tickers,
        "failure_count": len(failed_tickers),
    })

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
