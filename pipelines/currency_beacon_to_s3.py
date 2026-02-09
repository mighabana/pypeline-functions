import argparse

from infolio.apis.financial import CurrencyBeacon
from infolio.connectors.cloud_storage import S3
from infolio.utils.logger import get_logger

logger = get_logger(__name__)

def fetch_latest_exchange_rates(base_currencies:list[str], bucket_name:str, path_prefix:str) -> None:
    """"""
    currency_beacon = CurrencyBeacon()
    s3 = S3()

    data = currency_beacon.get_latest_rates(base_currencies=base_currencies)

    upload_file = s3.upload(data, bucket_name, path_prefix)
    logger.info(f"✅ Successfully uploaded [{','.join(base_currencies)}] data to {upload_file}")

def main() -> None:  # noqa: D103
    parser = argparse.ArgumentParser(
        description = "Extract Exchange Rate information from Currency Beacon and save to S3"
    )
    subparsers = parser.add_subparsers(dest="mode", required=True)

    # Current Mode
    current_parser = subparsers.add_parser(
        "current", help="Fetch the current latest exchange rates."
    )
    current_parser.add_argument(
        "--base_currencies",
        required=True,
        action="extend",
        nargs="+",
        choices=[
            "AED", "AFN", "ALL", "AMD", "ANG", "AOA", "ARS", "AUD", "AWG", "AZN",
            "BAM", "BBD", "BDT", "BGN", "BHD", "BIF", "BMD", "BND", "BOB", "BOV",
            "BRL", "BSD", "BTN", "BWP", "BYN", "BZD", "CAD", "CDF", "CHF", "CLF",
            "CLP", "CNY", "COP", "CRC", "CUC", "CUP", "CVE", "CZK", "DJF", "DKK",
            "DOP", "DZD", "EGP", "ERN", "ETB", "EUR", "FJD", "FKP", "GBP", "GEL",
            "GHS", "GIP", "GMD", "GNF", "GTQ", "GYD", "HKD", "HNL", "HRK", "HTG",
            "HUF", "IDR", "ILS", "INR", "IQD", "IRR", "ISK", "JMD", "JOD", "JPY",
            "KES", "KGS", "KHR", "KMF", "KPW", "KRW", "KWD", "KYD", "KZT", "LAK",
            "LBP", "LKR", "LRD", "LSL", "LTL", "LVL", "LYD", "MAD", "MDL", "MGA",
            "MKD", "MMK", "MNT", "MOP", "MRO", "MUR", "MVR", "MWK", "MXN", "MYR",
            "MZN", "NAD", "NGN", "NIO", "NOK", "NPR", "NZD", "OMR", "PAB", "PEN",
            "PGK", "PHP", "PKR", "PLN", "PYG", "QAR", "RON", "RSD", "RUB", "RWF",
            "SAR", "SBD", "SCR", "SDG", "SEK", "SGD", "SHP", "SLL", "SOS", "SRD",
            "SSP", "STD", "SVC", "SYP", "SZL", "THB", "TJS", "TMT", "TND", "TOP",
            "TRY", "TTD", "TWD", "TZS", "UAH", "UGX", "USD", "UYU", "UZS", "VEF",
            "VND", "VUV", "WST", "XAF", "XCD", "XOF", "XPF", "YER", "ZAR", "ZMW",
            "ZWL",
        ]
    )
    current_parser.add_argument(
        "--bucket_name",
        required=True,
        help="Bucket where the exchange rates will be stored."
    )
    current_parser.add_argument(
        "--path_prefix",
        required=True,
        help="Path prefix for the exchange rates extract."
    )

    args = parser.parse_args()

    if args.mode == "current":
        fetch_latest_exchange_rates(args.base_currencies, args.bucket_name, args.path_prefix)

if __name__ == "__main__":
    main()
