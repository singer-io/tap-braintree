#!/usr/bin/env python3

import os
from datetime import datetime, timedelta

import braintree
import pytz

import singer
from singer import utils

from .transform import transform_row

CONFIG = {}
STATE = {}
TRAILING_DAYS = timedelta(days=2)
DEFAULT_TIMESTAMP = "1970-01-01T00:00:00Z"

logger = singer.get_logger()


def get_abs_path(path):
    return os.path.join(os.path.dirname(os.path.realpath(__file__)), path)


def load_schema(entity):
    return utils.load_json(get_abs_path("schemas/{}.json".format(entity)))


def get_start(entity):
    if entity not in STATE:
        STATE[entity] = CONFIG["start_date"]

    return STATE[entity]


def to_utc(dt):
    return dt.replace(tzinfo=pytz.UTC)


def daterange(start_date, end_date):
    """
    Generator function that produces an iterable list of days between the two
    dates start_date and end_date as a tuple pair of datetimes.

    Note:
        All times are set to 0:00. Designed to be used in date query where query
        logic would be record_date >= 2019-01-01 0:00 and record_date < 2019-01-02 0:00

    Args:
        start_date (datetime): start of period
        end_date (datetime): end of period

    Yields:
        tuple: daily period
            * datetime: day within range
            * datetime: day within range + 1 day

    """

    # set to start of day
    start_date = to_utc(
        datetime.combine(
            start_date.date(),
            datetime.min.time(),  # set to the 0:00 on the day of the start date
        )
    )

    end_date = to_utc(end_date + timedelta(1))

    for n in range(int((end_date - start_date).days)):
        yield start_date + timedelta(n), start_date + timedelta(n + 1)


def sync_merchant_accounts(gateway):
    schema = load_schema("merchant_accounts")

    singer.write_schema("merchant_accounts", schema, ["id"])

    data = gateway.merchant_account.all()

    time_extracted = utils.now()

    row_written_count = 0

    for merchant_account in data.merchant_accounts:
        transformed = transform_row(merchant_account, schema)
        singer.write_record(
            "merchant_accounts", transformed, time_extracted=time_extracted
        )
        row_written_count += 1

    if row_written_count > 0:
        logger.info("merchant_accounts: Written {} records".format(row_written_count))

    return data.merchant_accounts


def sync_transactions(merchant_accounts, gateway):
    schema = load_schema("transactions")

    singer.write_schema(
        "transactions", schema, ["id"], bookmark_properties=["created_at"]
    )

    latest_updated_at = utils.strptime_to_utc(get_start("transactions"))

    run_maximum_updated_at = latest_updated_at

    period_start = latest_updated_at - TRAILING_DAYS

    logger.info("transactions: Syncing from {}".format(period_start))

    logger.info("transactions: latest_updated_at from {}".format(latest_updated_at))

    for merchant in merchant_accounts:
        logger.info(
            "transactions: Fetching transactions for account {}".format(merchant.id)
        )

        time_extracted = utils.now()
        data = gateway.transaction.search(
            braintree.TransactionSearch.merchant_account_id == merchant.id
        )

        logger.info(
            "transactions: Fetched {} records for account {}".format(
                data.maximum_size, merchant.id
            )
        )

        rows_to_sync = [
            row
            for row in data.items
            if row.updated_at.replace(tzinfo=pytz.UTC) >= period_start
        ]

        logger.info(
            "transactions: {} records to sync for account {}".format(
                len(rows_to_sync), merchant.id
            )
        )

        row_written_count = 0
        row_skipped_count = data.maximum_size - len(rows_to_sync)

        for row in rows_to_sync:
            # Ensure updated_at consistency
            if not getattr(row, "updated_at"):
                row.updated_at = row.created_at

            transformed = transform_row(row, schema)
            updated_at = to_utc(row.updated_at)

            # Is this the most recently updated transaction?
            # if so, update run_maximum_updated_at for STATE
            if updated_at > run_maximum_updated_at:
                run_maximum_updated_at = updated_at

            singer.write_record(
                "transactions", transformed, time_extracted=time_extracted
            )
            row_written_count += 1

            if row.disputes:
                logger.info(
                    "transactions: Fetching disputes for transaction {}".format(row.id)
                )
                _sync_disputes(gateway=gateway, transaction_id=row.id)

        if row_written_count:
            logger.info(
                "transactions: Written {} records for account {}".format(
                    row_written_count, merchant.id
                )
            )
        if row_skipped_count:
            logger.info(
                "transactions: Skipped {} records for account {}".format(
                    row_skipped_count, merchant.id
                )
            )

    # End day loop
    logger.info(
        "transactions: Complete. Last updated record: {}".format(run_maximum_updated_at)
    )

    latest_updated_at = run_maximum_updated_at

    utils.update_state(STATE, "transactions", latest_updated_at)

    singer.write_state(STATE)


def _sync_disputes(gateway, transaction_id):
    schema = load_schema("disputes")
    singer.write_schema("disputes", schema, ["id"])

    disputes = gateway.dispute.search(
        [braintree.DisputeSearch.transaction_id == transaction_id]
    )

    time_extracted = utils.now()
    row_written_count = 0
    for dispute in disputes.disputes.items:
        dispute.transaction_id = transaction_id
        transformed = transform_row(dispute, schema)
        singer.write_record("disputes", transformed, time_extracted=time_extracted)
        row_written_count += 1

    if row_written_count > 0:
        logger.info("disputes: Written {} records".format(row_written_count))


def do_sync(gateway):
    logger.info("Starting sync")
    merchants = sync_merchant_accounts(gateway)
    sync_transactions(merchant_accounts=merchants, gateway=gateway)
    logger.info("Sync completed")


@utils.handle_top_exception(logger)
def main():
    args = utils.parse_args(["merchant_id", "public_key", "private_key", "start_date"])
    config = args.config

    environment = getattr(
        braintree.Environment, config.pop("environment", "Production")
    )

    CONFIG["start_date"] = config.pop("start_date")

    gateway = braintree.BraintreeGateway(braintree.Configuration(environment, **config))

    if args.state:
        STATE.update(args.state)

    try:
        do_sync(gateway)
    except braintree.exceptions.authentication_error.AuthenticationError:
        logger.critical(
            "Authentication error occurred. "
            "Please check your merchant_id, public_key, and "
            "private_key for errors",
            exc_info=True,
        )


if __name__ == "__main__":
    main()
