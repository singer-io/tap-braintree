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


def write_schemas(schemas: dict):
    for schema, key_properties in schemas.items():
        schema_body = load_schema(schema)
        singer.write_schema(schema, schema_body, key_properties)


def sync_merchant_accounts(gateway):
    entity = "merchant_accounts"
    data = gateway.merchant_account.all()
    time_extracted = utils.now()
    row_written_count = 0

    for merchant_account in data.merchant_accounts:
        transformed = transform_row(merchant_account, load_schema(entity))
        singer.write_record(entity, transformed, time_extracted=time_extracted)
        row_written_count += 1

    if row_written_count > 0:
        logger.info("{}: Written {} records".format(entity, row_written_count))

    return data.merchant_accounts


def sync_transactions(merchant_accounts, gateway):
    entity = "transactions"
    latest_updated_at = utils.strptime_to_utc(get_start(entity))
    run_maximum_updated_at = latest_updated_at
    period_start = latest_updated_at - TRAILING_DAYS

    logger.info("{}: Syncing from {}".format(entity, period_start))
    logger.info("{}: latest_updated_at from {}".format(entity, latest_updated_at))

    for merchant in merchant_accounts:
        logger.info(
            "{}: Fetching transactions for account {}".format(entity, merchant.id)
        )

        time_extracted = utils.now()
        data = gateway.transaction.search(
            braintree.TransactionSearch.merchant_account_id == merchant.id
        )

        logger.info(
            "{}: Fetched {} records for account {}".format(
                entity, data.maximum_size, merchant.id
            )
        )

        rows_to_sync = [
            row
            for row in data.items
            if row.updated_at.replace(tzinfo=pytz.UTC) >= period_start
        ]

        logger.info(
            "{}: {} records to sync for account {}".format(
                entity, len(rows_to_sync), merchant.id
            )
        )

        row_written_count = 0
        row_skipped_count = data.maximum_size - len(rows_to_sync)

        for row in rows_to_sync:
            # Ensure updated_at consistency
            if not getattr(row, "updated_at"):
                row.updated_at = row.created_at

            transformed = transform_row(row, load_schema(entity))
            updated_at = to_utc(row.updated_at)

            # Is this the most recently updated transaction?
            # if so, update run_maximum_updated_at for STATE
            if updated_at > run_maximum_updated_at:
                run_maximum_updated_at = updated_at

            singer.write_record(entity, transformed, time_extracted=time_extracted)
            row_written_count += 1

            logger.info(
                "{}: Syncing transaction history for transaction {}".format(
                    entity, row.id
                )
            )
            _sync_transaction_status_history(
                status_history=row.status_history, transaction_id=row.id
            )

            if row.disputes:
                logger.info(
                    "{}: Syncing disputes for transaction {}".format(entity, row.id)
                )
                _sync_disputes(disputes=row.disputes)

        if row_written_count:
            logger.info(
                "{}: Written {} records for account {}".format(
                    entity, row_written_count, merchant.id
                )
            )
        if row_skipped_count:
            logger.info(
                "{}: Skipped {} records for account {}".format(
                    entity, row_skipped_count, merchant.id
                )
            )

    # End day loop
    logger.info(
        "{}: Complete. Last updated record: {}".format(entity, run_maximum_updated_at)
    )

    latest_updated_at = run_maximum_updated_at

    utils.update_state(STATE, entity, latest_updated_at)

    singer.write_state(STATE)


def _sync_disputes(*, disputes):
    entity = "disputes"
    time_extracted = utils.now()
    row_written_count = 0

    for dispute in disputes:
        dispute.transaction_id = dispute.transaction.id
        transformed = transform_row(dispute, load_schema(entity))
        singer.write_record(entity, transformed, time_extracted=time_extracted)
        row_written_count += 1
        _sync_dispute_status_history(
            status_history=dispute.status_history, dispute_id=dispute.id
        )

    if row_written_count > 0:
        logger.info("{}: Written {} records".format(entity, row_written_count))


def _sync_dispute_status_history(*, status_history, dispute_id):
    entity = "dispute_status_history"
    time_extracted = utils.now()
    row_written_count = 0

    for status in status_history:
        status.dispute_id = dispute_id
        transformed = transform_row(status, load_schema(entity))
        singer.write_record(entity, transformed, time_extracted=time_extracted)
        row_written_count += 1

    if row_written_count > 0:
        logger.info("{}: Written {} records".format(entity, row_written_count))


def _sync_transaction_status_history(*, status_history, transaction_id):
    entity = "transaction_status_history"
    time_extracted = utils.now()
    row_written_count = 0

    for status in status_history:
        status.transaction_id = transaction_id
        transformed = transform_row(status, load_schema(entity))
        singer.write_record(entity, transformed, time_extracted=time_extracted)
        row_written_count += 1

    if row_written_count > 0:
        logger.info("{}: Written {} records".format(entity, row_written_count))


def do_sync(gateway):
    logger.info("Starting sync")
    schemas = {
        "merchant_accounts": ["id"],
        "transactions": ["id"],
        "transaction_status_history": ["transaction_id", "status", "timestamp"],
        "disputes": ["id"],
        "dispute_status_history": ["dispute_id", "effective_date"],
    }

    write_schemas(schemas)
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
