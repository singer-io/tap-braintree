#!/usr/bin/env python3

import datetime

import braintree
import singer

from . import utils
from .transform import transform_row


STATE = {}
DEFAULT_START_DATE = utils.strftime(datetime.datetime.utcnow() - datetime.timedelta(days=365))

logger = singer.get_logger()


def sync_transactions():
    schema = utils.load_schema("transactions")
    singer.write_schema("transactions", schema, ["id"])

    now = datetime.datetime.utcnow()
    start = utils.strptime(STATE.get('transactions', DEFAULT_START_DATE))
    while start < now:
        end = start + datetime.timedelta(days=1)
        if end > now:
            end = now

        logger.info("Fetching from {} to {}".format(start, end))
        data = braintree.Transaction.search(braintree.TransactionSearch.created_at.between(start, end))
        logger.info("Fetched {} records".format(data.maximum_size, start, end))

        for row in data:
            transformed = transform_row(row, schema)
            singer.write_record("transactions", transformed)
            utils.update_state(STATE, "transactions", transformed["created_at"])

        singer.write_state(STATE)
        start += datetime.timedelta(days=1)


def do_sync():
    logger.info("Starting sync")
    sync_transactions()
    logger.info("Sync completed")


def main():
    args = utils.parse_args()
    config = utils.load_json(args.config)
    environment = getattr(braintree.Environment, config.pop("environment", "Production"))
    braintree.Configuration.configure(environment, **config)
    if args.state:
        STATE.update(utils.load_json(args.state))
    do_sync()


if __name__ == '__main__':
    main()
