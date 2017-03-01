#!/usr/bin/env python3

import datetime

import braintree
import singer

from . import utils
from .transform import transform_row


CONFIG = {}
STATE = {}

logger = singer.get_logger()


def get_start(entity):
    if entity not in STATE:
        STATE[entity] = CONFIG["start_date"]

    return STATE[entity]


def sync_transactions():
    schema = utils.load_schema("transactions")
    singer.write_schema("transactions", schema, ["id"])

    now = datetime.datetime.utcnow()
    start = utils.strptime(get_start("transactions"))
    logger.info("transactions: Syncing from {}".format(start))

    while start < now:
        end = start + datetime.timedelta(days=1)
        if end > now:
            end = now

        data = braintree.Transaction.search(braintree.TransactionSearch.created_at.between(start, end))
        logger.info("transactions: Fetched {} records from {} - {}".format(data.maximum_size, start, end))

        for row in data:
            transformed = transform_row(row, schema)
            singer.write_record("transactions", transformed)

        utils.update_state(STATE, "transactions", utils.strftime(end))
        singer.write_state(STATE)
        start += datetime.timedelta(days=1)


def do_sync():
    logger.info("Starting sync")
    sync_transactions()
    logger.info("Sync completed")


def main():
    args = utils.parse_args()

    config = utils.load_json(args.config)
    utils.check_config(config, ["merchant_id", "public_key", "private_key", "start_date"])
    environment = getattr(braintree.Environment, config.pop("environment", "Production"))
    CONFIG['start_date'] = config.pop('start_date')
    braintree.Configuration.configure(environment, **config)

    if args.state:
        STATE.update(utils.load_json(args.state))

    do_sync()


if __name__ == '__main__':
    main()
