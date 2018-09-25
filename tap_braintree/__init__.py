#!/usr/bin/env python3

import datetime
import os
# TODO I'm sorry to do this to you. We had a discussion internally and the
# Singer team is settled on using `pytz` exclusively for all timezone
# needs. The more rapidly updated TZ database is a requirement. Please
# convert everything back to `pytz`. I would absolutely accept a [best
# practices][1] PR to add it to the [dates section][2].
#
# [1]: https://github.com/singer-io/getting-started/blob/master/docs/BEST_PRACTICES.md#best-practices-for-building-a-singer-tap
# [2]: https://github.com/singer-io/getting-started/blob/master/docs/BEST_PRACTICES.md#dates
from datetime import timezone

import braintree
import singer

from singer import utils
from .transform import transform_row


CONFIG = {}
STATE = {}
TRAILING_DAYS = datetime.timedelta(days=30)
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
    return dt.replace(tzinfo=timezone.utc)

def sync_transactions():
    schema = load_schema("transactions")
    singer.write_schema("transactions", schema, ["id"], bookmark_properties=['created_at'])

    latest_updated_at = to_utc(utils.strptime(STATE.get('latest_updated_at', DEFAULT_TIMESTAMP)))
    run_maximum_updated_at = latest_updated_at
    latest_start_date = to_utc(utils.strptime(get_start("transactions")))
    start = latest_start_date - TRAILING_DAYS
    end = utils.now()

    logger.info("transactions: Syncing from {}".format(start))
    logger.debug("transactions: latest_updated_at from {}".format(latest_updated_at))
    logger.debug("transactions: latest_start_date from {}".format(latest_start_date))

    data = braintree.Transaction.search(braintree.TransactionSearch.created_at.between(start, end))
    time_extracted = utils.now()

    logger.debug("transactions: Fetched {} records from {} - {}".format(data.maximum_size, start, end))

    row_written_count = 0

    # TODO I think this comment is now out of date, as we're clearly not
    # sorting anymore. Let's remove it if that's the case or clarify it if
    # it's not.
    #
    # Note: requires fetching multiple days at once then sorting on updated_at
    # in order to assure that we get ratchet updated_at correctly.
    for row in data:
        # Ensure updated_at consistency
        if not getattr(row, 'updated_at'):
            row.updated_at = row.created_at

        transformed = transform_row(row, schema)
        updated_at = to_utc(row.updated_at)

        # Use >= due to non monotonic updated_at values
        if updated_at >= latest_updated_at:
            if updated_at > run_maximum_updated_at:
                run_maximum_updated_at = updated_at

            singer.write_record("transactions", transformed, time_extracted=time_extracted)
            row_written_count += 1
        # TODO I think it would be useful to have an `else` here that
        # incremented a `row_skipped_count` counter, as this change is
        # very likely to slow performance down for the tap. It would be
        # useful for people running it to see how much work it's doing
        # just to get those updates.

    logger.info("transactions: Written {} records from {} - {}".format(row_written_count, start, end))

    latest_updated_at = run_maximum_updated_at
    STATE['latest_updated_at'] = utils.strftime(latest_updated_at)
    utils.update_state(STATE, "transactions", utils.strftime(end))
    singer.write_state(STATE)


def do_sync():
    logger.info("Starting sync")
    sync_transactions()
    logger.info("Sync completed")


def main_impl():
    args = utils.parse_args(["merchant_id", "public_key", "private_key", "start_date"])
    config = args.config
    environment = getattr(braintree.Environment, config.pop("environment", "Production"))
    CONFIG['start_date'] = config.pop('start_date')
    braintree.Configuration.configure(environment, **config)

    if args.state:
        STATE.update(args.state)

    do_sync()

def main():
    try:
        main_impl()
    except Exception as exc:
        logger.critical(exc)
        raise exc

if __name__ == '__main__':
    main()
