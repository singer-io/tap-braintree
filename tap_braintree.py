#!/usr/bin/env python3

import argparse
import datetime
import json
import os
import sys

import braintree
import stitchstream


DATETIME_FMT = "%Y-%m-%dT%H:%M:%SZ"
DEFAULT_START_DATE = datetime.datetime(2015, 1, 1).strftime(DATETIME_FMT)

state = {
    "transactions": DEFAULT_START_DATE,
}

logger = stitchstream.get_logger()


def update_state(key, dt):
    if dt is None:
        return

    if isinstance(dt, datetime.datetime):
        dt = dt.strftime(DATETIME_FMT)

    if dt > state[key]:
        state[key] = dt


def load_schema(entity):
    path = os.path.join(
        os.path.dirname(os.path.realpath(__file__)),
        "tap_braintree",
        "{}.json".format(entity))

    with open(path) as f:
        return json.load(f)


def transform_field(value, schema):
    if "anyOf" in schema:
        for s in schema["anyOf"]:
            try:
                return transform_field(value, s)
            except ValueError:
                pass

        raise ValueError("No matching schema")

    if value is None:
        if "null" in schema["type"]:
            return value
        else:
            raise ValueError("Got None and null is not an allowed type")

    if "array" in schema['type']:
        tmp = []
        for v in value:
            tmp.append(transform_field(v, schema['items']))

        return tmp

    elif "object" in schema['type']:
        tmp = {}
        for field_name, field_schema in schema['properties'].items():
            if hasattr(value, field_name):
                tmp[field_name] = transform_field(getattr(value ,field_name), field_schema)

        return tmp

    elif "format" in schema:
        if schema['format'] == "date-time" and value:
            return value.strftime(DATETIME_FMT)

    elif "integer" in schema['type'] and value:
        return int(value)

    elif "number" in schema['type'] and value:
        return float(value)

    return value


def transform_record(record, schema):
    return {field_name: transform_field(getattr(record, field_name), field_schema)
            for field_name, field_schema in schema['properties'].items()
            if hasattr(record, field_name)}


def sync_transactions():
    schema = load_schema("transactions")
    stitchstream.write_schema("transactions", schema, "id")

    start = datetime.datetime.strptime(state['transactions'], DATETIME_FMT)
    now = datetime.datetime.utcnow()

    while start < now:
        end = start + datetime.timedelta(days=1)
        if end > now:
            end = now

        logger.info("Fetching from {} to {}".format(start, end))
        transactions = braintree.Transaction.search(braintree.TransactionSearch.created_at.between(start, end))
        logger.info("Fetched {} records".format(transactions.maximum_size, start, end))

        for transaction in transactions:
            transformed = transform_record(transaction, schema)
            stitchstream.write_record("transactions", transformed)
            update_state("transactions", transformed["created_at"])
            stitchstream.write_state(state)

        start += datetime.timedelta(days=1)


def do_sync():
    sync_transactions()


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('-c', '--config', help='Config file', required=True)
    parser.add_argument('-s', '--state', help='State file')
    args = parser.parse_args()

    if args.state:
        logger.info("Loading state from " + args.state)
        with open(args.state) as f:
            state.update(json.load(f))
    else:
        logger.info("Using default state")

    logger.info("Authorizing")
    with open(args.config) as f:
        config = json.load(f)

    environment = getattr(braintree.Environment, config.pop("environment", "Production"))
    braintree.Configuration.configure(environment, **config)

    try:
        logger.info("Starting sync")
        do_sync()
    except Exception as e:
        logger.exception("Error ocurred during sync. Aborting.")
        sys.exit(1)


if __name__ == '__main__':
    main()
