#!/usr/bin/env python3

from datetime import datetime, timedelta
import os
import pytz
import json
import sys

import braintree
import singer

from singer import utils
from tap_braintree.discover import discover
from tap_braintree.sync import sync as _sync

REQUIRED_CONFIG_KEYS = [
    "merchant_id", 
    "public_key", 
    "private_key", 
    "start_date"
]

logger = singer.get_logger()

def do_discover():
    logger.info("Starting discovery")
    catalog = discover()
    json.dump(catalog.to_dict(), sys.stdout, indent=2)
    logger.info("Finished discover")

@utils.handle_top_exception(logger)
def main():
    parsed_args = singer.utils.parse_args(REQUIRED_CONFIG_KEYS)
    config = {}
    state = {}
    
    if parsed_args.config:
        config = parsed_args.config

    if parsed_args.state:
        state = parsed_args.state

    environment = getattr(braintree.Environment, config.pop("environment", "Production"))
    
    gateway = braintree.BraintreeGateway(
        braintree.Configuration(
            environment,
            merchant_id = config['merchant_id'],
            public_key= config["public_key"],
            private_key=config["private_key"]
        )
    )
    try:
        if parsed_args.discover:
            do_discover()
        else:
            _sync(
                gateway, 
                config,
                parsed_args.catalog or discover(),
                state
            )    
    except braintree.exceptions.authentication_error.AuthenticationError:
        logger.critical('Authentication error occured. '
                        'Please check your merchant_id, public_key, and '
                        'private_key for errors', exc_info=True)


if __name__ == '__main__':
    main()
