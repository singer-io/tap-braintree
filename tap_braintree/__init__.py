#!/usr/bin/env python3
import sys
import json
import datetime
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

LOGGER = singer.get_logger()

def do_discover():
    """
    Run discovery mode
    """

    LOGGER.info("Starting discovery")
    catalog = discover()
    json.dump(catalog.to_dict(), sys.stdout, indent=2)
    LOGGER.info("Finished discover")

@utils.handle_top_exception(LOGGER)
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

    # This is added for credentials verification. If credentials are invalid then it will raise Authentication Error
    gateway.customer.search(
        braintree.CustomerSearch.created_at.between(
        datetime.datetime(2022, 6, 29),
        datetime.datetime(2011, 6, 30)
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
        LOGGER.critical('Authentication error occured. Please check your merchant_id, public_key, and private_key for errors', exc_info=True)

if __name__ == '__main__':
    main()
