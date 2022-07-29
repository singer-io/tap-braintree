#!/usr/bin/env python3
import sys
import json
import braintree
from requests import request
import singer
from singer import utils
from tap_braintree.discover import discover
from tap_braintree.sync import sync as _sync

REQUEST_TIME_OUT = 300

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
    
    request_timeout = config.get("request_timeout")
    if request_timeout and float(request_timeout):
        request_timeout = float(request_timeout)
    else:
        # If value in config is 0 or of type string then set default to 300 seconds.
        request_timeout = REQUEST_TIME_OUT

    gateway = braintree.BraintreeGateway(
        braintree.Configuration(
            environment,
            merchant_id = config['merchant_id'],
            public_key= config["public_key"],
            private_key=config["private_key"],
            timeout = request_timeout
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
