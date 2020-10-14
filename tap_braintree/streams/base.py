
import math
import sys
import singer
from singer import metrics, utils
from tap_braintree.context import Context

LOGGER = singer.get_logger()

RESULTS_PER_PAGE = 175

# We've observed 500 errors returned if this is too large (30 days was too
# large for a customer)
DATE_WINDOW_SIZE = 1

# We will retry a 500 error a maximum of 5 times before giving up
MAX_RETRIES = 5


def is_not_status_code_fn(status_code):
    def gen_fn(exc):
        if getattr(exc, 'code', None) and exc.code not in status_code:
            return True
        # Retry other errors up to the max
        return False

    return gen_fn


def leaky_bucket_handler(details):
    LOGGER.info("Received 429 -- sleeping for %s seconds",
                details['wait'])


def retry_handler(details):
    LOGGER.info("Received 500 or retryable error -- Retry %s/%s",
                details['tries'], MAX_RETRIES)


# pylint: disable=unused-argument
def retry_after_wait_gen(**kwargs):
    # This is called in an except block so we can retrieve the exception
    # and check it.
    exc_info = sys.exc_info()
    resp = exc_info[1].response
    # Retry-After is an undocumented header. But honoring
    # it was proven to work in our spikes.
    # It's been observed to come through as lowercase, so fallback if not present
    sleep_time_str = resp.headers.get('Retry-After', resp.headers.get('retry-after'))
    yield math.floor(float(sleep_time_str))


class Error(Exception):
    """Base exception for the API interaction module"""


class OutOfOrderIdsError(Error):
    """Raised if our expectation of ordering by ID is violated"""


class Stream:
    # Used for bookmarking and stream identification. Is overridden by
    # subclasses to change the bookmark key.
    name = None
    replication_method = 'INCREMENTAL'
    replication_key = 'updated_at'
    key_properties = ['id']
    # Controls which SDK object we use to call the API by default.
    replication_object = None
    # Status parameter override option
    status_key = None
