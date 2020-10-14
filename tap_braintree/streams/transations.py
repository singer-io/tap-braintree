import braintree

from tap_braintree.context import Context
from tap_braintree.streams.base import Stream


class Transactions(Stream):
    name = 'transactions'
    replication_object = braintree.Transaction


Context.stream_objects['transactions'] = Transactions
