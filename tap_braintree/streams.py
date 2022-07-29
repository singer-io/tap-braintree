import pytz
import singer
import braintree
import backoff
from singer import utils
from datetime import datetime, timedelta
from .transform import transform_row

from braintree.exceptions.too_many_requests_error import TooManyRequestsError
from braintree.exceptions.server_error import ServerError
from braintree.exceptions.service_unavailable_error import ServiceUnavailableError
from braintree.exceptions.gateway_timeout_error import GatewayTimeoutError


TRAILING_DAYS = timedelta(days=30)
DEFAULT_TIMESTAMP = "1970-01-01T00:00:00Z"
LOGGER = singer.get_logger()

class Stream:
    name = None
    replication_method = None
    replication_keys = None
    key_properties = None
    parent_stream = None

    # To write schema in output
    def write_schema(self, schema, stream_name, sync_streams , selected_streams):
        """
        To write schema in output
        """
        try:
            # Write_schema for the stream if it is selected in catalog
            if stream_name in selected_streams and stream_name in sync_streams:
                singer.write_schema(stream_name, schema, self.key_properties)
        except OSError as err:
            LOGGER.error('OS Error writing schema for: {}'.format(stream_name))
            raise err
    
    def to_utc(self, dt):
        """
        Set UTC offset for Python datetime object
        """
        return dt.replace(tzinfo=pytz.UTC)
    
    def write_bookmark(self, state, stream, value):
        """
        To write bookmark in sync mode
        """

        if 'bookmarks' not in state:
            state['bookmarks'] = {}
        if stream not in state['bookmarks']:
            state['bookmarks'][stream] = {}
            
        state['bookmarks'][stream] = value
        singer.write_state(state)
        
    def daterange(self, start_date, end_date):
        """
        Generator function that produces an iterable list of days between the two
        dates start_date and end_date as a tuple pair of datetimes.

        Note:
            All times are set to 0:00. Designed to be used in date query where query
            logic would be record_date >= 2019-01-01 0:00 and record_date < 2019-01-02 0:00

        Args:
            start_date (datetime): start of period
            end_date (datetime): end of period

        Yields:
            tuple: daily period
                * datetime: day within range
                * datetime: day within range + 1 day

        """

        # set to start of day
        start_date = self.to_utc(
            datetime.combine(
                start_date.date(),
                datetime.min.time()  # set to the 0:00 on the day of the start date
            )
        )
        end_date = self.to_utc(end_date + timedelta(1))

        for n in range(int((end_date - start_date).days)):
            yield start_date + timedelta(n), start_date + timedelta(n + 1)

class SyncWithoutWindow(Stream):
    sdk_call = None
    key_properties = ["id"]
    replication_keys = "updated_at"
    replication_method = "INCREMENTAL"

    # Backoff the request for 5 times when ConnectionError, TooManyRequestsError (status code = 429),
    # ServerError(status code = 500) , ServiceUnavailableError (status code = 503) ,
    # or GatewayTimeoutError (status code = 504) occurs
    @backoff.on_exception(
        backoff.expo,
        (ConnectionError, TooManyRequestsError, ServerError, ServiceUnavailableError, GatewayTimeoutError),
        max_tries=5,
        factor=2)
    def sync(self, gateway, config, schema, state, selected_streams):
        """
        Sync function for incremental stream without window logic
        """

        data = self.sdk_call(gateway)
        time_extracted = utils.now()
        latest_start_date = utils.strptime_to_utc(state.get("bookmarks", {}).get(self.name, {}).get(self.replication_keys, config['start_date']) )
        max_updated_at = latest_updated_at = latest_start_date

        period_start = latest_start_date - TRAILING_DAYS

        LOGGER.info("{}: Syncing from {}".format(self.name, period_start))
        
        LOGGER.info("{}: latest_start_date from {}".format(self.name, latest_start_date))
        
        row_written_count = 0
        
        for row in data:
            # Ensure updated_at consistency
            if not getattr(row, 'updated_at'):
                row.updated_at = row.created_at
                
            if self.to_utc(row.updated_at) >= latest_start_date:
                transformed = transform_row(row, schema)
                latest_updated_at = self.to_utc(row.updated_at)
                max_updated_at = max(max_updated_at, latest_updated_at)
                singer.write_record(self.name, transformed, time_extracted=time_extracted)
                row_written_count += 1
                
        LOGGER.info("{}: Written {} records from {} - {}".format(self.name, row_written_count, latest_start_date, time_extracted))
        
        LOGGER.info("{}: Complete. Last updated record: {}".format(self.name, latest_updated_at))

        state_value = {
            self.replication_keys: utils.strftime(max_updated_at)
        }
        self.write_bookmark(state, self.name, state_value)

        return row_written_count

class SyncWithWindow(Stream):
    sdk_call = None
    key_properties = ["id"]
    replication_keys = "created_at"
    replication_method = "INCREMENTAL"

    # Backoff the request for 5 times when ConnectionError, TooManyRequestsError (status code = 429),
    # ServerError(status code = 500) , ServiceUnavailableError (status code = 503) ,
    # or GatewayTimeoutError (status code = 504) occurs
    @backoff.on_exception(
        backoff.expo,
        (ConnectionError, TooManyRequestsError, ServerError, ServiceUnavailableError, GatewayTimeoutError),
        max_tries=5,
        factor=2)
    def sync(self, gateway, config, schema, state, selected_streams):
        """
        Sync function for incremental stream with window logic
        """
        has_updated_at = self.name in {"customers", "disputes", "subscriptions", "transactions"}
        has_disbursement = self.name in {"disputes", "transactions"}
        
        latest_start_date = utils.strptime_to_utc(state.get("bookmarks", {}).get(self.name, {}).get(self.replication_keys, config['start_date']) )

        period_start = latest_start_date - TRAILING_DAYS

        period_end = utils.now()

        LOGGER.info("{}: Syncing from {}".format(self.name, period_start))
        LOGGER.info("{}: latest_start_date from {}".format(self.name, latest_start_date))
        
        if has_updated_at:
            latest_updated_at = utils.strptime_to_utc(state.get("bookmarks", {}).get(self.name, {}).get('latest_updated_at', DEFAULT_TIMESTAMP))
            run_maximum_updated_at = latest_updated_at
            LOGGER.info("{}: latest_updated_at from {}".format(self.name, latest_updated_at))
            
        if has_disbursement:
            latest_disbursement_date = utils.strptime_to_utc(state.get("bookmarks", {}).get(self.name, {}).get('latest_disbursment_date', DEFAULT_TIMESTAMP))
            run_maximum_disbursement_date = latest_disbursement_date
            LOGGER.info("{}: disbursement_date from {}".format(self.name, latest_disbursement_date))

        # increment through each day (20k results max from api)
        for start, end in self.daterange(period_start, period_end):
            end = min(end, period_end)

            data = self.sdk_call(gateway, start, end)
            time_extracted = utils.now()

            row_written_count = 0
            row_skipped_count = 0

            for row in data:
                # Ensure updated_at consistency
                if has_updated_at and not getattr(row, 'updated_at'):
                    row.updated_at = row.created_at

                if self.name == "settlement_batch_summary":
                    row['settlement_date'] = utils.strftime(start)

                transformed = transform_row(row, schema)
                
                if has_disbursement:
                    # if disbursement is successful, get disbursement date
                    # set disbursement datetime to min if not found
                    if row.disbursement_details is None:
                        disbursement_date = self.to_utc(datetime.min)
                    else:
                        if row.disbursement_details.disbursement_date is None:
                            row.disbursement_details.disbursement_date = datetime.min
                        disbursement_date = self.to_utc(datetime.combine(row.disbursement_details.disbursement_date, datetime.min.time()))

                # Is this more recent than our past stored value of update_at?
                # Is this more recent than our past stored value of disbursement_date?
                # Use >= for updated_at due to non monotonic updated_at values
                # Use > for disbursement_date - confirming all records disbursed
                # at the same time
                # Update our high water mark for updated_at and disbursement_date
                # in this run
                
                if not (has_updated_at and has_disbursement) and self.name in selected_streams:
                    singer.write_record(self.name, transformed, time_extracted=time_extracted)
                    row_written_count += 1
                else:
                    updated_at = self.to_utc(row.updated_at)
                    
                    if has_disbursement and ((updated_at >= latest_updated_at) or (disbursement_date >= latest_disbursement_date)):
                        
                        run_maximum_updated_at = max(updated_at, run_maximum_updated_at)
                        run_maximum_disbursement_date = max(disbursement_date, run_maximum_disbursement_date)

                        if self.name in selected_streams:
                            singer.write_record(self.name, transformed, time_extracted=time_extracted)
                            row_written_count += 1
                            
                    elif has_updated_at and (updated_at >= latest_updated_at):
                        run_maximum_updated_at = max(updated_at, run_maximum_updated_at)
                        if self.name in selected_streams:
                            singer.write_record(self.name, transformed, time_extracted=time_extracted)
                            row_written_count += 1
                        
                    else:
                        row_skipped_count += 1

            LOGGER.info("{}: Written {} records from {} - {}".format(self.name, row_written_count, start, end))

            LOGGER.info("{}: Skipped {} records from {} - {}".format(self.name, row_skipped_count, start, end))
            
        state_value = {
            self.replication_keys: utils.strftime(end)
        }
        
        if has_updated_at:
            LOGGER.info("{}: Complete. Last updated record: {}".format(self.name, run_maximum_updated_at))
            latest_updated_at = run_maximum_updated_at
            state_value["latest_updated_at"] = utils.strftime(latest_updated_at)
            
        if has_disbursement:
            LOGGER.info("{}: Complete. Last disbursement date: {}".format(self.name, run_maximum_disbursement_date))
            latest_disbursement_date = run_maximum_disbursement_date
            state_value["latest_disbursement_date"] = utils.strftime(latest_disbursement_date)

        self.write_bookmark(state, self.name, state_value)

        return row_written_count

class FullTableSync(Stream):
    sdk_call = None
    key_properties = ["id"]

    # Backoff the request for 5 times when ConnectionError, TooManyRequestsError (status code = 429),
    # ServerError(status code = 500) , ServiceUnavailableError (status code = 503) ,
    # or GatewayTimeoutError (status code = 504) occurs
    @backoff.on_exception(
        backoff.expo,
        (ConnectionError, TooManyRequestsError, ServerError, ServiceUnavailableError, GatewayTimeoutError),
        max_tries=5,
        factor=2)
    def sync(self, gateway, config, schema, state, selected_streams):
        """
        Sync function for full_table stream
        """
        data = self.sdk_call(gateway)
        time_extracted = utils.now()
        row_written_count = 0
        
        for row in data:
            transformed = transform_row(row, schema)
            singer.write_record(self.name, transformed, time_extracted=time_extracted)
            row_written_count += 1
                
        return row_written_count
    
class AddOn(SyncWithoutWindow):
    name = "add_ons"
    sdk_call = lambda self, gateway: gateway.add_on.all()

class Customer(SyncWithWindow):
    name = "customers"
    sdk_call = lambda self, gateway, start, end: gateway.customer.search(braintree.CustomerSearch.created_at.between(start, end))

class Discount(SyncWithoutWindow):
    name = "discounts"
    sdk_call = lambda self, gateway: gateway.discount.all()
    
class Dispute(SyncWithWindow):
    name = "disputes"
    replication_keys = "received_date"
    sdk_call = lambda self, gateway, start, end: gateway.dispute.search(braintree.DisputeSearch.received_date.between(start, end)).disputes.items

class MerchantAccount(FullTableSync):
    name = "merchant_accounts"
    sdk_call = lambda self, gateway: gateway.merchant_account.all().merchant_accounts

class Plan(SyncWithoutWindow):
    name = "plans"
    sdk_call = lambda self, gateway: gateway.plan.all()

class SettlementBatchSummary(SyncWithWindow):
    name = "settlement_batch_summary"
    replication_keys = "settlement_date"
    key_properties = ["settlement_date"]
    sdk_call = lambda self, gateway, start, end: gateway.settlement_batch_summary.generate(start.strftime("%Y-%m-%d")).settlement_batch_summary.records

class Subscription(SyncWithWindow):
    name = "subscriptions"
    sdk_call = lambda self, gateway, start, end: gateway.subscription.search(braintree.SubscriptionSearch.created_at.between(start, end))

class Transaction(SyncWithWindow):
    name = "transactions"
    sdk_call = lambda self, gateway, start, end: gateway.transaction.search(braintree.TransactionSearch.created_at.between(start, end))

STREAMS = {
    "add_ons": AddOn,
    "customers": Customer,
    "discounts": Discount,
    "disputes": Dispute,
    "merchant_accounts": MerchantAccount,
    "plans": Plan,
    "settlement_batch_summary": SettlementBatchSummary,
    "subscriptions": Subscription,
    "transactions": Transaction
}
