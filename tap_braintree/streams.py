import singer
import braintree
from datetime import datetime, timedelta, timezone
from braintree.exceptions.authorization_error import AuthorizationError

LOGGER = singer.get_logger()


class Transaction:
    name = "transactions"
    key_properties = ["id"]
    parent_stream = None
    replication_keys = ["updated_at"]
    replication_method = "INCREMENTAL"

    def check_access(self) -> bool:
        """Check if the stream is accessible."""
        if self.parent_stream:
            return True
        try:
            now = datetime.now(tz=timezone.utc)
            braintree.Transaction.search(
                braintree.TransactionSearch.created_at.between(now - timedelta(hours=1), now)
            )
            return True
        except AuthorizationError as exc:
            LOGGER.warning(
                "Permission Error: Stream '%s' is not accessible. Excluding from catalog. %s",
                self.__class__.__name__,
                exc,
            )
            return False


STREAMS = {"transactions": Transaction}
