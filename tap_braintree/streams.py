class Transaction:
    name = "transactions"
    key_properties = ["id"]
    parent_stream = None
    replication_keys = "created_at"
    replication_method = "INCREMENTAL"


STREAMS = {"transactions": Transaction}
