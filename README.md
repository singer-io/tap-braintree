# tap-braintree

This is a [Singer](https://singer.io) tap that produces JSON-formatted data following the [Singer spec](https://github.com/singer-io/getting-started/blob/master/SPEC.md).

This tap:
- Pulls raw data from Braintree's REST API
- Extracts the following resources from Braintree
  - Transactions
- Outputs the schema for each resource
- Incrementally pulls data based on the input state

## Quick start

1. Install

    ```bash
    > pip install tap-braintree
    ```

2. Get your Public Key, Private Key, and Merchant ID


    1. Sign into your Braintree account, Click Account > My User.
    2. Scroll down to the API Keys, Tokenization Keys, Encryption Keys section and click View Authorizations.
    3. In the API Keys section, click Generate New API Key.
    4. After the key has been generated, click the View link in the Private Key column.

    This will display the Client Library Key page, which contains your Braintree API credentials. You’ll need the Public Key, Private Key, and Merchant ID to complete your configuration.

3. Create the config file

    Create a JSON file called `config.json` containing the Merchant ID, Public Key and Private Key.

    ```json
    {"merchant_id": "your-merchant-id",
     "public_key": "your-public-key",
     "private_key": "your-private-key",
     "start_date": "2017-01-17T20:32:05Z"}
    ```
   
   If desired, you can also provide `"environment": "Sandbox"` (useful for running it 
   locally during development).

4. [Optional] Create the initial state file

    You can provide JSON file that contains a date for the API endpoints
    to force the application to only fetch data newer than those dates.
    If you omit the file it will fetch all Braintree data starting at the 
    `start_date` above.

    ```json
    {"transactions": "2017-01-17T20:32:05Z"}
    ```

5. Run the application

    `tap-braintree` can be run with:

    ```bash
    tap-braintree --config config.json [--state state.json]
    ```

## Trailing days when syncing ransactions

As of now, Braintree does not offer an option to fetch transactions through its API
based on an `updated_at` field ([Transaction.search() documentation](https://developers.braintreepayments.com/reference/request/transaction/search/python)). 

This tap uses `created_at` as a filter instead, but since transactions can have their 
status or other fields updated after they have been created, this tap performs searches 
based on the last start date (if available on the state) minus 30 trailing days 
(currently fixed and not configurable) to check if any transaction created in the
last 30 days had any update. 

Note that if a transaction has indeed been updated, it will contain a `updated_at`
timestamp on the response, and this field (among with more logic) is used
to filter out transactions that have been fetched due to the "trailing days" logic
but have not been updated since the last run, avoiding them to be exported again on
every run. 

---

Copyright &copy; 2017 Stitch
