# Changelog

## 0.9.4
  * Bump dependency versions for twistlock compliance [#56](https://github.com/singer-io/tap-braintree/pull/56)

## 0.9.3
  * Dependabot update [#46](https://github.com/singer-io/tap-braintree/pull/46)

## 0.9.2
  * Update Braintree python SDK version to 4.18.1 [#45](https://github.com/singer-io/tap-braintree/pull/45)

## 0.9.1
  * Add `subscription_id` to `transactions` stream [#25](https://github.com/singer-io/tap-braintree/pull/25)

## 0.8.0
  * Use `disbursement_date` in addition to `updated_at` to sync updated records [#14](https://github.com/singer-io/tap-braintree/pull/14)
  * Add `payment_instrument_type`, `credit_card_details`, and `paypal_details` to `transactions` schema [#15](https://github.com/singer-io/tap-braintree/pull/15)

## 0.7.1
  * Update version of `requests` to `2.20.0` in response to CVE 2018-18074

## 0.7.0
  * Modifies the tap to capture updated transactions by:
      * bookmarking both the latest `created_at` and `updated_at`
      * pulling records from the API that were created after `(bookmarked_created_at - 30 days)`
      * emitting records if their `updated_at` field is greater than the bookmarked value
      * Pull Request [#8](https://github.com/singer-io/tap-braintree/pull/8)
