# Changelog

## 0.7.1
  * Update version of `requests` to `2.20.0` in response to CVE 2018-18074

## 0.7.0
  * Modifies the tap to capture updated transactions by:
      * bookmarking both the latest `created_at` and `updated_at`
      * pulling records from the API that were created after `(bookmarked_created_at - 30 days)`
      * emitting records if their `updated_at` field is greater than the bookmarked value
      * Pull Request [#8](https://github.com/singer-io/tap-braintree/pull/8)
