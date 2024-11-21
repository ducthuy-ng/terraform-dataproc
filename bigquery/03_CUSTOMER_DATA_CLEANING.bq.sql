UPDATE `bigquery_direct.customer_raw_loading`
SET EMAIL_ADDRESS = null
WHERE REGEXP_CONTAINS(EMAIL_ADDRESS, r'@[a-zA-Z0-9-]+\.[a-zA-Z0-9-.]+') = false;

UPDATE `bigquery_direct.customer_raw_loading`
SET CUSTOMER_LOCATION = null
WHERE REGEXP_CONTAINS(CUSTOMER_LOCATION, r'^0[\s]*\\[\s]*0$') = true;
