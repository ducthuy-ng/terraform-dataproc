SELECT
    customer_code,
    product_code,
    rank
FROM `modified-glyph-438213-k8.bigquery_direct.ppr`
WHERE customer_code = '3364390'
ORDER BY rank DESC
LIMIT 1000
