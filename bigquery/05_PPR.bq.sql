create or replace table bigquery_direct.ppr as
select
    transaction_header.customer_code,
    transaction_line.product_code,
    ROW_NUMBER()
        over (
            partition by
                transaction_header.customer_code
            order by COUNT(distinct transaction_header.invoice_no) desc
        )
        as rank
from
    bigquery_direct.saleheader_maintable as transaction_header
inner join
    bigquery_direct.saleline_maintable as transaction_line
    on
        transaction_header.invoice_no = transaction_line.invoice_no
        and transaction_header.store_code = transaction_line.store_code
        and transaction_header.till_no = transaction_line.till_no
        and transaction_header.sale_invc_type = transaction_line.sale_invc_type
where
    transaction_header.customer_code is not null and transaction_header.customer_code != '0'
group by
    transaction_header.customer_code, transaction_line.product_code
