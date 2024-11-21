#!/bin/bash

bq load --replace --source_format=CSV --autodetect=false --field_delimiter='|' --skip_leading_rows=1 \
    --max_bad_records=1000 --quote "" bigquery_direct.saleheader_raw_loading 'gs://retailer-raw-ingest-data/SalesHeaderFCT_inc_20240929.txt'
