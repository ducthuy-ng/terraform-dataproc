DECLARE temp string;
FOR columnname IN (
    SELECT column_name
    FROM
        bigquery_direct.information_schema.columns
    WHERE
        table_name = "customer_raw_loading" AND data_type = "STRING"
)
DO
    /*SET temp= CAST( (select columnname.COLUMN_NAME) AS STRING);*/
    SET temp
    = CONCAT(
        "UPDATE `bigquery_direct.customer_raw_loading` SET ",
        CAST((columnname.column_name) AS string),
        "=NULL WHERE REGEXP_CONTAINS(",
        CAST((columnname.column_name) AS string),
        ", r'[^\\s]')=FALSE;"
    );
    EXECUTE IMMEDIATE temp;
END FOR;
