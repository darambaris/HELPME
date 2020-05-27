-- Triangulation is interesting when we want to add new columns in specific orders.
CREATE TABLE schema.table_name_temp AS 
    SELECT 
        field_1,
        field_2,
        field_3,
        -1::bigint as new_bigint_field,
        field_4,
        field_5,
        ''::varchar as new_varchar_field,
        field_6,
        field_7,
        '2020-01-01 00:00:00'::timestamp as new_timestamp_field,
        '2020-01-01'::date as new_date_field,
        field_8,
        field_9,
        field_10
    FROM
        schema.table_name;

SELECT count(*) FROM schema.table_name;
SELECT count(*) FROM schema.table_name_temp;

DROP TABLE schema.table_name;

ALTER TABLE schema.table_name_temp
RENAME TO table_name;
