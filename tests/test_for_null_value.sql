-- tests/test_for_null_value.sql
WITH null_check AS (
    {{ check_null_values('st_yellowtaxi_data', ['DOLOCATIONID', 'PULOCATIONID', 'RATECODEID', 'VENDORID']) }}
)

SELECT * FROM null_check
WHERE 1=1
LIMIT 100
