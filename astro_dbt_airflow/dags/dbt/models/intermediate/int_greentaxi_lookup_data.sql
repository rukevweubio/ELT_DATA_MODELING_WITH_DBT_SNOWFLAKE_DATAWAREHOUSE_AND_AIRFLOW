{{ config(materialized='table') }}

WITH green_table_data AS (
    SELECT
        {{ dbt_utils.generate_surrogate_key(['VENDORID']) }} AS green_taxi_id, 
         
        DOLOCATIONID,
        PULOCATIONID,
        RATECODEID,
        VENDORID,
        CONGESTION_SURCHARGE,
        EXTRA,
        FARE_AMOUNT,
        IMPROVEMENT_SURCHARGE,
        MTA_TAX,
        PASSENGER_COUNT,
        PAYMENT_TYPE,
        STORE_AND_FWD_FLAG,
        TIP_AMOUNT,
        TOLLS_AMOUNT,
        TOTAL_AMOUNT,
        LPEP_DROPOFF_DATETIME,
        LPEP_PICKUP_DATETIME,
        TRIP_DISTANCE
    FROM {{ ref('st_greentaxi_data') }}
),

lookup_data AS (
    SELECT
        {{ dbt_utils.generate_surrogate_key(['LOCATIONID']) }} AS dbt_lookup_id,  -- updated macro
        LOCATIONID,
        BOROUGH,
        ZONE,
        SERVICE_ZONE
    FROM {{ ref('st_lookuptaxi_data') }}
)

SELECT 
    green_taxi_id,
    DOLOCATIONID,
    PULOCATIONID,
    RATECODEID,
    VENDORID,
    CONGESTION_SURCHARGE,
    EXTRA,
    FARE_AMOUNT,
    IMPROVEMENT_SURCHARGE,
    MTA_TAX,
    PASSENGER_COUNT,
    PAYMENT_TYPE,
    STORE_AND_FWD_FLAG,
    TIP_AMOUNT,
    TOLLS_AMOUNT,
    TOTAL_AMOUNT,
    LPEP_DROPOFF_DATETIME,
    LPEP_PICKUP_DATETIME,
    TRIP_DISTANCE,
    BOROUGH,
    ZONE,
    SERVICE_ZONE
FROM green_table_data
JOIN lookup_data
    ON green_table_data.DOLOCATIONID = lookup_data.LOCATIONID
    AND green_table_data.PULOCATIONID = lookup_data.LOCATIONID



