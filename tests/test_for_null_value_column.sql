
    SELECT
       VENDORID , COUNT(*) AS count
    FROM {{ ref('st_yellowtaxi_data') }}
    GROUP BY VENDORID
    HAVING COUNT(*) > 1 
