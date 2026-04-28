WITH source AS (
    SELECT * FROM {{ source('ltp_pricing_table_new_billing_calc', 'LTP_MIN_COMMIT_TBL') }}
),

renamed AS (
    SELECT
        TENANT_GLOBAL_ID,
        TENANT_NAME,
        MONTHLY_COMMITMENT_AMOUNT,
        START_DATE::date    AS START_DATE,
        END_DATE::date      AS END_DATE
    FROM source
)

SELECT * FROM renamed