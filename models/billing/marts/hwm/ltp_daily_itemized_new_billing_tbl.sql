{{ config(
    materialized='incremental',
    unique_key=['billing_date'],
    incremental_strategy='delete+insert'
) }}

-- WARNING: Do NOT run with --full-refresh. This will wipe all accumulated history
-- and rebuild with today's data only (the model SELECT only computes current_date).
-- All normal runs (without --full-refresh) preserve historical rows and only
-- replace today's rows. Recovery from accidental full-refresh requires backfill
-- from PROD_MART.FINANCE.LTP_DAILY_ITEMIZED_NEW_BILLING_TBL.

with pricing_function_all_ltps as (
    select * from {{ ref('ltp_daily_billing_calc')}} 
),

ltp_nfr_calc as (
    select * from {{ ref("ltp_nfr_calculation_new_billing")}} 
)


SELECT
current_date as billing_date,
ltp,
item,
sku,
partner_pricing,
price_type,
sum(quantity) as quantity,
sum(amount) as amount
from pricing_function_all_ltps
group by
    billing_date,
    ltp,
    item,
    sku,
    partner_pricing,
    price_type


union all

select
current_date as billing_date,
TENANT_GLOBAL_ID as ltp,
'NFR' as item,
'IS-LTP-NFR' as sku,
null as partner_pricing,
null as price_type,
BILLABLE_QTY as quantity,
INVOICE as amount
from ltp_nfr_calc