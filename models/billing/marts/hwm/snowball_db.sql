{{
    config(
        materialized='incremental',
        incremental_strategy='delete+insert',
        unique_key=['billing_date','first_layer_id','second_layer_id','third_layer_id',
                    'fourth_layer_id','fifth_layer_id','item','sku','is_nfr'],
        on_schema_change='fail'
    )
}}

-- snowball_db — translates SP_INSERT_SNOWBALL_DB() to dbt
-- Each daily run replaces today's rows only (delete+insert on billing_date)

with sltp_billing as (
    select * from {{ ref('sltp_daily_itemized_new_billing_tbl') }}
),

ltp_account_meta as (
    select * from {{ source('ltp_pricing_table_new_billing_calc','LTP_ACCOUNT_META') }}
),

ltp_pricing as (
    select * from {{ source('ltp_pricing_table_new_billing_calc','LTP_PRICING_TBL_UNPIVOT') }}
),


-- today's billing rows only
billing as (
    select *
    from sltp_billing
    where billing_date = current_date
),

-- per-tenant meta — deduped to one row per (snapshot_date, tenant)
ltp_meta_deduped as (
    select
        snapshot_date,
        tenant_global_id,
        max(ltp_type)          as ltp_type,
        max(registration_date) as registration_date,
        max(account_master_id) as account_master_id
    from ltp_account_meta
    group by snapshot_date, tenant_global_id
),

-- max price per (snapshot_date, tenant, sku, is_nfr)
ltp_pricing_max as (
    select
        snapshot_date,
        tenant_global_id,
        sku,
        is_nfr,
        max(price) as max_price
    from ltp_pricing
    group by snapshot_date, tenant_global_id, sku, is_nfr
),

-- LTP roll-up totals — populates total_ltp_billed_qty and ltp_sku_mrr
billing_summary as (
    select
        billing_date,
        first_layer_id,
        item,
        sku,
        partner_pricing,
        sum(quantity) as total_quantity,
        sum(amount)   as total_amount
    from billing
    group by billing_date, first_layer_id, item, sku, partner_pricing
)


select
    b.billing_date,
    b.first_layer_id,
    coalesce(b.second_layer_id, '') as second_layer_id,
    coalesce(b.third_layer_id,  '') as third_layer_id,
    coalesce(b.fourth_layer_id, '') as fourth_layer_id,
    coalesce(b.fifth_layer_id,  '') as fifth_layer_id,

    -- global_tenant_id = deepest non-empty layer (matches the procedure exactly)
    coalesce(
        nullif(b.fifth_layer_id,  ''),
        nullif(b.fourth_layer_id, ''),
        nullif(b.third_layer_id,  ''),
        nullif(b.second_layer_id, ''),
        b.first_layer_id
    ) as global_tenant_id,

    b.item,
    b.sku,
    b.partner_pricing as is_nfr,

    m.ltp_type,
    m.registration_date,
    m.account_master_id,

    p.max_price,

    b.quantity        as billed_qty,
    bs.total_quantity as total_ltp_billed_qty,
    b.amount          as tenant_mrr,
    bs.total_amount   as ltp_sku_mrr

from billing b

left join ltp_meta_deduped m
    on  b.billing_date   = m.snapshot_date
    and b.first_layer_id = m.tenant_global_id

left join ltp_pricing_max p
    on  b.billing_date             = p.snapshot_date
    and b.first_layer_id           = p.tenant_global_id
    and b.item                     = p.sku
    and b.partner_pricing::boolean = p.is_nfr::boolean

left join billing_summary bs
    on  b.billing_date    = bs.billing_date
    and b.first_layer_id  = bs.first_layer_id
    and b.item            = bs.item
    and b.sku             = bs.sku
    and b.partner_pricing = bs.partner_pricing