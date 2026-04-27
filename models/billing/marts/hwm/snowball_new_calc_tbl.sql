{{
    config(
        materialized='incremental',
        incremental_strategy='delete+insert',
        unique_key=['snapshot_date','first_layer_id','second_layer_id','third_layer_id',
                    'fourth_layer_id','fifth_layer_id','global_tenant_id','item','sku','is_nfr'],
        on_schema_change='fail'
    )
}}

-- snowball_new_calc_tbl — translates SP_INSERT_SNOWBALL_NEW_CALC() to dbt
-- Each daily run replaces today's snapshot only (delete+insert on snapshot_date)
--
-- Two fixes vs the procedure (search "FIX" below):
--   FIX 1: COALESCE(SUM(...), 0) in pivot_billing -> source NULLs no longer propagate
--   FIX 2: COALESCE(qty, 0) in bucket CASE -> NULLs no longer fall through to
--          'Billing Rate Change' (the Apr 25/26 hidden-churn bug)

{% set today = "current_date" %}
{% set prior_dt %}
    case when day({{ today }}) = 1
         then date_trunc('month', dateadd(month, -1, {{ today }}))
         else date_trunc('month', {{ today }})
    end
{% endset %}


with snowball_db as (
    select * from {{ ref('snowball_db') }}
),

ltp_pricing as (
    select * from {{ source('ltp_pricing_table_new_billing_calc','LTP_PRICING_TBL_UNPIVOT') }}
),

ltp_account_meta as (
    select * from {{ source('ltp_pricing_table_new_billing_calc','LTP_ACCOUNT_META') }}
),

fx_rates as (
    select * from {{ ref('stg_salesforce_conversion_rate_table') }}
),


-- prior-vs-current anchor dates
date_params as (
    select
        {{ today }}::date as current_dt,
        ({{ prior_dt }})::date as prior_dt
),

-- pivot prior-vs-current per (tenant, layers, item, sku, is_nfr)
-- FIX 1: each SUM is wrapped in COALESCE(..., 0) so source NULLs don't propagate
pivot_billing as (
    select
        global_tenant_id, first_layer_id, second_layer_id, third_layer_id,
        fourth_layer_id, fifth_layer_id, item, sku, is_nfr,
        coalesce(sum(case when billing_date = (select prior_dt   from date_params) then billed_qty           else 0 end), 0) as billed_qty_prior,
        coalesce(sum(case when billing_date = (select current_dt from date_params) then billed_qty           else 0 end), 0) as billed_qty_current,
        coalesce(sum(case when billing_date = (select prior_dt   from date_params) then tenant_mrr           else 0 end), 0) as tenant_mrr_prior,
        coalesce(sum(case when billing_date = (select current_dt from date_params) then tenant_mrr           else 0 end), 0) as tenant_mrr_current,
        coalesce(sum(case when billing_date = (select prior_dt   from date_params) then tenant_mrr * 12      else 0 end), 0) as tenant_arr_prior,
        coalesce(sum(case when billing_date = (select current_dt from date_params) then tenant_mrr * 12      else 0 end), 0) as tenant_arr_current,
        coalesce(sum(case when billing_date = (select prior_dt   from date_params) then total_ltp_billed_qty else 0 end), 0) as total_ltp_qty_prior,
        coalesce(sum(case when billing_date = (select current_dt from date_params) then total_ltp_billed_qty else 0 end), 0) as total_ltp_qty_current
    from snowball_db
    where billing_date in ((select prior_dt from date_params), (select current_dt from date_params))
    group by 1,2,3,4,5,6,7,8,9
),

meta_current as (
    select global_tenant_id, item, sku, is_nfr,
        max(ltp_type) as ltp_type,
        max(registration_date) as registration_date,
        max(account_master_id) as account_master_id,
        max(max_price) as max_price
    from snowball_db
    where billing_date = (select current_dt from date_params)
    group by 1,2,3,4
),

meta_prior as (
    select global_tenant_id, item, sku, is_nfr,
        max(ltp_type) as ltp_type,
        max(registration_date) as registration_date,
        max(account_master_id) as account_master_id,
        max(max_price) as max_price
    from snowball_db
    where billing_date = (select prior_dt from date_params)
    group by 1,2,3,4
),

-- point-in-time tier-1 price for each tenant/sku
effective_rates as (
    select
        tenant_global_id as first_layer_id,
        sku as item,
        min(price) as actual_effective_rate
    from ltp_pricing
    where snapshot_date = (
        select max(snapshot_date) from ltp_pricing
        where snapshot_date <= (select current_dt from date_params)
    )
    group by 1, 2
),

-- per-tenant qty totals across all SKUs (for "did the tenant churn entirely?")
tenant_totals as (
    select
        global_tenant_id,
        sum(billed_qty_prior)   as tenant_qty_prior_total,
        sum(billed_qty_current) as tenant_qty_curr_total
    from pivot_billing
    group by 1
),

-- FX point-in-time
fx_prior as (
    select isocode as currency, conversionrate as prior_rate
    from fx_rates
    where startdate = (
        select max(startdate) from fx_rates
        where startdate <= (select prior_dt from date_params)
    )
),

fx_current as (
    select isocode as currency, conversionrate as current_rate
    from fx_rates
    where startdate = (
        select max(startdate) from fx_rates
        where startdate <= (select current_dt from date_params)
    )
),

-- tenant billing currency (point-in-time)
ltp_currency as (
    select
        tenant_global_id as first_layer_id,
        max(upper(trim(currency))) as currency
    from ltp_account_meta
    where snapshot_date = (
        select max(snapshot_date) from ltp_account_meta
        where snapshot_date <= (select current_dt from date_params)
    )
    group by 1
),

-- channel migration flags — same (tenant, item, sku) under different first_layer_id
channel_flags as (
    select pr.global_tenant_id, pr.item, pr.sku, true as channel_changed
    from (
        select distinct global_tenant_id, item, sku, first_layer_id
        from snowball_db
        where billing_date = (select prior_dt from date_params)
    ) pr
    join (
        select distinct global_tenant_id, item, sku, first_layer_id
        from snowball_db
        where billing_date = (select current_dt from date_params)
    ) cu
        on  pr.global_tenant_id = cu.global_tenant_id
        and pr.item             = cu.item
        and pr.sku              = cu.sku
    where pr.first_layer_id <> cu.first_layer_id
),

-- pivot_billing + all the lookups
enriched as (
    select
        p.global_tenant_id, p.first_layer_id, p.second_layer_id, p.third_layer_id,
        p.fourth_layer_id, p.fifth_layer_id, p.item, p.sku, p.is_nfr,

        coalesce(mc.ltp_type,          mp.ltp_type)          as ltp_type,
        coalesce(mc.registration_date, mp.registration_date) as registration_date,
        coalesce(mc.account_master_id, mp.account_master_id) as account_master_id,
        coalesce(mc.max_price,         mp.max_price)         as max_price,

        p.billed_qty_prior, p.billed_qty_current,
        p.tenant_mrr_prior, p.tenant_mrr_current,
        p.tenant_arr_prior, p.tenant_arr_current,
        p.total_ltp_qty_prior, p.total_ltp_qty_current,
        tt.tenant_qty_prior_total, tt.tenant_qty_curr_total,

        er.actual_effective_rate,

        coalesce(lc.currency,    'USD') as billing_currency,
        coalesce(fp.prior_rate,   1)    as prior_fx_rate,
        coalesce(fc.current_rate, 1)    as current_fx_rate,
        coalesce(cf.channel_changed, false) as channel_changed
    from pivot_billing p
    left join meta_current   mc on  p.global_tenant_id = mc.global_tenant_id
                                and p.item             = mc.item
                                and equal_null(p.sku,    mc.sku)
                                and equal_null(p.is_nfr, mc.is_nfr)
    left join meta_prior     mp on  p.global_tenant_id = mp.global_tenant_id
                                and p.item             = mp.item
                                and equal_null(p.sku,    mp.sku)
                                and equal_null(p.is_nfr, mp.is_nfr)
    left join tenant_totals  tt on  p.global_tenant_id = tt.global_tenant_id
    left join effective_rates er on p.first_layer_id   = er.first_layer_id
                                and p.item             = er.item
    left join ltp_currency   lc on  p.first_layer_id   = lc.first_layer_id
    left join fx_prior       fp on  coalesce(lc.currency, 'USD') = fp.currency
    left join fx_current     fc on  coalesce(lc.currency, 'USD') = fc.currency
    left join channel_flags  cf on  p.global_tenant_id = cf.global_tenant_id
                                and p.item             = cf.item
                                and p.sku              = cf.sku
),

-- basic deltas + USD conversions + implied rate
with_calcs as (
    select *,
        billed_qty_current - billed_qty_prior as net_qty_change,
        tenant_mrr_current - tenant_mrr_prior as net_mrr_change,
        tenant_arr_current - tenant_arr_prior as net_arr_change,

        tenant_mrr_prior   * prior_fx_rate    as tenant_mrr_usd_prior,
        tenant_mrr_current * current_fx_rate  as tenant_mrr_usd_current,
        tenant_arr_prior   * prior_fx_rate    as tenant_arr_usd_prior,
        tenant_arr_current * current_fx_rate  as tenant_arr_usd_current,

        (tenant_mrr_prior * current_fx_rate
         - tenant_mrr_prior * prior_fx_rate) * 12 as fx_impact_arr_usd,

        case
            when billed_qty_current <> 0 then tenant_mrr_current / nullif(billed_qty_current, 0)
            when billed_qty_prior   <> 0 then tenant_mrr_prior   / nullif(billed_qty_prior,   0)
            else null
        end as implied_effective_rate
    from enriched
),

-- assign each row an arr_bucket
-- FIX 2: every quantity in the CASE is wrapped in COALESCE(..., 0) so NULLs don't
-- fall through silently to 'Billing Rate Change' (the Apr 25/26 bug pattern)
with_buckets as (
    select *,
        case
            when coalesce(tenant_qty_prior_total, 0) = 0 and coalesce(billed_qty_current, 0) > 0
                then 'New'
            when coalesce(tenant_qty_curr_total,  0) = 0 and coalesce(tenant_qty_prior_total, 0) > 0
                then 'Churn'
            when channel_changed = true
                and ((coalesce(billed_qty_prior,   0) > 0 and coalesce(billed_qty_current, 0) = 0)
                  or (coalesce(billed_qty_prior,   0) = 0 and coalesce(billed_qty_current, 0) > 0))
                then 'Change of Channel'
            when coalesce(tenant_qty_prior_total, 0) > 0
                and coalesce(billed_qty_current, 0) > coalesce(billed_qty_prior, 0)
                then 'Upsell'
            when coalesce(tenant_qty_prior_total, 0) > 0
                and coalesce(billed_qty_current, 0) < coalesce(billed_qty_prior, 0)
                then 'Downsell'
            when coalesce(billed_qty_current, 0) = coalesce(billed_qty_prior, 0)
                and coalesce(tenant_mrr_current, 0) = coalesce(tenant_mrr_prior, 0)
                and billing_currency <> 'USD'
                then 'FX Impact'
            when coalesce(billed_qty_current, 0) = coalesce(billed_qty_prior, 0)
                and coalesce(tenant_mrr_current, 0) = coalesce(tenant_mrr_prior, 0)
                then 'No Change'
            else 'Billing Rate Change'
        end as arr_bucket
    from with_calcs
),

-- volume vs rate split
with_impacts as (
    select *,
        coalesce(nullif(actual_effective_rate, 0), implied_effective_rate, 0) as effective_rate,
        case
            when arr_bucket in ('No Change','FX Impact') then 0
            when arr_bucket = 'New'   then  billed_qty_current
                                          * coalesce(nullif(actual_effective_rate, 0), implied_effective_rate, 0)
            when arr_bucket = 'Churn' then -billed_qty_prior
                                          * coalesce(nullif(actual_effective_rate, 0), implied_effective_rate, 0)
            when arr_bucket in ('Upsell','Downsell','Change of Channel')
                then (billed_qty_current - billed_qty_prior)
                     * coalesce(nullif(actual_effective_rate, 0), implied_effective_rate, 0)
            else 0
        end as volume_impact_mrr
    from with_buckets
)


select
    (select current_dt from date_params) as snapshot_date,
    (select prior_dt   from date_params) as prior_dt,
    (select current_dt from date_params) as current_dt,

    first_layer_id, second_layer_id, third_layer_id, fourth_layer_id, fifth_layer_id,
    global_tenant_id, item, sku, is_nfr,
    ltp_type, registration_date, account_master_id, max_price,
    billing_currency, prior_fx_rate, current_fx_rate,

    billed_qty_prior, billed_qty_current,
    tenant_qty_prior_total, tenant_qty_curr_total, net_qty_change,

    tenant_mrr_prior, tenant_mrr_current,
    tenant_arr_prior, tenant_arr_current,
    net_mrr_change, net_arr_change,

    tenant_mrr_usd_prior, tenant_mrr_usd_current,
    tenant_arr_usd_prior, tenant_arr_usd_current,
    tenant_mrr_usd_current - tenant_mrr_usd_prior as net_mrr_change_usd,
    tenant_arr_usd_current - tenant_arr_usd_prior as net_arr_change_usd,
    fx_impact_arr_usd,

    total_ltp_qty_prior, total_ltp_qty_current,
    actual_effective_rate, implied_effective_rate, effective_rate,
    channel_changed, arr_bucket,

    volume_impact_mrr,
    volume_impact_mrr * 12                             as volume_impact_arr,
    net_mrr_change - volume_impact_mrr                 as billing_rate_change_mrr,
    (net_mrr_change - volume_impact_mrr) * 12          as billing_rate_change_arr,

    volume_impact_mrr * prior_fx_rate                  as volume_impact_mrr_usd,
    volume_impact_mrr * 12 * prior_fx_rate             as volume_impact_arr_usd,
    (net_mrr_change - volume_impact_mrr) * current_fx_rate      as billing_rate_change_mrr_usd,
    (net_mrr_change - volume_impact_mrr) * 12 * current_fx_rate as billing_rate_change_arr_usd

from with_impacts
where not (
    billed_qty_prior   = 0 and billed_qty_current   = 0
    and tenant_mrr_prior = 0 and tenant_mrr_current = 0
)