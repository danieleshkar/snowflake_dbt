with global_tenant_history_daily as (
    select * from {{ ref('current_global_tenant_ltp_hwm')}} 
),

meta as (
    select * from {{ ref('ltp_account_meta')}}
),

ltp_daily_itemized as (
    select * from {{ ref('ltp_daily_billing_calc')}} 
),

conversion_tbl_sf as (
    select * from {{ ref('stg_salesforce_conversion_rate_table')}} 
),

-- NEW: total billing per LTP-day across ALL items (not just NFR rows).
-- Used by the activity check below: MSPs paying via Min Commit, plans, premium,
-- IM, ATO, S&T, DMARC, etc. now correctly qualify as "active" for the
-- first-100-NFR-seats-free benefit. Per finance: any organic charge counts.
ltp_total_billing as (
    select
        ltp,
        date_recorded,
        sum(amount) as ltp_total_amount
    from ltp_daily_itemized
    group by ltp, date_recorded
),

tenant_details as (
                    select
                    root as ltp,
                    count(distinct g.tenant_global_id) as total_customers,
                    CASE p.profile_type
                        when 'active' then sum(Active_profiles)
                        when 'license' then sum(licensed_profiles)
                        when 'shared' then 
                                        case 
                                            when sum(SHARED_PROFILES) is null then sum(Active_profiles)
                                            else (sum(Active_profiles) - sum(SHARED_PROFILES))
                                        end
                    end as total_emails,
                    -- from prod_mart.operation.global_tenant_history g
                    from global_tenant_history_daily g
                    left join meta p on g.root = p.tenant_global_id and p.snapshot_date = current_date
                    where
                    {# date_recorded = current_date #}
                    {# and  #}
                    billing_status in ('Active','Active-POC')
                    and approved = true
                    and partner_pricing = false
                    and g.root in (select tenant_global_id from meta where snapshot_date = current_date)
                    -- and g.root = 'US-12122'
                    group by 
                    root,
                    p.profile_type  
                    ),
                    
conversion_tbl as (
                        select  
                        isocode,
                        conversionrate,
                        startdate
                        from conversion_tbl_sf
                        )

select
p.tenant_global_id,
p.tenant_name,
p.account_master_id,
l.item,
l.quantity,
l.amount,
td.total_customers,
td.total_emails,
CASE 
    WHEN td.total_customers > 1 
         OR td.total_emails > 100 
         OR coalesce(tb.ltp_total_amount, 0) > 0   -- FIXED: was `OR l.amount > 0`. Now checks the LTP's TOTAL billing across all items rather than the NFR row alone (which is $0 by design). See QA note in this file's PR.
    THEN CASE 
             WHEN l.quantity > 100 THEN l.quantity - 100 
             ELSE 0 
         END
    ELSE l.quantity
END as billable_qty,
p.currency,
CASE
    WHEN l.item = 'Complete Protect' then ifnull(conversionrate*2,2)
    WHEN l.item = 'Email Protect' then ifnull(conversionrate*1,1)
end as nfr_rate,
billable_qty*nfr_rate as invoice
from meta p
left join ltp_daily_itemized l on p.tenant_global_id = l.ltp and p.snapshot_date = l.date_recorded
left join tenant_details td on p.tenant_global_id = td.ltp
left join conversion_tbl c on p.currency = c.isocode and l.date_recorded = dateadd(day, 1, date(c.startdate))
left join ltp_total_billing tb on p.tenant_global_id = tb.ltp and p.snapshot_date = tb.date_recorded   -- NEW: join to LTP-wide totals (one row per ltp/date)

where
snapshot_date = current_date
and ltp_type = 'msp'
and registration_date <= dateadd(day, -90, current_date)
and l.partner_pricing = true
and billable_qty > 0
and l.quantity > 1