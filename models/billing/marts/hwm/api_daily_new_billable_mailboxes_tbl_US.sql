with daily as (
    select * from 
    {{ ref('daily_new_billable_mailboxes_tbl_US') }}
)
    
    SELECT
    record_date,
    billing_date,
    tenant_global_id,
    tenant_name,
    parent_global_id,
    parent_name,
    licensed_profiles,
    active_profiles,
    shared_profiles,
    billable_profiles,
    plan_id,
    plan_expiry_date,
    trial_plan_expiry_date,
    registration_date,
    parent_type,
    TO_JSON(billable_items) as billable_items,
    TO_JSON(active_add_ons) as active_add_ons,
    -- high_water_mark,
    -- non_profit_flag,
    not_for_resale_flag as IS_PARTNER,
    -- price_per_mailbox,
    tree_key
    FROM daily
    WHERE billable_profiles IS not NULL