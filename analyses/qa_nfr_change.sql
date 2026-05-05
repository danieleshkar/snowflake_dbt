WITH ltp_totals AS (
    SELECT ltp, SUM(amount) AS total_paid
    FROM {{ ref('ltp_daily_billing_calc') }}
    GROUP BY ltp
),
td AS (
    SELECT
        g.root AS ltp,
        COUNT(DISTINCT g.tenant_global_id) AS total_customers,
        CASE p.profile_type
            WHEN 'active'  THEN SUM(active_profiles)
            WHEN 'license' THEN SUM(licensed_profiles)
            WHEN 'shared'  THEN COALESCE(SUM(active_profiles) - SUM(shared_profiles), SUM(active_profiles))
        END AS total_emails
    FROM {{ ref('current_global_tenant_ltp_hwm') }} g
    LEFT JOIN {{ ref('ltp_account_meta') }} p
        ON g.root = p.tenant_global_id AND p.snapshot_date = current_date
    WHERE billing_status IN ('Active','Active-POC')
      AND approved = TRUE
      AND partner_pricing = FALSE
    GROUP BY g.root, p.profile_type
),
compare AS (
    SELECT
        l.ltp,
        l.item,
        l.quantity,
        l.amount      AS row_amount,
        lt.total_paid AS ltp_total_paid,
        td.total_customers,
        td.total_emails,
        CASE WHEN td.total_customers > 1 OR td.total_emails > 100 OR l.amount > 0
             THEN CASE WHEN l.quantity > 100 THEN l.quantity - 100 ELSE 0 END
             ELSE l.quantity
        END AS billable_qty_old,
        CASE WHEN td.total_customers > 1 OR td.total_emails > 100 OR lt.total_paid > 0
             THEN CASE WHEN l.quantity > 100 THEN l.quantity - 100 ELSE 0 END
             ELSE l.quantity
        END AS billable_qty_new
    FROM {{ ref('ltp_daily_billing_calc') }} l
    JOIN {{ ref('ltp_account_meta') }} p
        ON p.tenant_global_id = l.ltp AND p.snapshot_date = l.date_recorded
    LEFT JOIN ltp_totals lt ON lt.ltp = l.ltp
    LEFT JOIN td             ON td.ltp = l.ltp
    WHERE p.snapshot_date = current_date
      AND p.ltp_type = 'msp'
      AND p.registration_date <= dateadd(day, -90, current_date)
      AND l.partner_pricing = TRUE
      AND l.quantity > 1
      AND l.item IN ('Complete Protect','Email Protect')
)
SELECT *
FROM compare
WHERE billable_qty_old <> billable_qty_new
ORDER BY row_amount DESC