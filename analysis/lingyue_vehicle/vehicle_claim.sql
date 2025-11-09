-- 1) Overall volume with average payout and average liability across all claims
SELECT
  COUNT(*)                          AS total_claims,
  AVG(claim_est_payout)             AS avg_payout,
  AVG(liab_prct)                    AS avg_liability
FROM stg.claim;

-- 2) Subrogation rate and average payout by vehicle category (join on vehicle_key)
SELECT
  v.vehicle_category,
  COUNT(*)                                                   AS claims,
  AVG(c.claim_est_payout)                                    AS avg_payout,
  AVG(CASE WHEN c.subrogation IS NOT NULL AND c.subrogation > 0 THEN 1 ELSE 0 END)::float AS subro_rate
FROM stg.claim   AS c
JOIN stg.vehicle AS v
  ON c.vehicle_key = v.vehicle_key
GROUP BY v.vehicle_category
ORDER BY claims DESC;

-- 3) Trend by vehicle model year: how payouts vary with vehicle_made_year
SELECT
  v.vehicle_made_year,
  COUNT(*)                AS claims,
  AVG(c.claim_est_payout) AS avg_payout
FROM stg.vehicle v
JOIN stg.claim   c ON c.vehicle_key = v.vehicle_key
GROUP BY v.vehicle_made_year
ORDER BY v.vehicle_made_year DESC;

-- 4) Do higher-priced vehicles have higher claim payouts, and does the reporting channel matter?
SELECT
  v.vehicle_category,
  c.channel,
  ROUND(AVG(v.vehicle_price)::numeric, 2)       AS avg_vehicle_price,
  ROUND(AVG(c.claim_est_payout)::numeric, 2)    AS avg_payout,
  ROUND(AVG(c.liab_prct)::numeric, 2)           AS avg_liability,
  COUNT(*)                                      AS num_claims
FROM stg.vehicle v
JOIN stg.claim   c ON c.vehicle_key = v.vehicle_key
GROUP BY v.vehicle_category, c.channel
ORDER BY v.vehicle_category, avg_payout DESC;

-- 5) How do claim channels differ across vehicle categories?
SELECT
  v.vehicle_category,
  c.channel,
  COUNT(*) AS num_claims,
  ROUND(COUNT(*)::numeric / SUM(COUNT(*)) OVER (PARTITION BY v.vehicle_category) * 100, 2)
    AS channel_share_pct
FROM stg.vehicle v
JOIN stg.claim c 
  ON v.vehicle_key = c.vehicle_key
GROUP BY v.vehicle_category, c.channel
ORDER BY v.vehicle_category, channel_share_pct DESC;
