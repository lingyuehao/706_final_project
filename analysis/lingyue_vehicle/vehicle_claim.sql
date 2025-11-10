-- 1) By vehicle category & color: subrogation rate, avg payout (subro vs non-subro), claim-to-value ratio, volume
SELECT
  v.vehicle_category,
  v.vehicle_color,
  COUNT(*) AS num_claims,
  ROUND(AVG(CASE WHEN c.subrogation > 0 THEN 1 ELSE 0 END)::numeric, 4) AS subro_rate,
  ROUND(AVG(CASE WHEN c.subrogation > 0 THEN c.claim_est_payout END)::numeric, 2) AS avg_payout_subro,
  ROUND(AVG(CASE WHEN c.subrogation = 0 THEN c.claim_est_payout END)::numeric, 2) AS avg_payout_non,
  ROUND(AVG(c.claim_est_payout / NULLIF(v.vehicle_price,0))::numeric, 4) AS avg_claim_to_value_ratio
FROM stg.vehicle v
JOIN stg.claim   c ON c.vehicle_key = v.vehicle_key
GROUP BY v.vehicle_category, v.vehicle_color
ORDER BY v.vehicle_category, subro_rate DESC;


-- 2) By vehicle price band: which price tiers have higher subrogation propensity and heavier payouts
WITH priced AS (
  SELECT
    v.vehicle_key,
    CASE
      WHEN v.vehicle_price < 15000 THEN '<15k'
      WHEN v.vehicle_price >= 15000 AND v.vehicle_price < 25000 THEN '15k–25k'
      WHEN v.vehicle_price >= 25000 AND v.vehicle_price < 40000 THEN '25k–40k'
      ELSE '≥40k'
    END AS price_band,
    v.vehicle_price,
    c.subrogation,
    c.claim_est_payout
  FROM stg.vehicle v
  JOIN stg.claim c ON c.vehicle_key = v.vehicle_key
)
SELECT
  price_band,
  COUNT(*) AS num_claims,
  ROUND(AVG(CASE WHEN subrogation > 0 THEN 1 ELSE 0 END)::numeric, 4) AS subro_rate,
  ROUND(AVG(claim_est_payout)::numeric, 2) AS avg_payout,
  ROUND(AVG(claim_est_payout / NULLIF(vehicle_price,0))::numeric, 4) AS avg_claim_to_value_ratio
FROM priced
GROUP BY price_band
ORDER BY
  CASE price_band WHEN '<15k' THEN 1 WHEN '15k–25k' THEN 2 WHEN '25k–40k' THEN 3 ELSE 4 END;


-- 3) By mileage band: does higher mileage correlate with more/less subrogation and different liability %
WITH miles AS (
  SELECT
    CASE
      WHEN v.vehicle_mileage < 10_000 THEN '<10k'
      WHEN v.vehicle_mileage >= 10_000 AND v.vehicle_mileage < 50_000 THEN '10k–50k'
      WHEN v.vehicle_mileage >= 50_000 AND v.vehicle_mileage < 100_000 THEN '50k–100k'
      ELSE '≥100k'
    END AS mileage_band,
    c.subrogation,
    c.liab_prct,
    c.claim_est_payout
  FROM stg.vehicle v
  JOIN stg.claim   c ON c.vehicle_key = v.vehicle_key
)
SELECT
  mileage_band,
  COUNT(*) AS num_claims,
  ROUND(AVG(CASE WHEN subrogation > 0 THEN 1 ELSE 0 END)::numeric,4) AS subro_rate,
  ROUND(AVG(CASE WHEN subrogation > 0 THEN liab_prct END)::numeric,2) AS avg_liability_if_subro,
  ROUND(AVG(claim_est_payout)::numeric,2) AS avg_payout
FROM miles
GROUP BY mileage_band
ORDER BY
  CASE mileage_band WHEN '<10k' THEN 1 WHEN '10k–50k' THEN 2 WHEN '50k–100k' THEN 3 ELSE 4 END;

-- 4) Which witness/police indicators within each vehicle category lift subrogation rate
SELECT
  v.vehicle_category,
  c.witness_present_ind,
  c.policy_report_filed_ind,
  COUNT(*) AS num_claims,
  ROUND(AVG(CASE WHEN c.subrogation > 0 THEN 1 ELSE 0 END)::numeric,4) AS subro_rate
FROM stg.vehicle v
JOIN stg.claim   c ON c.vehicle_key = v.vehicle_key
GROUP BY v.vehicle_category, c.witness_present_ind, c.policy_report_filed_ind
ORDER BY v.vehicle_category, subro_rate DESC, num_claims DESC;

-- 5) Interaction of category × channel: where does subrogation actually happen more often  
SELECT
  v.vehicle_category,
  c.channel,
  COUNT(*) AS num_claims,
  ROUND(AVG(CASE WHEN c.subrogation > 0 THEN 1 ELSE 0 END)::numeric,4) AS subro_rate,
  ROUND(AVG(c.claim_est_payout)::numeric,2) AS avg_payout
FROM stg.vehicle v
JOIN stg.claim   c ON c.vehicle_key = v.vehicle_key
GROUP BY v.vehicle_category, c.channel
HAVING COUNT(*) >= 50    
ORDER BY v.vehicle_category, subro_rate DESC, num_claims DESC;
