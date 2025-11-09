-- total accidents and basic stats
SELECT 
    COUNT(*) AS total_accidents,
    COUNT(DISTINCT accident_site) AS unique_sites,
    COUNT(DISTINCT accident_type) AS unique_types
FROM stg.accident;

-- sample of accident data
SELECT *
FROM stg.accident
LIMIT 10;

-- Distribution by accident type
SELECT 
    accident_type,
    COUNT(*) AS accident_count,
    ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER(), 2) AS percentage
FROM stg.accident
GROUP BY accident_type
ORDER BY accident_count DESC;

-- Distribution by accident site
SELECT 
    accident_site,
    COUNT(*) AS accident_count,
    ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER(), 2) AS percentage
FROM stg.accident
GROUP BY accident_site
ORDER BY accident_count DESC;

-- Identify multi-vehicle accidents
SELECT 
    accident_type,
    COUNT(*) AS multi_vehicle_count,
    ROUND(COUNT(*) * 100.0 / (SELECT COUNT(*) FROM stg.accident), 2) AS percentage_of_all
FROM stg.accident
WHERE accident_type LIKE '%multi_vehicle%'
GROUP BY accident_type
ORDER BY multi_vehicle_count DESC;

-- Cross-analysis: Where do multi-vehicle accidents happen?
SELECT 
    accident_site,
    accident_type,
    COUNT(*) AS accident_count
FROM stg.accident
GROUP BY accident_site, accident_type
ORDER BY accident_count DESC
LIMIT 20;

-- Combine accident characteristics with claim indicators
SELECT 
    a.accident_key,
    a.accident_site,
    a.accident_type,
    c.witness_present_ind,
    c.policy_report_filed_ind,
    c.in_network_bodyshop
FROM stg.accident a
INNER JOIN stg.claim c ON a.accident_key = c.accident_key
LIMIT 20;

-- High subrogation potential: Multi-vehicle + witnesses + police report
SELECT 
    a.accident_type,
    a.accident_site,
    COUNT(*) AS high_potential_claims,
    ROUND(COUNT(*) * 100.0 / (SELECT COUNT(*) FROM stg.claim), 2) AS percentage_of_claims
FROM stg.accident a
INNER JOIN stg.claim c ON a.accident_key = c.accident_key
WHERE a.accident_type LIKE '%multi_vehicle%'
  AND c.witness_present_ind = 'Yes'
  AND c.policy_report_filed_ind = 1
GROUP BY a.accident_type, a.accident_site
ORDER BY high_potential_claims DESC;


-- Compare subrogation indicators across accident types
SELECT 
    a.accident_type,
    COUNT(*) AS total_claims,
    SUM(CASE WHEN c.witness_present_ind = 'Yes' THEN 1 ELSE 0 END) AS claims_with_witness,
    SUM(CASE WHEN c.policy_report_filed_ind = 1 THEN 1 ELSE 0 END) AS claims_with_police_report,
    SUM(CASE WHEN c.witness_present_ind = 'Yes' AND c.policy_report_filed_ind = 1 THEN 1 ELSE 0 END) AS claims_with_both,
    ROUND(SUM(CASE WHEN c.witness_present_ind = 'Yes' AND c.policy_report_filed_ind = 1 THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2) AS subrogation_potential_pct
FROM stg.accident a
INNER JOIN stg.claim c ON a.accident_key = c.accident_key
GROUP BY a.accident_type
ORDER BY subrogation_potential_pct DESC;

-- Compare subrogation indicators across accident sites
SELECT 
    a.accident_site,
    COUNT(*) AS total_claims,
    SUM(CASE WHEN c.witness_present_ind = 'Yes' THEN 1 ELSE 0 END) AS claims_with_witness,
    SUM(CASE WHEN c.policy_report_filed_ind = 1 THEN 1 ELSE 0 END) AS claims_with_police_report,
    SUM(CASE WHEN c.witness_present_ind = 'Yes' AND c.policy_report_filed_ind = 1 THEN 1 ELSE 0 END) AS claims_with_both,
    ROUND(SUM(CASE WHEN c.witness_present_ind = 'Yes' AND c.policy_report_filed_ind = 1 THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2) AS subrogation_potential_pct
FROM stg.accident a
INNER JOIN stg.claim c ON a.accident_key = c.accident_key
GROUP BY a.accident_site
ORDER BY subrogation_potential_pct DESC;

-- a comprehensive view of accidents with subrogation potential
SELECT 
    a.accident_key,
    a.accident_site,
    a.accident_type,
    COUNT(c.accident_key) AS claim_count,
    SUM(CASE WHEN c.witness_present_ind = 'Yes' THEN 1 ELSE 0 END) AS witness_count,
    SUM(CASE WHEN c.policy_report_filed_ind = 1 THEN 1 ELSE 0 END) AS police_report_count,
    CASE 
        WHEN a.accident_type LIKE '%multi_vehicle%' 
         AND SUM(CASE WHEN c.witness_present_ind = 'Yes' THEN 1 ELSE 0 END) > 0
         AND SUM(CASE WHEN c.policy_report_filed_ind = 1 THEN 1 ELSE 0 END) > 0
        THEN 'High'
        WHEN a.accident_type LIKE '%multi_vehicle%' THEN 'Medium'
        ELSE 'Low'
    END AS subrogation_priority
FROM stg.accident a
INNER JOIN stg.claim c ON a.accident_key = c.accident_key
GROUP BY a.accident_key, a.accident_site, a.accident_type
ORDER BY subrogation_priority, claim_count DESC;