-- basic data information for claim
-- total number of claims
SELECT COUNT(*) AS total_claims
FROM stg.claim;

-- view sample claims
SELECT *
FROM stg.claim
LIMIT 10;

-- Check for NULL values in key fields
SELECT 
    COUNT(*) AS total_records,
    COUNT(witness_present_ind) AS witness_present_count,
    COUNT(policy_report_filed_ind) AS policy_report_filed_count,
    COUNT(in_network_bodyshop) AS in_network_bodyshop_count,
    COUNT(accident_key) AS accident_key_count,
    COUNT(policyholder_key) AS policyholder_key_count,
    COUNT(vehicle_key) AS vehicle_key_count,
    COUNT(driver_key) AS driver_key_count
FROM stg.claim;

-- Distribution of claims by witness presence
SELECT 
    witness_present_ind,
    COUNT(*) AS claim_count,
    ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER(), 2) AS percentage
FROM stg.claim
GROUP BY witness_present_ind
ORDER BY claim_count DESC;

-- Distribution of claims by police report filing
SELECT 
    policy_report_filed_ind,
    COUNT(*) AS claim_count,
    ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER(), 2) AS percentage
FROM stg.claim
GROUP BY policy_report_filed_ind
ORDER BY claim_count DESC;

-- Distribution of claims by network bodyshop usage
SELECT 
    in_network_bodyshop,
    COUNT(*) AS claim_count,
    ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER(), 2) AS percentage
FROM stg.claim
GROUP BY in_network_bodyshop
ORDER BY claim_count DESC;

-- Cross-tabulation: witness presence vs police report
SELECT 
    witness_present_ind,
    policy_report_filed_ind,
    COUNT(*) AS claim_count
FROM stg.claim
GROUP BY witness_present_ind, policy_report_filed_ind
ORDER BY witness_present_ind, policy_report_filed_ind;

-- Claims with both witness and police report (there is higher subrogation potential)
SELECT 
    COUNT(*) AS claims_with_both,
    ROUND(COUNT(*) * 100.0 / (SELECT COUNT(*) FROM stg.claim), 2) AS percentage_of_total
FROM stg.claim
WHERE witness_present_ind = 'Yes' 
  AND policy_report_filed_ind = 1;

-- Count claims per accident 
-- identify multi-vehicle accidents
SELECT 
    accident_key,
    COUNT(*) AS claim_count
FROM stg.claim
GROUP BY accident_key
HAVING COUNT(*) > 1
ORDER BY claim_count DESC
LIMIT 20;

-- summary statistics for foreign key fields
SELECT 
    COUNT(DISTINCT accident_key) AS unique_accidents,
    COUNT(DISTINCT policyholder_key) AS unique_policyholders,
    COUNT(DISTINCT vehicle_key) AS unique_vehicles,
    COUNT(DISTINCT driver_key) AS unique_drivers
FROM stg.claim;

