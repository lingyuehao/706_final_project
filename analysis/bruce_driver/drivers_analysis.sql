-- File: analysis/bruce_driver/drivers_analysis.sql

-- This SQL query analyzes driver data to provide insights on average age when obtaining a driver's license
-- and average safety ratings, segmented by gender

SELECT
  gender,
  COUNT(*) AS total_drivers,
  AVG(age_of_DL) AS avg_age_when_got_DL,
  AVG(safety_rating) AS avg_safety_rating
FROM drivers
GROUP BY gender;


-- Categorize drivers based on age when they obtained their driver's license
WITH driver_age_groups AS (
  SELECT
    driver_key,
    gender,
    CASE
      WHEN age_of_DL < 18 THEN 'Early'
      WHEN age_of_DL BETWEEN 18 AND 25 THEN 'Young'
      WHEN age_of_DL BETWEEN 26 AND 40 THEN 'Mid'
      ELSE 'Late'
    END AS dl_age_group,
    safety_rating
  FROM drivers
)
SELECT
  dl_age_group,
  gender,
  COUNT(*) AS total_drivers,
  AVG(safety_rating) AS avg_safety_rating
FROM driver_age_groups
GROUP BY dl_age_group, gender
ORDER BY dl_age_group, gender;


-- Rank drivers within their gender based on safety ratings and calculate difference from average
SELECT
  driver_key,
  gender,
  safety_rating,
  RANK() OVER (PARTITION BY gender ORDER BY safety_rating DESC) AS rank_within_gender,
  AVG(safety_rating) OVER (PARTITION BY gender) AS avg_rating_gender,
  safety_rating - AVG(safety_rating) OVER (PARTITION BY gender) AS diff_from_avg
FROM drivers;