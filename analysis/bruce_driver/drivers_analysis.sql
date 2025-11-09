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