-- File: analysis/bruce_driver/drivers_analysis.sql

-- This SQL query analyzes driver data to provide insights on average age when obtaining a driver's license
-- and average safety ratings, segmented by gender

SELECT gender,
  COUNT(*) AS total_drivers,
  ROUND(AVG("age_of_DL"),1) AS avg_age_when_got_DL,
  ROUND(AVG(safety_rating),2) AS avg_safety_rating
FROM Driver
GROUP BY gender;


-- Categorize drivers based on age when they obtained their driver's license
WITH driver_age_groups AS (
  SELECT
    driver_key,
    gender,
    CASE
      WHEN "age_of_DL" < 18 THEN 'Early'
      WHEN "age_of_DL" BETWEEN 18 AND 25 THEN 'Young'
      WHEN "age_of_DL" BETWEEN 26 AND 40 THEN 'Mid'
      ELSE 'Late'
    END AS dl_age_group,
    safety_rating
  FROM Driver
)
SELECT
  dl_age_group,
  gender,
  COUNT(*) AS total_drivers,
  ROUND(AVG(safety_rating),2) AS avg_safety_rating
FROM driver_age_groups
GROUP BY dl_age_group, gender
ORDER BY dl_age_group, gender;


-- Rank drivers within their gender based on safety ratings and calculate difference from average
-- Compute global stats per gender
WITH gender_stats AS (
    SELECT
        gender,
        COUNT(*) AS total_drivers,
        PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY safety_rating) AS median_gender
    FROM Driver
    GROUP BY gender
),
-- Assign percentile ranks per gender
ranked AS (
    SELECT
        d.*,
        PERCENT_RANK() OVER (PARTITION BY gender ORDER BY safety_rating) AS p_rank
    FROM Driver d
),
-- Tag top and bottom quartile
labeled AS (
    SELECT
        r.driver_key,
        r.gender,
        r.safety_rating,
        g.median_gender,
        CASE
            WHEN r.p_rank <= 0.25 THEN 'bottom_quartile'
            WHEN r.p_rank >= 0.75 THEN 'top_quartile'
            ELSE 'middle_50pct'
        END AS group_label
    FROM ranked r
    JOIN gender_stats g USING (gender)
)
-- Compute medians and compare against each total median
SELECT
    l.gender,
    l.group_label,
    COUNT(*) AS driver_count,
    PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY l.safety_rating) AS median_group,
    MAX(l.median_gender) AS median_total_gender,
    PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY l.safety_rating) - MAX(l.median_gender) AS diff_from_total_median
GROUP BY l.gender, l.group_label
ORDER BY l.gender, l.group_label;