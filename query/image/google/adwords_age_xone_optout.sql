SELECT
  campaign_id,
  ad_group_id,
  date,
  COALESCE(SUM(CAST(impressions AS INT64)), 0) AS age_type_impressions,
  COALESCE(SUM(CAST(clicks AS INT64)), 0) AS age_type_clicks,
  COALESCE(SUM(CAST(cost AS FLOAT64)), 0) AS age_type_costs,
  CASE age_range
    WHEN 'Undetermined' THEN 'AGE_RANGE_UNDETERMINED'
    WHEN '65 or more' THEN 'AGE_RANGE_65_UP'
    WHEN '55-64' THEN 'AGE_RANGE_55_64'
    WHEN '45-54' THEN 'AGE_RANGE_45_54'
    WHEN '35-44' THEN 'AGE_RANGE_35_44'
    WHEN '25-34' THEN 'AGE_RANGE_25_34'
    WHEN '18-24' THEN 'AGE_RANGE_18_24'
END
  AS age_range_type
FROM
  `ida-prd.xone_optout.adwords_age`
WHERE
  date BETWEEN '2021-07-01'
  AND '2022-02-24'
GROUP BY
  campaign_id,
  ad_group_id,
  date,
  age_range_type
ORDER BY
  campaign_id,
  ad_group_id,
  date,
  age_range_type
