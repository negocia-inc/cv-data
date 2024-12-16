SELECT
  campaign_id,
  adgroup_id as ad_group_id,
  date,
  COALESCE(SUM(CAST(impression AS INT64)), 0) AS age_type_impressions,
  COALESCE(SUM(CAST(clicks AS INT64)), 0) AS age_type_clicks,
  COALESCE(SUM(CAST(cost AS FLOAT64)), 0) AS age_type_costs,
  age AS age_range_type
FROM
  `ida-prd.xone_optout.lap_ad_group_daily_age_report`
WHERE
  date BETWEEN '2022-06-29'
  AND '2024-06-30'
GROUP BY
  campaign_id,
  adgroup_id,
  date,
  age
ORDER BY
  campaign_id,
  adgroup_id,
  date,
  age