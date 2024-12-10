SELECT
  campaign_id,
  ad_group_id,
  date,
  COALESCE(SUM(CAST(metrics_impressions AS INT64)), 0) AS gender_type_impressions,
  COALESCE(SUM(CAST(metrics_clicks AS INT64)), 0) AS gender_type_clicks,
  COALESCE(SUM(CAST(metrics_cost_micros AS FLOAT64)), 0) AS gender_type_costs,
  ad_group_criterion_gender_type AS gender_type
FROM
  `ida-prd.xone_optout.googleads_gender`
WHERE
  date BETWEEN '2021-07-01'
  AND '2024-06-30'
GROUP BY
  campaign_id,
  ad_group_id,
  date,
  ad_group_criterion_gender_type
ORDER BY
  campaign_id,
  ad_group_id,
  date,
  ad_group_criterion_gender_type
