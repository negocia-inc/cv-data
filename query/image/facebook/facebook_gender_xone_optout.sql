SELECT
  campaign_id,
  date,
  COALESCE(SUM(CAST(impressions AS INT64)), 0) AS gender_type_impressions,
  COALESCE(SUM(CAST(inline_link_clicks AS INT64)), 0) AS gender_type_clicks,
  COALESCE(SUM(CAST(spend AS FLOAT64)), 0) AS gender_type_costs,
  gender AS gender_type
FROM
  `ida-prd.xone_optout.facebook_campaign_gender_age_stats`
WHERE
  date BETWEEN '2021-07-01'
  AND '2024-06-30'
GROUP BY
  campaign_id,
  date,
  gender
ORDER BY
  campaign_id,
  date,
  gender_type