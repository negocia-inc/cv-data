SELECT
  date,
  id AS campaign_id,
  COALESCE(SUM(CAST(impressions AS INT64)), 0) AS gender_type_impressions,
  COALESCE(SUM(CAST(url_clicks AS INT64)), 0) AS gender_type_clicks,
  COALESCE(SUM(CAST(billed_charge_local_micro AS FLOAT64)), 0) AS gender_type_costs,
  segment_name AS gender_type
FROM
  `ida-prd.xone_optout.twitter_campaign_gender_stats`
WHERE
  date BETWEEN '2021-07-01'
  AND '2024-06-30'
GROUP BY
  campaign_id,
  date,
  gender_type
ORDER BY
  id,
  date,
  gender_type