SELECT
  campaign_id,
  adgroup_id AS ad_group_id,
  date,
  COALESCE(SUM(CAST(impression AS INT64)), 0) AS gender_type_impressions,
  COALESCE(SUM(CAST(clicks AS INT64)), 0) AS gender_type_clicks,
  COALESCE(SUM(CAST(cost AS FLOAT64)), 0) AS gender_type_costs,
  gender AS gender_type
FROM
  `ht-data-management.hdy_databank_x1dl_export_enabled.lap_ad_group_daily_gender_report`
WHERE
  date BETWEEN '2022-06-29'
  AND '2024-06-30'
GROUP BY
  campaign_id,
  adgroup_id,
  date,
  gender
ORDER BY
  campaign_id,
  adgroup_id,
  date,
  gender