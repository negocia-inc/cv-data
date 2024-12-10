SELECT
  campaign_id,
  ad_group_id,
  date,
  COALESCE(SUM(CAST(impressions AS INT64)), 0) AS age_type_impressions,
  COALESCE(SUM(CAST(clicks AS INT64)), 0) AS age_type_clicks,
  COALESCE(SUM(CAST(cost AS FLOAT64)), 0) AS age_type_costs,
  age AS age_range_type
FROM
  `ht-data-management.hdy_databank_x1dl_export_enabled.ydn_age`
WHERE
  date BETWEEN '2021-07-01'
  AND '2024-06-30'
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