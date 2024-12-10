SELECT
  date,
  id AS campaign_id,
  COALESCE(SUM(CAST(impressions AS INT64)), 0) AS age_type_impressions,
  COALESCE(SUM(CAST(url_clicks AS INT64)), 0) AS age_type_clicks,
  COALESCE(SUM(CAST(billed_charge_local_micro AS FLOAT64)), 0) AS age_type_costs,
  segment_name AS age_range_type
FROM
  `ht-data-management.hdy_databank_x1dl_export_enabled.twitter_campaign_age_stats`
WHERE
  date BETWEEN '2022-01-23'
  AND '2024-06-30'
GROUP BY
  campaign_id,
  date,
  age_range_type
ORDER BY
  id,
  date,
  age_range_type