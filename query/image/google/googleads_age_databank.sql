SELECT
  campaign_id,
  ad_group_id,
  date,
  COALESCE(SUM(CAST(metrics_impressions AS INT64)), 0) AS age_type_impressions,
  COALESCE(SUM(CAST(metrics_clicks AS INT64)), 0) AS age_type_clicks,
  COALESCE(SUM(CAST(metrics_cost_micros AS FLOAT64)), 0) AS age_type_costs,
  ad_group_criterion_age_range_type AS age_range_type
FROM
  `ht-data-management.hdy_databank_x1dl_export_enabled.googleads_age`
WHERE
  date BETWEEN '2022-02-25'
  AND '2024-06-30'
GROUP BY
  campaign_id,
  ad_group_id,
  date,
  ad_group_criterion_age_range_type
ORDER BY
  campaign_id,
  ad_group_id,
  date,
  ad_group_criterion_age_range_type