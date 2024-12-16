SELECT
  campaign_id,
  ad_group_id,
  date,
  COALESCE(SUM(CAST(impressions AS INT64)), 0) AS gender_type_impressions,
  COALESCE(SUM(CAST(clicks AS INT64)), 0) AS gender_type_clicks,
  COALESCE(SUM(CAST(cost AS FLOAT64)), 0) AS gender_type_costs,
  CASE
    WHEN gender = 'Undetermined' THEN 'UNDETERMINED'
    WHEN gender = 'Male' THEN 'MALE'
    WHEN gender= 'Female' THEN 'FEMALE'
END
  AS gender_type
FROM
  `ht-data-management.hdy_databank_x1dl_export_enabled.adwords_gender`
WHERE
  date BETWEEN '2021-07-01'
  AND '2022-02-24'
GROUP BY
  campaign_id,
  ad_group_id,
  date,
  gender_type
ORDER BY
  campaign_id,
  ad_group_id,
  date,
  gender_type