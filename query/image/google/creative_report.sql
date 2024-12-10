SELECT
  a.campaign_id,
  a.ad_group_id,
  a.id,
  a.path,
  b.type,
  EXTRACT (year FROM date) AS year,
  EXTRACT (month FROM date) AS month,
  EXTRACT (day FROM date) AS day,
  a.width,
  a.height,
  COALESCE(SUM(CAST(c.impressions AS INT64)), 0) AS impressions,
  COALESCE(SUM(CAST(c.clicks AS INT64)), 0) AS clicks,
  COALESCE(SUM(CAST(c.costs AS FLOAT64)), 0) AS costs,
FROM `ida-prd.ipalette_optout.googleads_ad_image_assets` AS a
JOIN `ida-prd.ipalette_optout.googleads_ad_uploaded` AS b
ON a.ad_account_id = b.ad_account_id
AND a.campaign_id = b.campaign_id
AND a.ad_group_id = b.ad_group_id
AND a.ad_id = b.id
JOIN `ida-prd.ipalette_optout.googleads_ad` as c
ON a.ad_account_id = c.ad_account_id
AND a.campaign_id = c.campaign_id
AND a.ad_group_id = c.ad_group_id
AND a.ad_id = c.id
WHERE date between '2021-07-01' and '2024-06-30' -- 取得したい範囲に応じて変更する
AND b.type in ("IMAGE_AD", "DEMAND_GEN_MULTI_ASSET_AD", "DISCOVERY_MULTI_ASSET_AD")
GROUP BY
  campaign_id,
  ad_group_id,
  id,
  path,
  year,
  month,
  day,
  width,
  height,
  type
ORDER BY
  campaign_id,
  ad_group_id,
  id,
  path,
  year,
  month,
  day,
  width,
  height,
  type