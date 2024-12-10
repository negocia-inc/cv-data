SELECT
  a.campaign_id,
  a.ad_group_id,
  a.id,
  a.name,
  a.path,
  b.type,
  EXTRACT (year
  FROM
    date) AS year,
  EXTRACT (month
  FROM
    date) AS month,
  EXTRACT (day
  FROM
    date) AS day,
  a.thumbnail_width,
  a.thumbnail_height,
  a.duration,
  COALESCE(SUM(CAST(c.impressions AS INT64)), 0) AS impressions,
  COALESCE(SUM(CAST(c.clicks AS INT64)), 0) AS clicks,
  COALESCE(SUM(CAST(c.costs AS FLOAT64)), 0) AS costs,
  COALESCE(SUM(CAST(c.video_impressions AS INT64)), 0) AS video_impressions,
  COALESCE(SUM(CAST(c.video_quartile_25 AS INT64)), 0) AS video_quartile_25,
  COALESCE(SUM(CAST(c.video_quartile_50 AS INT64)), 0) AS video_quartile_50,
  COALESCE(SUM(CAST(c.video_quartile_75 AS INT64)), 0) AS video_quartile_75,
  COALESCE(SUM(CAST(c.video_quartile_100 AS INT64)), 0) AS video_quartile_100,
FROM
  `ida-prd.ipalette_optout.facebook_ad_video_assets` AS a
JOIN
  `ida-prd.ipalette_optout.facebook_ad_uploaded` AS b
ON
  a.ad_account_id = b.ad_account_id
  AND a.campaign_id = b.campaign_id
  AND a.ad_group_id = b.ad_group_id
  AND a.ad_id = b.id
JOIN
  `ida-prd.ipalette_optout.facebook_ad` AS c
ON
  a.ad_account_id = c.ad_account_id
  AND a.campaign_id = c.campaign_id
  AND a.ad_group_id = c.ad_group_id
  AND a.ad_id = c.id
WHERE
  date BETWEEN '2021-10-01'
  AND '2024-09-30'
  AND b.type="video"
  AND a.path IS NOT NULL
GROUP BY
  campaign_id,
  ad_group_id,
  id,
  name,
  path,
  year,
  month,
  day,
  thumbnail_width,
  thumbnail_height,
  duration,
  type
ORDER BY
  campaign_id,
  ad_group_id,
  id,
  name,
  path,
  year,
  month,
  day,
  thumbnail_width,
  thumbnail_height,
  duration,
  type