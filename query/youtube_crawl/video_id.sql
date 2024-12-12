DECLARE
  date_from date DEFAULT '2024-09-30'; -- 日付の開始地点を指定
DECLARE
  date_to date DEFAULT '2024-09-30'; -- 日付の終了地点を指定    
WITH
  app_asset AS(
  SELECT
    DISTINCT ad_group_ad_ad_app_ad_youtube_videos
  FROM
    `ida-prd.xone_optout.googleads_ad`
  WHERE
    ad_group_ad_ad_type ='APP_AD'
    AND date BETWEEN date_from
    AND date_to),
  app_asset_unnest AS (
  SELECT
    DISTINCT REGEXP_EXTRACT(JSON_EXTRACT_SCALAR(asset, '$.asset'), r'assets/(\d+)$') AS asset_id
  FROM
    app_asset,
    UNNEST(JSON_EXTRACT_ARRAY(ad_group_ad_ad_app_ad_youtube_videos)) AS asset )
SELECT
  DISTINCT video.path AS id
FROM
  `ida-prd.ipalette_optout.googleads_ad_video_assets` AS video
INNER JOIN
  `ida-prd.ipalette_optout.googleads_ad_uploaded` AS up
ON
  video.ad_account_id = up.ad_account_id
  AND video.campaign_id = up.campaign_id
  AND video.ad_group_id = up.ad_group_id
  AND video.ad_id = up.id
INNER JOIN
  `ida-prd.ipalette_optout.googleads_ad` AS ad
ON
  video.ad_account_id = ad.ad_account_id
  AND video.campaign_id = ad.campaign_id
  AND video.ad_group_id = ad.ad_group_id
  AND video.ad_id = ad.id
WHERE
  ad.date BETWEEN date_from
  AND date_to
UNION DISTINCT
SELECT
  DISTINCT gau.asset_youtube_video_asset_youtube_video_id AS id
FROM
  app_asset_unnest AS aau
INNER JOIN
  `ida-dev-340802.save_datalake_optout.googleads_asset_uploaded` AS gau
ON
  aau.asset_id=gau.asset_id