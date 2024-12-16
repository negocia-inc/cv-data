declare date_from date default '2021-10-01';
declare date_to date default '2024-09-30';

WITH tiktok_ad_report AS (
    SELECT
        advertiser_id AS ad_account_id, 
        campaign_id,
        adgroup_id AS ad_group_id,
        ad_id AS id,
        date,
        ad_name AS name,
        SUM(CAST(impressions AS BIGINT)) AS impressions,
        SUM(CAST(clicks AS BIGINT)) AS clicks,
        SUM(CAST(spend AS NUMERIC)) AS costs,
        SUM(CAST(video_play_actions AS NUMERIC)) AS video_impressions,
        SUM(CAST(video_views_p25 AS NUMERIC)) AS video_quartile_25,
        SUM(CAST(video_views_p50 AS NUMERIC)) AS video_quartile_50,
        SUM(CAST(video_views_p75 AS NUMERIC)) AS video_quartile_75,
        SUM(CAST(video_views_p100 AS NUMERIC)) AS video_quartile_100
    FROM `ht-data-management.hdy_databank_x1dl_export_enabled.tiktok_auction_ads_basic_ad_report`
    WHERE date BETWEEN date_from AND date_to
    GROUP BY
        advertiser_id,
        campaign_id,
        adgroup_id,
        ad_id,
        date,
        ad_name
), 
tiktok_ad_uploaded AS (
    SELECT 
        date,
        advertiser_id AS ad_account_id,
        campaign_id,
        adgroup_id AS ad_group_id,
        ad_id AS id,
        video_id,
        ad_name AS name,
        ad_format AS type,
    FROM `ht-data-management.hdy_databank_x1dl_export_enabled.tiktok_ad`
    WHERE date BETWEEN date_from AND date_to
),
tiktok_ad_video_assets AS (
    SELECT
        date,
        advertiser_id AS ad_account_id,
        id AS video_id,
        duration,
        width AS thumbnail_width,
        height AS thumbnail_height,
        url,
    FROM `ht-data-management.hdy_databank_x1dl_export_enabled.tiktok_creative_video`
    WHERE date BETWEEN date_from AND date_to
),
creative_meta AS (
    -- 取得時と同じ形式にするため : https://file.notion.so/f/f/436e1ae2-1dd9-4970-b291-5174da39102a/3f5c2e60-ed17-42d0-8a5f-4ec1ad73f059/sql-get-creative-tiktok-creative-video.sql?table=block&id=137db36c-c601-807d-8d59-ee54ffd34f8c&spaceId=436e1ae2-1dd9-4970-b291-5174da39102a&expirationTimestamp=1731394879978&signature=WqNxjfDatcuUM6cxPBWg7pkBTdRyx_SFzy3vrTjmZd8&downloadName=sql-get-creative-tiktok-creative-video.sql
    -- サムネイルを除くため、poster_urlを取得せず
    SELECT distinct
       report_date,
       creative_url,
       storage_uri
       FROM
          `ht-data-management.hdy_databank_x1dl_export_enabled.creative_metadata`
       WHERE
           report_date BETWEEN date_from AND date_to
           AND if_id = 'tiktok.creative_video'
)
SELECT
    tar.campaign_id,
    tar.ad_group_id,
    tau.video_id AS id,
    tar.name,
    cm.storage_uri AS path, 
    tau.type,
    EXTRACT (year FROM tar.date) AS year,
    EXTRACT (month FROM tar.date) AS month,
    EXTRACT (day FROM tar.date) AS day,
    tava.thumbnail_width,
    tava.thumbnail_height,
    tava.duration,
    COALESCE(SUM(CAST(tar.impressions AS INT64)), 0) AS impressions,
    COALESCE(SUM(CAST(tar.clicks AS INT64)), 0) AS clicks,
    COALESCE(SUM(CAST(tar.costs AS FLOAT64)), 0) AS costs,
    COALESCE(SUM(CAST(tar.video_impressions AS INT64)), 0) AS video_impressions,
    COALESCE(SUM(CAST(tar.video_quartile_25 AS INT64)), 0) AS video_quartile_25,
    COALESCE(SUM(CAST(tar.video_quartile_50 AS INT64)), 0) AS video_quartile_50,
    COALESCE(SUM(CAST(tar.video_quartile_75 AS INT64)), 0) AS video_quartile_75,
    COALESCE(SUM(CAST(tar.video_quartile_100 AS INT64)), 0) AS video_quartile_100
FROM tiktok_ad_report AS tar 
JOIN tiktok_ad_uploaded AS tau
ON tar.ad_account_id = tau.ad_account_id
AND tar.campaign_id = tau.campaign_id
AND tar.ad_group_id = tau.ad_group_id
AND tar.id = tau.id 
AND tar.date = tau.date
JOIN tiktok_ad_video_assets AS tava
ON tar.ad_account_id = tava.ad_account_id
AND tau.video_id = tava.video_id
AND tar.date = tava.date
JOIN creative_meta AS cm
ON tava.url = cm.creative_url
AND tar.date = cm.report_date
WHERE tau.type = 'SINGLE_VIDEO'
GROUP BY
    tar.campaign_id,
    tar.ad_group_id,
    tau.video_id,
    tar.name,
    tau.type,
    year,
    month,
    day,
    tava.thumbnail_width,
    tava.thumbnail_height,
    tava.duration,
    cm.storage_uri