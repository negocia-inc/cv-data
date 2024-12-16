declare date_from date default '2024-09-18';
declare date_to date default '2024-09-30';

WITH meta_video_view AS (
    SELECT
        date,
        account_id AS ad_account_id,
        campaign_id AS campaign_id,
        adset_id AS ad_group_id,
        ad_id AS id,
        CAST(impressions AS BIGINT) AS impressions,
        CAST(inline_link_clicks AS BIGINT) AS clicks,
        CAST(spend AS NUMERIC) AS cost,
        JSON_VALUE(PARSE_JSON(video_play_actions)[0], '$.value') AS video_play_actions,
        JSON_VALUE(PARSE_JSON(video_p25_watched_actions)[0], '$.value') AS video_p25_watched_value,
        JSON_VALUE(PARSE_JSON(video_p50_watched_actions)[0], '$.value') AS video_p50_watched_actions,
        JSON_VALUE(PARSE_JSON(video_p75_watched_actions)[0], '$.value') AS video_p75_watched_actions,
        JSON_VALUE(PARSE_JSON(video_p100_watched_actions)[0], '$.value') AS video_p100_watched_actions,
    FROM
        `ht-data-management.hdy_databank_x1dl_export_enabled.facebook_adgroup_stats`
    WHERE
        date BETWEEN date_from AND date_to
),
meta_video_ad_report AS (
    SELECT
        ad_account_id,
        campaign_id,
        ad_group_id,
        id,
        date,
        SUM(impressions) AS impressions,
        SUM(clicks) AS clicks,
        CAST(SUM(cost) AS NUMERIC) AS costs,
        SUM(CAST(video_play_actions AS NUMERIC)) AS video_impressions,
        SUM(CAST(video_p25_watched_value AS NUMERIC)) AS video_quartile_25,
        SUM(CAST(video_p50_watched_actions AS NUMERIC)) AS video_quartile_50,
        SUM(CAST(video_p75_watched_actions AS NUMERIC)) AS video_quartile_75,
        SUM(CAST(video_p100_watched_actions AS NUMERIC)) AS video_quartile_100,
    FROM
        meta_video_view
    GROUP BY
        ad_account_id,
        campaign_id,
        ad_group_id,
        id,
        date
),
meta_video_ad_uploaded_history AS (
    SELECT DISTINCT
        account_id,
        campaign_id,
        adset_id,
        adgroup_id,
        JSON_VALUE(creative, '$.id') AS creative_id,
        JSON_EXTRACT(creative, '$.object_story_spec') AS object_story_spec,
        JSON_EXTRACT(creative, '$.asset_feed_spec') AS asset_feed_spec,
        JSON_EXTRACT(creative, '$.effective_instagram_media_id') AS effective_instagram_media_id,
        JSON_EXTRACT(creative, '$.effective_instagram_story_id') AS effective_instagram_story_id,
        JSON_EXTRACT(creative, '$.effective_object_story_id') AS effective_object_story_id,
        JSON_EXTRACT(creative, '$.object_story_id') AS object_story_id
    FROM
        `ht-data-management.hdy_databank_x1dl_export_enabled.facebook_adgroup_meta_history`
    WHERE 
        start_date <= date_to AND end_date >= date_from
),
meta_video_ad_uploaded AS (
    SELECT DISTINCT
        account_id,
        campaign_id,
        adset_id,
        adgroup_id,
        creative_id,
        case
            when object_story_spec like '%"template_data":%' then 'ダイナミック広告'
            when object_story_spec like '%"video_data":%' and object_story_spec not like '%"collection_thumbnails":%' and object_story_spec not like '%"retailer_item_ids":%' then '動画広告'
            when object_story_spec like '%"video_data":%' and (object_story_spec like '%"collection_thumbnails":%' or object_story_spec like '%"retailer_item_ids":%') then 'コレクション広告'
            when object_story_spec like '%"link_data":%' and (object_story_spec like '%"collection_thumbnails":%' or object_story_spec like '%"retailer_item_ids":%') then 'コレクション広告'
            when object_story_spec like '%"link_data":%' and object_story_spec like '%"child_attachments":%' then 'カルーセル広告'
            when object_story_spec like '%"link_data":%' and ARRAY_LENGTH(REGEXP_EXTRACT_ALL(object_story_spec, r'"image_hash":\s*"(.*?)"')) = 1 and ARRAY_LENGTH(REGEXP_EXTRACT_ALL(object_story_spec, r'"link":\s*"(.*?)"')) = 1 then '静止画広告'
            when object_story_spec like '%"photo_data":%' then '静止画広告'
            when asset_feed_spec like '%"ad_formats": ["AUTOMATIC_FORMAT"]%' and asset_feed_spec like '%"images"%' and asset_feed_spec not like '%"video_id":%' and ARRAY_LENGTH(REGEXP_EXTRACT_ALL(asset_feed_spec, r'"hash":\s*"(.*?)"')) > 0 then '静止画広告'
            when asset_feed_spec like '%"ad_formats": ["AUTOMATIC_FORMAT"]%' and asset_feed_spec like '%"videos"%' and asset_feed_spec like '%"video_id":%' and ARRAY_LENGTH(REGEXP_EXTRACT_ALL(asset_feed_spec, r'"video_id":\s*"(.*?)"')) > 0 then '動画広告'
            when asset_feed_spec like '%"ad_formats": ["SINGLE_IMAGE"]%' then '静止画広告'
            when asset_feed_spec like '%"ad_formats": ["SINGLE_VIDEO"]%' then '動画広告'
            when asset_feed_spec like '%"ad_formats": ["CAROUSEL"]%' then 'カルーセル広告'
            when object_story_id is not null and effective_object_story_id is not null and LENGTH(object_story_id) > 0 and LENGTH(effective_object_story_id) > 0 then 'オーガニック広告'
            when effective_instagram_media_id is not null and effective_instagram_story_id is not null and LENGTH(effective_instagram_media_id) > 0 and LENGTH(effective_instagram_story_id) > 0 then 'オーガニック広告'
            else 'Unknown' end as ad_type
    FROM
        meta_video_ad_uploaded_history
),
meta_creative AS (
    SELECT 
        date,   
        account_id,
        creative_id,   
        JSON_VALUE(JSON_EXTRACT(object_story_spec, '$.video_data'), '$.video_id') AS video_id,
    FROM `ht-data-management.hdy_databank_x1dl_export_enabled.facebook_creative`
    WHERE date BETWEEN date_from AND date_to
    UNION DISTINCT
    SELECT 
        date,   
        account_id,
        creative_id,   
        JSON_VALUE(JSON_EXTRACT(asset_feed_spec, '$.video_data'), '$.video_id') AS video_id,
    FROM `ht-data-management.hdy_databank_x1dl_export_enabled.facebook_creative`
    WHERE date BETWEEN date_from AND date_to
),
video_asset AS (
  SELECT
    date,
    account_id,
    video_id,
    title,
    length AS duration,
    source
  FROM `ht-data-management.hdy_databank_x1dl_export_enabled.facebook_video`
  WHERE date BETWEEN date_from AND date_to
),
creative_meta AS (
    SELECT distinct
    -- if_idが重複があるためdistinct
       report_date,
       creative_url,
       storage_uri
       FROM
          `ht-data-management.hdy_databank_x1dl_export_enabled.creative_metadata`
       WHERE
           report_date BETWEEN date_from AND date_to
           AND if_id in ("facebook.adgroup_stats")
)
SELECT
    mvar.campaign_id,
    mvar.ad_group_id,
    mc.video_id AS id,
    va.title AS name,
    cm.storage_uri AS path, 
    mvau.ad_type AS type,
    EXTRACT (year FROM mvar.date) AS year,
    EXTRACT (month FROM mvar.date) AS month,
    EXTRACT (day FROM mvar.date) AS day,
    va.duration,
    COALESCE(SUM(CAST(mvar.impressions AS INT64)), 0) AS impressions,
    COALESCE(SUM(CAST(mvar.clicks AS INT64)), 0) AS clicks,
    COALESCE(SUM(CAST(mvar.costs AS FLOAT64)), 0) AS costs,
    COALESCE(SUM(CAST(mvar.video_impressions AS INT64)), 0) AS video_impressions,
    COALESCE(SUM(CAST(mvar.video_quartile_25 AS INT64)), 0) AS video_quartile_25,
    COALESCE(SUM(CAST(mvar.video_quartile_50 AS INT64)), 0) AS video_quartile_50,
    COALESCE(SUM(CAST(mvar.video_quartile_75 AS INT64)), 0) AS video_quartile_75,
    COALESCE(SUM(CAST(mvar.video_quartile_100 AS INT64)), 0) AS video_quartile_100
FROM meta_video_ad_report AS mvar
JOIN meta_video_ad_uploaded AS mvau
ON mvar.ad_account_id = mvau.account_id
AND mvar.campaign_id = mvau.campaign_id
AND mvar.ad_group_id = mvau.adset_id 
AND mvar.id = mvau.adgroup_id
JOIN meta_creative AS mc
ON mvar.ad_account_id = mc.account_id
AND mvau.creative_id = mc.creative_id
AND mvar.date = mc.date
JOIN video_asset AS va
ON mc.account_id = va.account_id
AND mc.video_id = va.video_id
AND mc.date = va.date
JOIN creative_meta AS cm
ON va.source = cm.creative_url
AND va.date = cm.report_date
WHERE mvau.ad_type = '動画広告'
GROUP BY
    mvar.campaign_id,
    mvar.ad_group_id,
    mc.video_id,
    va.title,
    cm.storage_uri,
    mvau.ad_type,
    year,
    month,
    day,
    va.duration
    
    
    