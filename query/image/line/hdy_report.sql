declare date_from date default '2021-07-01';
declare date_to date default '2024-06-30';

--lap--  
WITH  
  report AS (  
    SELECT  
      ad_account_id,  
      campaign_id,  
      CAST(adgroup_id AS STRING) AS ad_group_id,  
      CAST(ad_id AS STRING) AS ad_id,  
      clicks,  
      impression,  
      cost,  
      date  
    FROM  
      `ht-data-management.hdy_databank_x1dl_export_enabled.lap_ad_daily_report`  
    WHERE  
      date BETWEEN date_from AND date_to  
  ),  
  ads AS (  
    SELECT  
      CAST(adgroup_id AS STRING) AS ad_group_id,  
      CAST(id AS STRING) AS ad_id,  
      media_hash AS creative_media_hash,  
      date,  
      creative_id,  
      creative_format  
    FROM  
      `ht-data-management.hdy_databank_x1dl_export_enabled.lap_ads`,  
      UNNEST([  
        creative_media_hash,  
        creative_image_hash,  
        creative_video_hash,  
        creative_animation_hash  
      ]) AS media_hash  
    WHERE  
      date BETWEEN date_from AND date_to  
      AND creative_format IN ('IMAGE', 'STATIC BANNER')  
  
    UNION ALL  
  
    SELECT  
      CAST(adgroup_id AS STRING) AS ad_group_id,  
      CAST(id AS STRING) AS ad_id,  
      IFNULL(  
        JSON_EXTRACT_SCALAR(creative_slot, '$.mediaHash'),  
        JSON_EXTRACT_SCALAR(creative_slot, '$.imageHash')  
      ) AS creative_media_hash,  
      date,  
      creative_id,  
      creative_format  
    FROM  
      `ht-data-management.hdy_databank_x1dl_export_enabled.lap_ads`,  
      UNNEST(JSON_EXTRACT_ARRAY(creative_slots)) AS creative_slot  
    WHERE  
      date BETWEEN date_from AND date_to  
      AND creative_format IN ('IMAGE', 'STATIC BANNER')  
  ),  
  creative AS (  
    SELECT  
      media_hash,  
      source_url,  
      date,  
      width,  
      height,  
      id  
    FROM  
      `ht-data-management.hdy_databank_x1dl_export_enabled.lap_media`  
    WHERE  
      date BETWEEN date_from AND date_to  
      AND source_url IS NOT NULL  
  ),  
  creative_meta AS (  
    SELECT  
      report_date,  
      creative_url,  
      storage_uri  
    FROM  
      `ht-data-management.hdy_databank_x1dl_export_enabled.creative_metadata`  
    WHERE  
      report_date BETWEEN date_from AND date_to  
      AND if_id = 'lap.ad_daily_report'  
  )  
SELECT  
  report.campaign_id,  
  report.ad_group_id,  
  creative.id,  
  creative_meta.storage_uri AS path,  
  ads.creative_format AS type,  
  EXTRACT(YEAR FROM report.date) AS year,  
  EXTRACT(MONTH FROM report.date) AS month,  
  EXTRACT(DAY FROM report.date) AS day,  
  creative.width,  
  creative.height,  
  COALESCE(SUM(CAST(report.impression AS INT64)), 0) AS impressions,
  COALESCE(SUM(CAST(report.clicks AS INT64)), 0) AS clicks,
  COALESCE(SUM(CAST(report.cost AS FLOAT64)), 0) AS costs
FROM  
  report  
  INNER JOIN ads  
    ON ads.ad_group_id = report.ad_group_id  
    AND ads.ad_id = report.ad_id  
    AND ads.date = report.date  
  INNER JOIN creative  
    ON creative.media_hash = ads.creative_media_hash  
    AND creative.date = ads.date  
  INNER JOIN creative_meta  
    ON report.date = creative_meta.report_date  
    AND creative.source_url = creative_meta.creative_url  
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
