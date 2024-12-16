declare date_from date default '2021-07-01';
declare date_to date default '2024-06-30';

--YDN---
WITH ydn_ad AS (  
  SELECT  
    date AS report_date,  
    account_id AS client_id,  
    media_id,  
    CONCAT('YDN/', account_id, '/', media_id) AS creative_url,  
    campaign_id,  
    ad_group_id,  
    ad_type,  
    ad_style,  
    creative_size,  
    clicks,  
    impressions,  
    cost  
  FROM  
    `ht-data-management.hdy_databank_x1dl_export_enabled.ydn_ad`  
  WHERE  
    date BETWEEN date_from AND date_to  
    AND media_id IS NOT NULL  
    AND media_id <> '--'  
),  
creative_meta AS (  
  SELECT DISTINCT
    report_date,  
    creative_url,  
    storage_uri  
  FROM  
    `ht-data-management.hdy_databank_x1dl_export_enabled.creative_metadata`  
  WHERE  
    report_date BETWEEN date_from AND date_to  
    AND if_id = 'ydn.ad'  
)  
SELECT  
  ydn_ad.campaign_id,  
  ydn_ad.ad_group_id,  
  ydn_ad.media_id AS id,  
  creative_meta.storage_uri AS path,  
  ydn_ad.ad_type AS type,  
  EXTRACT(YEAR FROM ydn_ad.report_date) AS year,  
  EXTRACT(MONTH FROM ydn_ad.report_date) AS month,  
  EXTRACT(DAY FROM ydn_ad.report_date) AS day,  
  CAST(SUBSTRING(ydn_ad.creative_size, 1, STRPOS(ydn_ad.creative_size, 'x') - 1) AS INT64) AS width,  
  CAST(SUBSTRING(ydn_ad.creative_size, STRPOS(ydn_ad.creative_size, 'x') + 1) AS INT64) AS height,  
  COALESCE(SUM(CAST(ydn_ad.impressions AS INT64)), 0) AS impressions,  
  COALESCE(SUM(CAST(ydn_ad.clicks AS INT64)), 0) AS clicks,  
  COALESCE(SUM(CAST(ydn_ad.cost AS FLOAT64)), 0) AS costs  
FROM  
  ydn_ad  
INNER JOIN  
  creative_meta  
ON  
  ydn_ad.report_date = creative_meta.report_date  
  AND ydn_ad.creative_url = creative_meta.creative_url  
WHERE  
  (ydn_ad.ad_style = '画像' AND ydn_ad.ad_type = 'バナー')  
  OR ydn_ad.ad_type = 'バナー（画像）'  
GROUP BY  
  ydn_ad.campaign_id,  
  ydn_ad.ad_group_id,  
  ydn_ad.media_id,  
  creative_meta.storage_uri,  
  ydn_ad.ad_type,  
  year,  
  month,  
  day,  
  width,  
  height  
ORDER BY  
  ydn_ad.campaign_id,  
  ydn_ad.ad_group_id,  
  ydn_ad.media_id,  
  creative_meta.storage_uri,  
  year,  
  month,  
  day,  
  width,  
  height,  
  ydn_ad.ad_type;  
