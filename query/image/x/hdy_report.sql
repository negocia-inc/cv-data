declare date_from date default '2021-07-01';
declare date_to date default '2024-06-30'; 
  
WITH  
  -- Fetch promoted tweet stats within the date range  
  promoted_tweet_stats AS (  
    SELECT  
      account_id,  
      id,  
      date,  
      impressions,  
      url_clicks,
      billed_charge_local_micro  
    FROM  
      `ht-data-management.hdy_databank_x1dl_export_enabled.twitter_promoted_tweet_stats`  
    WHERE  
      date BETWEEN date_from AND date_to  
  ),  
    
  -- Fetch promoted tweet metadata  
  promoted_tweet_meta AS (  
    SELECT DISTINCT
      account_id,  
      id,  
      line_item_id,  
      tweet_id  
    FROM  
      `ht-data-management.hdy_databank_x1dl_export_enabled.twitter_promoted_tweet_meta`
  ),  
    
  -- Fetch tweet metadata  
  tweet_meta AS (  
    SELECT DISTINCT
      account_id,  
      tweet_id,  
      card_uri  
    FROM  
      `ht-data-management.hdy_databank_x1dl_export_enabled.twitter_tweet_meta` 
    -- WHERE  
    --   date BETWEEN date_from AND date_to  
  ),  
    
  -- Fetch line item metadata  
  line_item_meta AS (  
    SELECT DISTINCT
      account_id,  
      id,  
      campaign_id  
    FROM  
      `ht-data-management.hdy_databank_x1dl_export_enabled.twitter_line_item_meta`
    -- WHERE  
    --   date BETWEEN date_from AND date_to    
  ),  
    
  -- Fetch card carousel components within the date range  
  card_carousel_components AS (  
    SELECT
      account_id,  
      JSON_EXTRACT_ARRAY(components) AS components_array,  
      card_type,  
      card_uri  
    FROM  
      `ht-data-management.hdy_databank_x1dl_export_enabled.twitter_card_carousel_meta`  
    WHERE  
      date BETWEEN date_from AND date_to  
  ),  
    
  -- Extract media keys from card carousel components  
  card_carousel_meta AS (  
    SELECT DISTINCT
      account_id,  
      JSON_EXTRACT_SCALAR(component, '$.media_key') AS media_key,  
      component,  
      card_type,  
      card_uri  
    FROM  
      card_carousel_components,  
      UNNEST(components_array) AS component  
    WHERE  
      JSON_EXTRACT_SCALAR(component, '$.media_key') IS NOT NULL  
  
    UNION ALL  
  
    SELECT  
      account_id,  
      media_key,  
      component,  
      card_type,  
      card_uri  
    FROM (  
      SELECT  
        account_id,  
        component,  
        card_type,  
        JSON_EXTRACT_STRING_ARRAY(JSON_EXTRACT(component, '$.media_keys')) AS media_keys,  
        card_uri  
      FROM  
        card_carousel_components,  
        UNNEST(components_array) AS component  
    ), UNNEST(media_keys) AS media_key  
    WHERE  
      media_key IS NOT NULL  
  ),  
    
  -- Fetch media library metadata  
  media_library_meta AS (  
    SELECT DISTINCT
      account_id,  
      media_key,  
      poster_media_url AS creative_url,  
      original_width AS width,  
      original_height AS height  
    FROM  
      `ht-data-management.hdy_databank_x1dl_export_enabled.twitter_media_library_meta`  
    WHERE  
      poster_media_url IS NOT NULL
      -- AND date BETWEEN date_from AND date_to  
  
    UNION ALL  
  
    SELECT DISTINCT
      account_id,  
      media_key,  
      media_url AS creative_url,  
      original_width AS width,  
      original_height AS height  
    FROM  
      `ht-data-management.hdy_databank_x1dl_export_enabled.twitter_media_library_meta`  
    WHERE  
      media_url IS NOT NULL
      -- AND date BETWEEN date_from AND date_to    
  ),  
    
  -- Combine creative data from different sources  
  creative AS (  
    SELECT DISTINCT
      account_id,  
      poster_media_url AS creative_url,  
      image_display_width AS width,  
      image_display_height AS height,  
      card_type,  
      card_uri,  
      media_key  
    FROM  
      `ht-data-management.hdy_databank_x1dl_export_enabled.twitter_card_meta`  
    WHERE  
      poster_media_url IS NOT NULL  
      AND date BETWEEN date_from AND date_to  
  
    UNION ALL  
  
    SELECT DISTINCT
      account_id,  
      media_url AS creative_url,  
      image_display_width AS width,  
      image_display_height AS height,  
      card_type,  
      card_uri,  
      media_key  
    FROM  
      `ht-data-management.hdy_databank_x1dl_export_enabled.twitter_card_meta`  
    WHERE  
      media_url IS NOT NULL  
      AND date BETWEEN date_from AND date_to  
  
    UNION ALL  
  
    SELECT
      ccm.account_id,  
      mlm.creative_url,  
      REGEXP_EXTRACT(JSON_EXTRACT(ccm.component, '$.media_metadata'), 'width":([0-9]+)') AS width,  
      REGEXP_EXTRACT(JSON_EXTRACT(ccm.component, '$.media_metadata'), 'height":([0-9]+)') AS height,  
      ccm.card_type,  
      ccm.card_uri,  
      ccm.media_key  
    FROM  
      card_carousel_meta AS ccm  
      INNER JOIN media_library_meta AS mlm  
        ON ccm.account_id = mlm.account_id  
        AND ccm.media_key = mlm.media_key  
  ),  
    
  -- Fetch creative metadata  
  creative_meta AS (  
    SELECT DISTINCT
      report_date,  
      creative_url,  
      storage_uri  
    FROM  
      `ht-data-management.hdy_databank_x1dl_export_enabled.creative_metadata`  
    WHERE  
      report_date BETWEEN date_from AND date_to  
      AND if_id = 'twitter.promoted_tweet_stats'  
  )  
    
SELECT  
  lim.campaign_id AS campaign_id,  
  lim.id AS ad_group_id,  
  c.media_key AS id,  
  cm.storage_uri AS path,  
  c.card_type AS type,  
  EXTRACT(YEAR FROM pts.date) AS year,  
  EXTRACT(MONTH FROM pts.date) AS month,  
  EXTRACT(DAY FROM pts.date) AS day,  
  c.width,  
  c.height,  
  COALESCE(SUM(CAST(pts.impressions AS INT64)), 0) AS impressions,  
  COALESCE(SUM(CAST(pts.url_clicks AS INT64)), 0) AS clicks,  
  COALESCE(SUM(CAST(pts.billed_charge_local_micro AS FLOAT64)), 0) AS costs  
FROM  
  promoted_tweet_stats pts  
  INNER JOIN promoted_tweet_meta ptm  
    ON ptm.account_id = pts.account_id  
    AND ptm.id = pts.id  
  INNER JOIN tweet_meta tm  
    ON tm.account_id = ptm.account_id  
    AND tm.tweet_id = ptm.tweet_id  
  INNER JOIN line_item_meta lim  
    ON lim.account_id = ptm.account_id  
    AND lim.id = ptm.line_item_id  
  INNER JOIN creative c  
    ON c.account_id = tm.account_id  
    AND c.card_uri = tm.card_uri  
  INNER JOIN creative_meta cm  
    ON pts.date = cm.report_date  
    AND c.creative_url = cm.creative_url  
WHERE  
  c.card_type IN ('IMAGE_APP', 'IMAGE_WEBSITE')  
GROUP BY  
  campaign_id,  
  ad_group_id,  
  id,  
  path,  
  year,  
  month,  
  day,  
  c.width,  
  c.height,  
  type  
ORDER BY  
  campaign_id,  
  ad_group_id,  
  id,  
  path,  
  year,  
  month,  
  day,  
  c.width,  
  c.height,  
  type;   