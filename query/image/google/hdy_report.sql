declare date_from date default '2021-07-01';
declare date_to date default '2024-06-30';


--googleads--
WITH source1 AS (
    SELECT
        date AS report_date,
        customer_id AS client_id,
        url AS creative_url,
        campaign_id,
        ad_group_id,
        ad_group_ad_ad_image_ad_image_url,
        ad_group_ad_ad_type,
        ad_group_ad_ad_image_ad_pixel_height AS image_height,
        ad_group_ad_ad_image_ad_pixel_width AS image_width,
        metrics_clicks AS clicks,
        metrics_impressions AS impressions,
        metrics_cost_micros AS cost
    FROM
        `ht-data-management.hdy_databank_x1dl_export_enabled.googleads_ad`,
        UNNEST([ad_group_ad_ad_image_ad_image_url]) AS url
    WHERE
        date BETWEEN date_from AND date_to
        AND url IS NOT NULL
), 
report AS (
    -- 使用していない要素で重複が発生しているため、DISTINCTを追加
    SELECT DISTINCT
        date,   
        customer_id,
        asset_id,
        campaign_id,
        ad_group_id,
        ad_group_ad_ad_image_ad_image_url,
        ad_group_ad_ad_type,
        ad_group_ad_ad_image_ad_pixel_height,
        ad_group_ad_ad_image_ad_pixel_width,
        metrics_clicks,
        metrics_impressions,
        metrics_cost_micros
    FROM `ht-data-management.hdy_databank_x1dl_export_enabled.googleads_ad`,
        UNNEST (ARRAY_CONCAT(
            IFNULL(REGEXP_EXTRACT_ALL(ad_group_ad_ad_app_ad_images, 'assets/(.*?)"'), []),
            IFNULL(REGEXP_EXTRACT_ALL(ad_group_ad_ad_app_engagement_ad_images, 'assets/(.*?)"'), []),
            IFNULL(REGEXP_EXTRACT_ALL(ad_group_ad_ad_app_pre_registration_ad_images, 'assets/(.*?)"'), []),
            IFNULL(REGEXP_EXTRACT_ALL(ad_group_ad_ad_demand_gen_carousel_ad_carousel_cards, 'assets/(.*?)"'), []),
            IFNULL(REGEXP_EXTRACT_ALL(ad_group_ad_ad_demand_gen_carousel_ad_logo_image, 'assets/(.*?)"'), []),
            IFNULL(REGEXP_EXTRACT_ALL(ad_group_ad_ad_demand_gen_multi_asset_ad_logo_images, 'assets/(.*?)"'), []),
            IFNULL(REGEXP_EXTRACT_ALL(ad_group_ad_ad_demand_gen_multi_asset_ad_marketing_images, 'assets/(.*?)"'), []),
            IFNULL(REGEXP_EXTRACT_ALL(ad_group_ad_ad_demand_gen_multi_asset_ad_portrait_marketing_images, 'assets/(.*?)"'), []),
            IFNULL(REGEXP_EXTRACT_ALL(ad_group_ad_ad_demand_gen_multi_asset_ad_square_marketing_images, 'assets/(.*?)"'), []),
            IFNULL(REGEXP_EXTRACT_ALL(ad_group_ad_ad_demand_gen_video_responsive_ad_logo_images, 'assets/(.*?)"'), []),
            IFNULL(REGEXP_EXTRACT_ALL(ad_group_ad_ad_local_ad_logo_images, 'assets/(.*?)"'), []),
            IFNULL(REGEXP_EXTRACT_ALL(ad_group_ad_ad_local_ad_marketing_images, 'assets/(.*?)"'), []),
            IFNULL(REGEXP_EXTRACT_ALL(ad_group_ad_ad_responsive_display_ad_logo_images, 'assets/(.*?)"'), []),
            IFNULL(REGEXP_EXTRACT_ALL(ad_group_ad_ad_responsive_display_ad_marketing_images, 'assets/(.*?)"'), []),
            IFNULL(REGEXP_EXTRACT_ALL(ad_group_ad_ad_responsive_display_ad_square_logo_images, 'assets/(.*?)"'), []),
            IFNULL(REGEXP_EXTRACT_ALL(ad_group_ad_ad_responsive_display_ad_square_marketing_images, 'assets/(.*?)"'), []),
            IFNULL(REGEXP_EXTRACT_ALL(ad_group_ad_ad_video_responsive_ad_companion_banners, 'assets/(.*?)"'), []),
            IFNULL(REGEXP_EXTRACT_ALL(ad_group_ad_ad_video_ad_bumper_companion_banner_asset, 'assets/(.*?)$'), []),
            IFNULL(REGEXP_EXTRACT_ALL(ad_group_ad_ad_video_ad_in_stream_companion_banner_asset, 'assets/(.*?)$'), []),
            IFNULL(REGEXP_EXTRACT_ALL(ad_group_ad_ad_video_ad_non_skippable_companion_banner_asset, 'assets/(.*?)$'), []),
            IFNULL(REGEXP_EXTRACT_ALL(ad_group_ad_ad_demand_gen_product_ad_logo_image, 'assets/(.*?)"'), [])
        )) AS asset_id
    WHERE
        date BETWEEN date_from AND date_to

),
asset_url AS (
    -- 使用していない要素で重複が発生しているため、DISTINCTを追加
    SELECT DISTINCT
        date,
        customer_id,
        asset_id,
        url
    FROM
        `ht-data-management.hdy_databank_x1dl_export_enabled.googleads_asset_uploaded`,
        UNNEST([
            asset_dynamic_custom_asset_image_url,
            asset_dynamic_education_asset_image_url,
            asset_dynamic_education_asset_thumbnail_image_url,
            asset_dynamic_flights_asset_image_url,
            asset_dynamic_hotels_and_rentals_asset_image_url,
            asset_dynamic_jobs_asset_image_url,
            asset_dynamic_local_asset_image_url,
            asset_dynamic_real_estate_asset_image_url,
            asset_dynamic_travel_asset_image_url,
            asset_image_asset_full_size_url
        ]) AS url
    WHERE
        date BETWEEN date_from AND date_to
),
carousel_asset_uploaded AS (
    -- 使用していない要素で重複が発生しているため、DISTINCTを追加
    SELECT DISTINCT
        date,
        customer_id,
        asset_id AS carousel_card_asset_id,
        image_asset_id
    FROM
        `ht-data-management.hdy_databank_x1dl_export_enabled.googleads_asset_uploaded`,
        UNNEST(ARRAY_CONCAT(
            IFNULL(REGEXP_EXTRACT_ALL(asset_demand_gen_carousel_card_asset_marketing_image_asset, 'assets/(.*?)$'), []),
            IFNULL(REGEXP_EXTRACT_ALL(asset_demand_gen_carousel_card_asset_portrait_marketing_image_asset, 'assets/(.*?)$'), []),
            IFNULL(REGEXP_EXTRACT_ALL(asset_demand_gen_carousel_card_asset_square_marketing_image_asset, 'assets/(.*?)$'), [])
        )) AS image_asset_id
    WHERE
        date BETWEEN date_from AND date_to
),
uploaded2 AS (
    SELECT
        asset_uploaded.date,
        asset_uploaded.customer_id,
        asset_uploaded.carousel_card_asset_id AS asset_id,
        asset_url.url
    FROM
        carousel_asset_uploaded AS asset_uploaded
    INNER JOIN
        asset_url
    ON
        asset_url.date = asset_uploaded.date
        AND asset_url.customer_id = asset_uploaded.customer_id
        AND asset_url.asset_id = asset_uploaded.image_asset_id
),

uploaded AS (
    SELECT date, customer_id, asset_id, url FROM asset_url
    UNION ALL
    SELECT date, customer_id, asset_id, url FROM uploaded2
),

source2 AS (
    SELECT
        report.date AS report_date,
        report.customer_id AS client_id,
        uploaded.url AS creative_url,
        report.campaign_id,
        report.ad_group_id,
        report.ad_group_ad_ad_image_ad_image_url,
        report.ad_group_ad_ad_type,
        report.ad_group_ad_ad_image_ad_pixel_height AS image_height,
        report.ad_group_ad_ad_image_ad_pixel_width AS image_width,
        report.metrics_clicks AS clicks,
        report.metrics_impressions AS impressions,
        report.metrics_cost_micros AS cost
    FROM
        report
    INNER JOIN
        uploaded
    ON
        uploaded.date = report.date
        AND uploaded.customer_id = report.customer_id
        AND uploaded.asset_id = report.asset_id
    WHERE
        uploaded.url IS NOT NULL
),
source AS (
    SELECT * FROM source1
    UNION ALL
    SELECT * FROM source2
),
creative_meta AS (
    -- 取得時にDISTINCTを使用しているため、重複が発生している可能性がある
    SELECT DISTINCT
        report_date,
        creative_url,
        storage_uri
    FROM
        `ht-data-management.hdy_databank_x1dl_export_enabled.creative_metadata`
    WHERE
        report_date BETWEEN date_from AND date_to
        AND if_id = 'googleads.ad'
)

SELECT
    source.campaign_id,
    source.ad_group_id,
    ARRAY_REVERSE(SPLIT(ad_group_ad_ad_image_ad_image_url, "/"))[SAFE_OFFSET(0)] AS id,
    creative_meta.storage_uri AS path,
    ad_group_ad_ad_type AS type,
    EXTRACT(YEAR FROM source.report_date) AS year,
    EXTRACT(MONTH FROM source.report_date) AS month,
    EXTRACT(DAY FROM source.report_date) AS day,
    source.image_width AS width,
    source.image_height AS height,
    COALESCE(SUM(CAST(source.impressions AS INT64)), 0) AS impressions,
    COALESCE(SUM(CAST(source.clicks AS INT64)), 0) AS clicks,
    COALESCE(SUM(CAST(source.cost AS FLOAT64)), 0) AS costs
FROM
    source
INNER JOIN
    creative_meta
ON
    source.report_date = creative_meta.report_date
    AND source.creative_url = creative_meta.creative_url
WHERE
    ad_group_ad_ad_type = "IMAGE_AD"
GROUP BY
    campaign_id,
    ad_group_id,
    id,
    path,
    type,
    year,
    month,
    day,
    width,
    height
ORDER BY
    campaign_id,
    ad_group_id,
    id,
    path,
    type,
    year,
    month,
    day,
    width,
    height
