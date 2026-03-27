USE adtech_cassandra;

SELECT campaign_id, event_date, campaign_name, impressions, clicks, ctr, spend
FROM campaign_daily_metrics_snapshot_by_campaign
WHERE campaign_id = 1;

SELECT advertiser_id, advertiser_name, total_spend, total_impressions, total_clicks
FROM advertiser_spend_30d_snapshot
WHERE snapshot_date = '2024-10-31'
  AND rank_bucket = 'all'
LIMIT 5;

SELECT user_id, event_time, impression_id, campaign_id, campaign_name, device_type, clicked, click_time
FROM user_ad_history_by_user
WHERE user_id = 1
LIMIT 10;

SELECT user_id, total_clicks
FROM user_clicks_30d_snapshot
WHERE snapshot_date = '2024-10-31'
  AND rank_bucket = 'all'
LIMIT 10;

SELECT advertiser_id, advertiser_name, total_spend
FROM advertiser_spend_30d_by_region_snapshot
WHERE snapshot_date = '2024-10-31'
  AND region = 'USA'
  AND rank_bucket = 'all'
LIMIT 5;
