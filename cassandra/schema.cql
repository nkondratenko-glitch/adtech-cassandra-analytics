CREATE KEYSPACE IF NOT EXISTS adtech_cassandra
WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1};

USE adtech_cassandra;

CREATE TABLE IF NOT EXISTS campaign_daily_metrics_by_campaign (
    campaign_id INT,
    event_date DATE,
    campaign_name TEXT,
    advertiser_id INT,
    advertiser_name TEXT,
    impressions COUNTER,
    clicks COUNTER,
    PRIMARY KEY ((campaign_id), event_date)
) WITH CLUSTERING ORDER BY (event_date DESC);

CREATE TABLE IF NOT EXISTS campaign_daily_metrics_snapshot_by_campaign (
    campaign_id INT,
    event_date DATE,
    campaign_name TEXT,
    advertiser_id INT,
    advertiser_name TEXT,
    impressions BIGINT,
    clicks BIGINT,
    spend DECIMAL,
    ctr DECIMAL,
    PRIMARY KEY ((campaign_id), event_date)
) WITH CLUSTERING ORDER BY (event_date DESC);

CREATE TABLE IF NOT EXISTS user_ad_history_by_user (
    user_id INT,
    event_time TIMESTAMP,
    impression_id BIGINT,
    campaign_id INT,
    campaign_name TEXT,
    advertiser_id INT,
    advertiser_name TEXT,
    device_type TEXT,
    clicked BOOLEAN,
    click_time TIMESTAMP,
    impression_cost DECIMAL,
    cpc_amount DECIMAL,
    PRIMARY KEY ((user_id), event_time, impression_id)
) WITH CLUSTERING ORDER BY (event_time DESC, impression_id DESC);

CREATE TABLE IF NOT EXISTS advertiser_spend_by_day (
    spend_date DATE,
    advertiser_id INT,
    advertiser_name TEXT,
    region TEXT,
    total_spend DECIMAL,
    total_impressions BIGINT,
    total_clicks BIGINT,
    PRIMARY KEY ((spend_date), advertiser_id, region)
);

CREATE TABLE IF NOT EXISTS advertiser_spend_30d_snapshot (
    snapshot_date DATE,
    rank_bucket TEXT,
    total_spend DECIMAL,
    advertiser_id INT,
    advertiser_name TEXT,
    total_impressions BIGINT,
    total_clicks BIGINT,
    PRIMARY KEY ((snapshot_date, rank_bucket), total_spend, advertiser_id)
) WITH CLUSTERING ORDER BY (total_spend DESC, advertiser_id ASC);

CREATE TABLE IF NOT EXISTS user_clicks_30d_snapshot (
    snapshot_date DATE,
    rank_bucket TEXT,
    total_clicks BIGINT,
    user_id INT,
    PRIMARY KEY ((snapshot_date, rank_bucket), total_clicks, user_id)
) WITH CLUSTERING ORDER BY (total_clicks DESC, user_id ASC);

CREATE TABLE IF NOT EXISTS advertiser_spend_30d_by_region_snapshot (
    snapshot_date DATE,
    region TEXT,
    rank_bucket TEXT,
    total_spend DECIMAL,
    advertiser_id INT,
    advertiser_name TEXT,
    PRIMARY KEY ((snapshot_date, region, rank_bucket), total_spend, advertiser_id)
) WITH CLUSTERING ORDER BY (total_spend DESC, advertiser_id ASC);
