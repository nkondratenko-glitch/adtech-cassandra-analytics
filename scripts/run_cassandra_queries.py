import json
import os
from pathlib import Path
from cassandra.cluster import Cluster

CASSANDRA_HOST = os.getenv("CASSANDRA_HOST", "127.0.0.1")
CASSANDRA_PORT = int(os.getenv("CASSANDRA_PORT", "9042"))
KEYSPACE = os.getenv("CASSANDRA_KEYSPACE", "adtech_cassandra")
OUTPUT_DIR = Path("cassandra_reports")
OUTPUT_DIR.mkdir(exist_ok=True)

DEMO_CAMPAIGN_ID = int(os.getenv("DEMO_CAMPAIGN_ID", "1"))
DEMO_USER_ID = int(os.getenv("DEMO_USER_ID", "1"))
DEMO_REGION = os.getenv("DEMO_REGION", "USA")
SNAPSHOT_DATE = os.getenv("SNAPSHOT_DATE", "2024-10-31")


def rows_to_dicts(result):
    return [dict(row._asdict()) for row in result]


def main():
    cluster = Cluster([CASSANDRA_HOST], port=CASSANDRA_PORT)
    session = cluster.connect(KEYSPACE)

    queries = {
        "q1_ctr_per_campaign_per_day": f"SELECT campaign_id, event_date, campaign_name, impressions, clicks, ctr, spend FROM campaign_daily_metrics_snapshot_by_campaign WHERE campaign_id = {DEMO_CAMPAIGN_ID}",
        "q2_top_5_advertisers_by_spend_30d": f"SELECT advertiser_id, advertiser_name, total_spend, total_impressions, total_clicks FROM advertiser_spend_30d_snapshot WHERE snapshot_date = '{SNAPSHOT_DATE}' AND rank_bucket = 'all' LIMIT 5",
        "q3_last_10_ads_seen_by_user": f"SELECT user_id, event_time, impression_id, campaign_id, campaign_name, device_type, clicked, click_time FROM user_ad_history_by_user WHERE user_id = {DEMO_USER_ID} LIMIT 10",
        "q4_top_10_users_by_clicks_30d": f"SELECT user_id, total_clicks FROM user_clicks_30d_snapshot WHERE snapshot_date = '{SNAPSHOT_DATE}' AND rank_bucket = 'all' LIMIT 10",
        "q5_top_5_advertisers_by_region_30d": f"SELECT advertiser_id, advertiser_name, total_spend FROM advertiser_spend_30d_by_region_snapshot WHERE snapshot_date = '{SNAPSHOT_DATE}' AND region = '{DEMO_REGION}' AND rank_bucket = 'all' LIMIT 5",
    }

    all_results = {}
    for name, query in queries.items():
        all_results[name] = rows_to_dicts(session.execute(query))

    (OUTPUT_DIR / "cassandra_report.json").write_text(json.dumps(all_results, indent=2, default=str), encoding="utf-8")
    print("Cassandra report written to cassandra_reports/cassandra_report.json")
    cluster.shutdown()


if __name__ == "__main__":
    main()
