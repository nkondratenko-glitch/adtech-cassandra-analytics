import os
from collections import defaultdict
from datetime import timedelta
from decimal import Decimal

import mysql.connector
from cassandra.cluster import Cluster

MYSQL_CONFIG = {
    "host": os.getenv("MYSQL_HOST", "127.0.0.1"),
    "port": int(os.getenv("MYSQL_PORT", "3306")),
    "user": os.getenv("MYSQL_USER", "root"),
    "password": os.getenv("MYSQL_PASSWORD", "root"),
    "database": os.getenv("MYSQL_DATABASE", "adtech"),
}

CASSANDRA_HOST = os.getenv("CASSANDRA_HOST", "127.0.0.1")
CASSANDRA_PORT = int(os.getenv("CASSANDRA_PORT", "9042"))
CASSANDRA_KEYSPACE = os.getenv("CASSANDRA_KEYSPACE", "adtech_cassandra")


def mysql_conn():
    return mysql.connector.connect(**MYSQL_CONFIG)


def cassandra_session():
    cluster = Cluster([CASSANDRA_HOST], port=CASSANDRA_PORT)
    session = cluster.connect(CASSANDRA_KEYSPACE)
    return cluster, session


def table_has_column(cur, table_name: str, column_name: str) -> bool:
    cur.execute("""
        SELECT COUNT(*)
        FROM information_schema.columns
        WHERE table_schema = DATABASE()
          AND table_name = %s
          AND column_name = %s
    """, (table_name, column_name))
    return cur.fetchone()[0] > 0


def fetch_campaign_map(cur):
    cur.execute("""
        SELECT c.campaign_id, c.campaign_name, c.advertiser_id, a.advertiser_name
        FROM campaigns c
        JOIN advertisers a ON a.advertiser_id = c.advertiser_id
    """)
    return {
        row[0]: {
            "campaign_name": row[1],
            "advertiser_id": row[2],
            "advertiser_name": row[3],
        }
        for row in cur.fetchall()
    }


def fetch_user_region_map(cur):
    cur.execute("""
        SELECT u.user_id, COALESCE(l.location_name, 'Unknown') AS region
        FROM users u
        LEFT JOIN locations l ON l.location_id = u.location_id
    """)
    return {row[0]: row[1] for row in cur.fetchall()}


def fetch_events(cur):
    has_device_type = table_has_column(cur, "impressions", "device_type")
    device_expr = "i.device_type" if has_device_type else "'unknown'"
    cur.execute(f"""
        SELECT
            i.impression_id,
            i.campaign_id,
            i.user_id,
            i.impression_time,
            i.impression_cost,
            {device_expr} AS device_type,
            cl.click_id,
            cl.click_time,
            cl.cpc_amount
        FROM impressions i
        LEFT JOIN clicks cl ON cl.impression_id = i.impression_id
        ORDER BY i.impression_time, i.impression_id, cl.click_time
    """)
    return cur.fetchall()


def main():
    mysql = mysql_conn()
    cur = mysql.cursor()

    campaign_map = fetch_campaign_map(cur)
    user_region = fetch_user_region_map(cur)
    rows = fetch_events(cur)

    cluster, session = cassandra_session()

    for table in [
        "campaign_daily_metrics_snapshot_by_campaign",
        "user_ad_history_by_user",
        "advertiser_spend_by_day",
        "advertiser_spend_30d_snapshot",
        "user_clicks_30d_snapshot",
        "advertiser_spend_30d_by_region_snapshot",
    ]:
        session.execute(f"TRUNCATE {table}")

    daily_campaign = defaultdict(lambda: {"impressions": 0, "clicks": 0, "spend": Decimal("0")})
    daily_advertiser_region = defaultdict(lambda: {"spend": Decimal("0"), "impressions": 0, "clicks": 0})
    user_clicks_by_day = defaultdict(int)

    insert_user_history = session.prepare("""
        INSERT INTO user_ad_history_by_user (
            user_id, event_time, impression_id, campaign_id, campaign_name,
            advertiser_id, advertiser_name, device_type, clicked, click_time,
            impression_cost, cpc_amount
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """)

    seen_impressions = set()

    for row in rows:
        impression_id, campaign_id, user_id, impression_time, impression_cost, device_type, click_id, click_time, cpc_amount = row
        if campaign_id not in campaign_map:
            continue

        event_date = impression_time.date()
        campaign = campaign_map[campaign_id]
        region = user_region.get(user_id, "Unknown")

        key = (campaign_id, event_date)
        if impression_id not in seen_impressions:
            daily_campaign[key]["impressions"] += 1
            daily_campaign[key]["spend"] += Decimal(str(impression_cost or 0))

            adv_key = (event_date, campaign["advertiser_id"], campaign["advertiser_name"], region)
            daily_advertiser_region[adv_key]["spend"] += Decimal(str(impression_cost or 0))
            daily_advertiser_region[adv_key]["impressions"] += 1

            session.execute(insert_user_history, (
                int(user_id),
                impression_time,
                int(impression_id),
                int(campaign_id),
                campaign["campaign_name"],
                int(campaign["advertiser_id"]),
                campaign["advertiser_name"],
                device_type or "unknown",
                bool(click_id is not None),
                click_time,
                Decimal(str(impression_cost or 0)),
                Decimal(str(cpc_amount or 0)),
            ))
            seen_impressions.add(impression_id)

        if click_id is not None:
            daily_campaign[key]["clicks"] += 1
            adv_key = (event_date, campaign["advertiser_id"], campaign["advertiser_name"], region)
            daily_advertiser_region[adv_key]["clicks"] += 1
            user_clicks_by_day[(click_time.date(), int(user_id))] += 1

    insert_campaign_day = session.prepare("""
        INSERT INTO campaign_daily_metrics_snapshot_by_campaign (
            campaign_id, event_date, campaign_name, advertiser_id, advertiser_name,
            impressions, clicks, spend, ctr
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
    """)

    for (campaign_id, event_date), metrics in daily_campaign.items():
        campaign = campaign_map[campaign_id]
        impressions = metrics["impressions"]
        clicks = metrics["clicks"]
        ctr = Decimal("0")
        if impressions:
            ctr = (Decimal(clicks) / Decimal(impressions)) * Decimal("100")
        session.execute(insert_campaign_day, (
            int(campaign_id),
            event_date,
            campaign["campaign_name"],
            int(campaign["advertiser_id"]),
            campaign["advertiser_name"],
            int(impressions),
            int(clicks),
            metrics["spend"],
            ctr.quantize(Decimal("0.01")),
        ))

    insert_adv_day = session.prepare("""
        INSERT INTO advertiser_spend_by_day (
            spend_date, advertiser_id, advertiser_name, region, total_spend, total_impressions, total_clicks
        ) VALUES (?, ?, ?, ?, ?, ?, ?)
    """)

    for (spend_date, advertiser_id, advertiser_name, region), metrics in daily_advertiser_region.items():
        session.execute(insert_adv_day, (
            spend_date,
            int(advertiser_id),
            advertiser_name,
            region,
            metrics["spend"],
            int(metrics["impressions"]),
            int(metrics["clicks"]),
        ))

    if daily_advertiser_region:
        all_dates = sorted({d for (d, _, _, _) in daily_advertiser_region.keys()})
        last_date = all_dates[-1]
        window_start = last_date - timedelta(days=29)

        advertiser_rollup = defaultdict(lambda: {"spend": Decimal("0"), "impressions": 0, "clicks": 0, "name": None})
        region_rollup = defaultdict(lambda: {"spend": Decimal("0"), "name": None})

        for (spend_date, advertiser_id, advertiser_name, region), metrics in daily_advertiser_region.items():
            if window_start <= spend_date <= last_date:
                advertiser_rollup[advertiser_id]["spend"] += metrics["spend"]
                advertiser_rollup[advertiser_id]["impressions"] += metrics["impressions"]
                advertiser_rollup[advertiser_id]["clicks"] += metrics["clicks"]
                advertiser_rollup[advertiser_id]["name"] = advertiser_name

                region_rollup[(region, advertiser_id)]["spend"] += metrics["spend"]
                region_rollup[(region, advertiser_id)]["name"] = advertiser_name

        insert_adv_30d = session.prepare("""
            INSERT INTO advertiser_spend_30d_snapshot (
                snapshot_date, rank_bucket, total_spend, advertiser_id, advertiser_name, total_impressions, total_clicks
            ) VALUES (?, ?, ?, ?, ?, ?, ?)
        """)

        for advertiser_id, metrics in advertiser_rollup.items():
            session.execute(insert_adv_30d, (
                last_date,
                "all",
                metrics["spend"],
                int(advertiser_id),
                metrics["name"],
                int(metrics["impressions"]),
                int(metrics["clicks"]),
            ))

        insert_adv_region_30d = session.prepare("""
            INSERT INTO advertiser_spend_30d_by_region_snapshot (
                snapshot_date, region, rank_bucket, total_spend, advertiser_id, advertiser_name
            ) VALUES (?, ?, ?, ?, ?, ?)
        """)

        for (region, advertiser_id), metrics in region_rollup.items():
            session.execute(insert_adv_region_30d, (
                last_date,
                region,
                "all",
                metrics["spend"],
                int(advertiser_id),
                metrics["name"],
            ))

        user_rollup = defaultdict(int)
        for (click_day, user_id), total_clicks in user_clicks_by_day.items():
            if window_start <= click_day <= last_date:
                user_rollup[user_id] += total_clicks

        insert_user_30d = session.prepare("""
            INSERT INTO user_clicks_30d_snapshot (
                snapshot_date, rank_bucket, total_clicks, user_id
            ) VALUES (?, ?, ?, ?)
        """)

        for user_id, total_clicks in user_rollup.items():
            session.execute(insert_user_30d, (
                last_date,
                "all",
                int(total_clicks),
                int(user_id),
            ))

        print(f"Loaded Cassandra data. Snapshot date: {last_date}")
    else:
        print("No impression/click event data found.")

    cur.close()
    mysql.close()
    cluster.shutdown()


if __name__ == "__main__":
    main()
