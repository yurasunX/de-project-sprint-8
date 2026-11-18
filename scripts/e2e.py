#!/usr/bin/env python3
"""Real kcat -> Spark -> PostgreSQL and Kafka acceptance test."""
import argparse
import json
import os
from pathlib import Path
import subprocess
import sys
import time
import uuid
import psycopg
from restaurant_streaming.config import Settings, load_env
from restaurant_streaming.manage import initialize
from restaurant_streaming.transforms import OUTPUT_FIELDS


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--env-file", required=True)
    args = parser.parse_args()
    load_env(args.env_file)
    settings = Settings.from_env()
    root = Path(__file__).resolve().parents[1]
    local = root / ".local"
    local.mkdir(exist_ok=True)
    with psycopg.connect(**settings.source.connect_options()) as conn:
        row = conn.execute("SELECT restaurant_id FROM " + settings.source_table +
                           " GROUP BY restaurant_id ORDER BY restaurant_id LIMIT 1").fetchone()
        if not row:
            raise RuntimeError("Source subscriptions table is empty")
        restaurant = row[0]
        clients = {r[0] for r in conn.execute("SELECT DISTINCT client_id FROM " +
                   settings.source_table + " WHERE restaurant_id = %s", (restaurant,)).fetchall()}
    initialize()
    now = int(time.time())
    campaigns = {}
    for state, start, end in [("active", now - 60, now + 3600),
                              ("future", now + 3600, now + 7200),
                              ("expired", now - 7200, now - 3600)]:
        campaigns[state] = dict(restaurant_id=restaurant, adv_campaign_id=str(uuid.uuid4()),
                               adv_campaign_content="E2E " + state, adv_campaign_owner="Demo",
                               adv_campaign_owner_contact="demo@example.invalid",
                               adv_campaign_datetime_start=start, adv_campaign_datetime_end=end,
                               datetime_created=now)
    command = [sys.executable, "-m", "restaurant_streaming.manage", "kafka"]
    subprocess.run(command + ["produce"], input="\n".join(json.dumps(m) for m in campaigns.values()) + "\n",
                   text=True, check=True, timeout=45)
    env = dict(os.environ, CHECKPOINT_LOCATION=str(local / ("e2e-checkpoint-" + uuid.uuid4().hex)))
    with (local / "e2e-spark.log").open("w") as log:
        subprocess.run([sys.executable, "-m", "restaurant_streaming.app", "--once"], env=env,
                       stdout=log, stderr=subprocess.STDOUT, check=True, timeout=300)
    consumed = subprocess.run(command + ["consume", "--topic", "output", "--count", "100000"],
                              text=True, capture_output=True, check=True, timeout=45)
    ids = {m["adv_campaign_id"] for m in campaigns.values()}
    messages = []
    for line in consumed.stdout.splitlines():
        message = json.loads(line)
        if message.get("adv_campaign_id") in ids:
            messages.append(message)
    active_id = campaigns["active"]["adv_campaign_id"]
    assert messages and all(m["adv_campaign_id"] == active_id for m in messages), "Inactive campaign leaked"
    assert {m["client_id"] for m in messages} == clients, "Kafka recipients mismatch"
    assert len(messages) == len(clients), "Unexpected duplicate Kafka notifications"
    assert all(set(m) == set(OUTPUT_FIELDS) for m in messages), "Kafka schema mismatch"
    with psycopg.connect(**settings.feedback.connect_options()) as conn:
        rows = conn.execute("SELECT adv_campaign_id, client_id, feedback FROM " + settings.feedback_table +
                            " WHERE adv_campaign_id = ANY(%s)", (list(ids),)).fetchall()
    assert {r[0] for r in rows} == {active_id}, "Inactive campaign in feedback table"
    assert {r[1] for r in rows} == clients and len(rows) == len(clients), "Postgres recipients mismatch"
    assert all(r[2] is None for r in rows), "Initial feedback must be NULL"
    # A second execution with the same checkpoint must not replay committed input.
    with (local / "e2e-resume.log").open("w") as log:
        subprocess.run([sys.executable, "-m", "restaurant_streaming.app", "--once"], env=env,
                       stdout=log, stderr=subprocess.STDOUT, check=True, timeout=300)
    resumed = subprocess.run(command + ["consume", "--topic", "output", "--count", "100000"],
                             text=True, capture_output=True, check=True, timeout=45)
    resumed_messages = [json.loads(line) for line in resumed.stdout.splitlines()]
    resumed_messages = [m for m in resumed_messages if m.get("adv_campaign_id") in ids]
    assert len(resumed_messages) == len(messages), "Checkpoint restart replayed input"
    with psycopg.connect(**settings.feedback.connect_options()) as conn:
        resumed_rows = conn.execute("SELECT adv_campaign_id, client_id, feedback FROM " + settings.feedback_table +
                                    " WHERE adv_campaign_id = ANY(%s)", (list(ids),)).fetchall()
    assert sorted(resumed_rows) == sorted(rows), "Checkpoint restart changed feedback rows"
    report = {"status": "passed", "expected_recipients": len(clients),
              "kafka_messages": len(messages), "feedback_rows": len(rows),
              "future_campaigns_delivered": 0, "expired_campaigns_delivered": 0,
              "checkpoint_resume": "passed",
              "checked_at_epoch": int(time.time())}
    (local / "e2e-report.json").write_text(json.dumps(report, indent=2) + "\n")
    print(json.dumps(report, indent=2))


if __name__ == "__main__":
    main()
