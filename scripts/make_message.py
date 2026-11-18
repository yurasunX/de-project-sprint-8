#!/usr/bin/env python3
"""Generate a fresh demonstration campaign. No embedded historical dates."""
import argparse
import json
import time
import uuid

parser = argparse.ArgumentParser(description=__doc__)
parser.add_argument("--restaurant-id", default="123e4567-e89b-12d3-a456-426614174000")
parser.add_argument("--campaign-id", default=None)
parser.add_argument("--state", choices=["active", "future", "expired"], default="active")
parser.add_argument("--with-key", action="store_true")
args = parser.parse_args()
now = int(time.time())
intervals = {"active": (now - 60, now + 3600), "future": (now + 3600, now + 7200),
             "expired": (now - 7200, now - 3600)}
start, end = intervals[args.state]
message = dict(restaurant_id=args.restaurant_id, adv_campaign_id=args.campaign_id or str(uuid.uuid4()),
               adv_campaign_content="Demonstration offer", adv_campaign_owner="Demo restaurant",
               adv_campaign_owner_contact="demo@example.invalid", adv_campaign_datetime_start=start,
               adv_campaign_datetime_end=end, datetime_created=now)
print(("campaign:" if args.with_key else "") + json.dumps(message, ensure_ascii=False))
