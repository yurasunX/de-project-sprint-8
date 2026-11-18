"""Optional real PostgreSQL test, isolated in a temporary schema."""
import os
from pathlib import Path
import unittest
import uuid
from restaurant_streaming.config import Postgres
from restaurant_streaming.sinks import write_partition


@unittest.skipUnless(os.getenv("RUN_POSTGRES_TESTS") == "1", "real PostgreSQL not configured")
class PostgresTests(unittest.TestCase):
    def test_duplicate_delivery_keeps_user_feedback(self):
        import psycopg
        postgres = Postgres.from_env("FEEDBACK_PG_")
        schema = "test_streaming_" + uuid.uuid4().hex
        table = schema + ".subscribers_feedback"
        ddl = (Path(__file__).resolve().parents[1] / "sql/01_feedback.sql").read_text()
        ddl = ddl.replace("public.subscribers_feedback", table)
        event = dict(restaurant_id="test-restaurant", adv_campaign_id="test-campaign",
                     adv_campaign_content="Demo", adv_campaign_owner="Demo",
                     adv_campaign_owner_contact="demo@example.invalid",
                     adv_campaign_datetime_start=100, adv_campaign_datetime_end=2659207116,
                     datetime_created=99, client_id="test-client", trigger_datetime_created=101)
        try:
            with psycopg.connect(**postgres.connect_options()) as conn:
                conn.execute("CREATE SCHEMA " + schema)
                conn.execute(ddl)
            write_partition(iter([event]), postgres, table)
            with psycopg.connect(**postgres.connect_options()) as conn:
                conn.execute("UPDATE " + table + " SET feedback = 'liked'")
            write_partition(iter([event, event]), postgres, table)
            with psycopg.connect(**postgres.connect_options()) as conn:
                rows = conn.execute("SELECT feedback, adv_campaign_datetime_end FROM " + table).fetchall()
            self.assertEqual(rows, [("liked", 2659207116)])
        finally:
            with psycopg.connect(**postgres.connect_options()) as conn:
                conn.execute("DROP SCHEMA IF EXISTS " + schema + " CASCADE")
