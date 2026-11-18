import json
import unittest
from pyspark.sql import SparkSession, functions as F
from restaurant_streaming.transforms import (CAMPAIGN_FIELDS, OUTPUT_FIELDS,
                                              build_notifications, kafka_values, parse_campaigns)
from restaurant_streaming.sinks import NotificationSink, insert_sql
from restaurant_streaming.config import Kafka, Postgres, identifier

NOW = 1795000000


def campaign(**updates):
    result = dict(restaurant_id="r1", adv_campaign_id="campaign-1",
                  adv_campaign_content="Offer", adv_campaign_owner="Demo",
                  adv_campaign_owner_contact="demo@example.invalid",
                  adv_campaign_datetime_start=NOW - 10,
                  adv_campaign_datetime_end=NOW + 10, datetime_created=NOW - 20)
    result.update(updates)
    return result


class PipelineTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.spark = (SparkSession.builder.master("local[2]").appName("project-tests")
                     .config("spark.ui.enabled", "false")
                     .config("spark.sql.session.timeZone", "UTC")
                     .config("spark.sql.shuffle.partitions", "2").getOrCreate())
        cls.spark.sparkContext.setLogLevel("ERROR")

    @classmethod
    def tearDownClass(cls):
        cls.spark.stop()

    def raw(self, records):
        return self.spark.createDataFrame([(r.encode(),) for r in records], "value binary")

    def subscriptions(self):
        return self.spark.createDataFrame([("r1", "c1"), ("r1", "c2"), ("r2", "c3"),
                                           ("r1", "c1")], "restaurant_id string, client_id string")

    def notifications(self, campaigns=None):
        records = campaigns if campaigns is not None else [campaign()]
        parsed = parse_campaigns(self.raw([json.dumps(c) for c in records]))
        return build_notifications(parsed, self.subscriptions(), as_of=NOW)

    def test_only_subscribers_of_correct_restaurant(self):
        result = self.notifications()
        self.assertEqual({r.client_id for r in result.collect()}, {"c1", "c2"})
        self.assertEqual(result.columns, OUTPUT_FIELDS)
        self.assertEqual(result.count(), 2)

    def test_time_boundaries_and_expired_future(self):
        records = [campaign(adv_campaign_id="start", adv_campaign_datetime_start=NOW),
                   campaign(adv_campaign_id="end", adv_campaign_datetime_end=NOW),
                   campaign(adv_campaign_id="future", adv_campaign_datetime_start=NOW + 1),
                   campaign(adv_campaign_id="expired", adv_campaign_datetime_start=NOW - 20,
                            adv_campaign_datetime_end=NOW - 1)]
        rows = self.notifications(records).collect()
        self.assertEqual(len(rows), 2)
        self.assertEqual({r.adv_campaign_id for r in rows}, {"start"})
        self.assertTrue(all(r.trigger_datetime_created == NOW for r in rows))

    def test_no_subscribers(self):
        self.assertEqual(self.notifications([campaign(restaurant_id="missing")]).count(), 0)

    def test_malformed_missing_fields_and_invalid_intervals(self):
        missing = campaign()
        del missing["adv_campaign_owner"]
        records = ["not json", "null", "{}", json.dumps(missing),
                   json.dumps(campaign(restaurant_id=" ")),
                   json.dumps(campaign(adv_campaign_datetime_start=NOW + 10))]
        self.assertEqual(parse_campaigns(self.raw(records)).count(), 0)

    def test_epochs_beyond_int32(self):
        rows = self.notifications([campaign(adv_campaign_datetime_end=2659207116)]).collect()
        self.assertTrue(all(r.adv_campaign_datetime_end == 2659207116 for r in rows))
        self.assertEqual(dict(self.notifications().dtypes)["adv_campaign_datetime_end"], "bigint")

    def test_kafka_only_json_value_no_feedback_or_database_id(self):
        df = self.notifications().withColumn("feedback", F.lit("not for Kafka")).withColumn("id", F.lit(5))
        result = kafka_values(df)
        self.assertEqual(result.dtypes, [("value", "string")])
        payload = json.loads(result.first().value)
        self.assertEqual(set(payload), set(OUTPUT_FIELDS))
        self.assertEqual(payload["trigger_datetime_created"], NOW)
        self.assertNotIn("feedback", payload)
        self.assertNotIn("id", payload)

    def test_sink_deduplicates_and_caches_both_writes(self):
        calls = []
        refs = []
        def writer(name):
            def perform(df, _settings):
                refs.append(df)
                calls.append((name, df.count(), df.is_cached))
            return perform
        df = self.notifications()
        NotificationSink(None, writer("postgres"), writer("kafka"))(df.unionByName(df), 7)
        self.assertEqual(calls, [("postgres", 2, True), ("kafka", 2, True)])
        self.assertTrue(all(not df.is_cached for df in refs))

    def test_cache_released_when_second_sink_fails(self):
        refs = []
        def first(df, _):
            refs.append(df)
        def fail(df, _):
            raise RuntimeError("simulated Kafka outage")
        with self.assertRaisesRegex(RuntimeError, "simulated Kafka outage"):
            NotificationSink(None, first, fail)(self.notifications(), 8)
        self.assertFalse(refs[0].is_cached)

    def test_empty_batch_does_not_write(self):
        def fail(*_):
            self.fail("empty batch reached writer")
        NotificationSink(None, fail, fail)(self.notifications().limit(0), 9)


class ConfigurationTests(unittest.TestCase):
    def test_passwords_absent_from_repr(self):
        postgres = Postgres("localhost", 5432, "de", "private-user", "private-secret")
        kafka = Kafka("localhost:9092", "in", "out", username="private-user", password="private-secret")
        self.assertNotIn("private-secret", repr(postgres) + repr(kafka))
        self.assertNotIn("private-user", repr(postgres) + repr(kafka))

    def test_sql_identifier_validation(self):
        self.assertEqual(identifier("subscribers_feedback"), "subscribers_feedback")
        with self.assertRaises(ValueError):
            identifier("feedback; DROP TABLE other")

    def test_upsert_does_not_overwrite_feedback(self):
        sql = insert_sql("public.subscribers_feedback")
        self.assertIn("ON CONFLICT (restaurant_id, adv_campaign_id, client_id) DO NOTHING", sql)
        self.assertIn("NULL", sql)
        self.assertEqual(sql.count("%s"), len(OUTPUT_FIELDS))


if __name__ == "__main__":
    unittest.main()
