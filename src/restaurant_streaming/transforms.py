"""Pure Spark transformations, shared by the running stream and tests."""
from pyspark.sql import functions as F
from pyspark.sql.types import LongType, StringType, StructField, StructType

CAMPAIGN_FIELDS = ["restaurant_id", "adv_campaign_id", "adv_campaign_content",
                   "adv_campaign_owner", "adv_campaign_owner_contact",
                   "adv_campaign_datetime_start", "adv_campaign_datetime_end", "datetime_created"]
OUTPUT_FIELDS = CAMPAIGN_FIELDS + ["client_id", "trigger_datetime_created"]
EVENT_KEY = ["restaurant_id", "adv_campaign_id", "client_id"]
CAMPAIGN_SCHEMA = StructType([
    StructField(name, LongType() if name in CAMPAIGN_FIELDS[5:] else StringType(), True)
    for name in CAMPAIGN_FIELDS
])


def parse_campaigns(raw):
    parsed = raw.select(F.from_json(F.col("value").cast("string"), CAMPAIGN_SCHEMA).alias("campaign"))
    campaigns = parsed.select("campaign.*")
    valid = F.lit(True)
    for name in CAMPAIGN_FIELDS:
        valid = valid & F.col(name).isNotNull()
        if name not in CAMPAIGN_FIELDS[5:]:
            valid = valid & (F.length(F.trim(F.col(name))) > 0)
    return campaigns.filter(valid & (F.col("adv_campaign_datetime_end") >
                                     F.col("adv_campaign_datetime_start")))


def build_notifications(campaigns, subscriptions, as_of=None):
    now = F.current_timestamp().cast("long") if as_of is None else F.lit(as_of).cast("long")
    active = campaigns.filter((F.col("adv_campaign_datetime_start") <= now) &
                              (now < F.col("adv_campaign_datetime_end")))
    subscribers = subscriptions.select("restaurant_id", "client_id").filter(
        F.col("restaurant_id").isNotNull() & F.col("client_id").isNotNull() &
        (F.length(F.trim(F.col("client_id"))) > 0)).dropDuplicates()
    return (active.join(subscribers, on="restaurant_id", how="inner")
            .withColumn("trigger_datetime_created", now)
            .select(*OUTPUT_FIELDS))


def kafka_values(notifications):
    # feedback and database-generated id never enter the notification payload.
    return notifications.select(F.to_json(F.struct(*[F.col(n) for n in OUTPUT_FIELDS])).alias("value"))
