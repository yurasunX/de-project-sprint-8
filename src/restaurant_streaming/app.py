"""Entry point: python -m restaurant_streaming.app --env-file .local/project.env."""
import argparse
import logging
from pathlib import Path
import pyspark
from pyspark.sql import SparkSession
from pyspark import StorageLevel
from .config import Settings, load_env
from .sinks import NotificationSink
from .transforms import build_notifications, parse_campaigns


def create_spark(settings):
    version = pyspark.__version__
    scala = "2.13" if int(version.split(".")[0]) >= 4 else "2.12"
    packages = settings.packages or ("org.apache.spark:spark-sql-kafka-0-10_%s:%s," %
                                    (scala, version) + "org.postgresql:postgresql:42.7.5")
    return (SparkSession.builder.appName("RestaurantSubscribeStreamingService")
            .master(settings.master).config("spark.sql.session.timeZone", "UTC")
            .config("spark.sql.shuffle.partitions", "4")
            .config("spark.jars.packages", packages).getOrCreate())


def run(settings, once=False):
    spark = create_spark(settings)
    query = None
    subscriptions = None
    try:
        subscriptions = (spark.read.format("jdbc")
                         .options(**settings.source.jdbc_options(settings.source_table)).load()
                         .persist(StorageLevel.MEMORY_AND_DISK))
        subscriptions.count()  # Materialize a consistent subscription snapshot at startup.
        raw = (spark.readStream.format("kafka").options(**settings.kafka.spark_options())
               .option("subscribe", settings.kafka.input_topic)
               .option("startingOffsets", settings.starting_offsets).load())
        result = build_notifications(parse_campaigns(raw), subscriptions)
        writer = (result.writeStream.outputMode("append")
                  .option("checkpointLocation", settings.checkpoint)
                  .foreachBatch(NotificationSink(settings)))
        writer = writer.trigger(availableNow=True) if once else writer.trigger(processingTime=settings.trigger)
        query = writer.start()
        query.awaitTermination()
    finally:
        if query is not None:
            query.stop()
        if subscriptions is not None:
            subscriptions.unpersist()
        spark.stop()


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--env-file", type=Path)
    parser.add_argument("--once", action="store_true", help="Process available messages, then exit")
    args = parser.parse_args()
    load_env(args.env_file)
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    settings = Settings.from_env()
    run(settings, args.once)


if __name__ == "__main__":
    main()
