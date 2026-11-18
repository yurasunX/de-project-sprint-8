"""Two batch sinks; SQL writes are idempotent, Kafka delivery is at-least-once."""
import logging
from functools import partial
from pyspark import StorageLevel
from .transforms import EVENT_KEY, OUTPUT_FIELDS, kafka_values

LOG = logging.getLogger(__name__)


def insert_sql(table):
    columns = ", ".join(OUTPUT_FIELDS + ["feedback"])
    values = ", ".join(["%s"] * len(OUTPUT_FIELDS) + ["NULL"])
    return ("INSERT INTO " + table + " (" + columns + ") VALUES (" + values + ") "
            "ON CONFLICT (restaurant_id, adv_campaign_id, client_id) DO NOTHING")


def write_partition(rows, postgres, table):
    import psycopg
    iterator = iter(rows)
    first = next(iterator, None)
    if first is None:
        return
    with psycopg.connect(**postgres.connect_options()) as conn:
        with conn.cursor() as cursor:
            chunk = [tuple(first[n] for n in OUTPUT_FIELDS)]
            for row in iterator:
                chunk.append(tuple(row[n] for n in OUTPUT_FIELDS))
                if len(chunk) >= 500:
                    cursor.executemany(insert_sql(table), chunk)
                    chunk = []
            if chunk:
                cursor.executemany(insert_sql(table), chunk)


def write_postgres(df, settings):
    df.select(*OUTPUT_FIELDS).foreachPartition(partial(
        write_partition, postgres=settings.feedback, table=settings.feedback_table))


def write_kafka(df, settings):
    options = settings.kafka.spark_options()
    options.pop("kafka.group.id", None)  # Consumer ACLs do not apply to the producer.
    (kafka_values(df).write.format("kafka")
     .options(**options)
     .option("topic", settings.kafka.output_topic).save())


class NotificationSink:
    def __init__(self, settings, postgres_writer=write_postgres, kafka_writer=write_kafka):
        self.settings = settings
        self.postgres_writer = postgres_writer
        self.kafka_writer = kafka_writer

    def __call__(self, batch, batch_id):
        cached = batch.dropDuplicates(EVENT_KEY).persist(StorageLevel.MEMORY_AND_DISK)
        try:
            count = cached.count()
            if not count:
                LOG.info("batch=%s rows=0", batch_id)
                return
            self.postgres_writer(cached, self.settings)
            self.kafka_writer(cached, self.settings)
            LOG.info("batch=%s rows=%s delivered_to=postgres,kafka", batch_id, count)
        finally:
            cached.unpersist()
