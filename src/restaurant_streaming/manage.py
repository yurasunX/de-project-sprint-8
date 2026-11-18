"""Database setup and kcat helpers. Never prints credentials."""
import argparse
import os
from pathlib import Path
import shutil
import subprocess
import tempfile
from .config import Kafka, Postgres, identifier, load_env

ROOT = Path(__file__).resolve().parents[2]


def initialize(demo=False):
    import psycopg
    feedback = Postgres.from_env("FEEDBACK_PG_")
    feedback_table = os.getenv("FEEDBACK_TABLE", "public.subscribers_feedback")
    source_table = os.getenv("SOURCE_TABLE", "public.subscribers_restaurants")
    for table in (feedback_table, source_table):
        parts = table.split(".")
        if len(parts) != 2:
            raise ValueError("Table must be schema.table")
        for part in parts:
            identifier(part)
    with psycopg.connect(**feedback.connect_options()) as conn:
        conn.execute((ROOT / "sql/01_feedback.sql").read_text().replace(
            "public.subscribers_feedback", feedback_table))
    if demo:
        source = Postgres.from_env("SOURCE_PG_")
        with psycopg.connect(**source.connect_options()) as conn:
            conn.execute((ROOT / "sql/02_demo_subscribers.sql").read_text().replace(
                "public.subscribers_restaurants", source_table))
    print("Database preparation complete")


def kcat(mode, topic, count):
    kafka = Kafka.from_env()
    binary = shutil.which("kcat") or shutil.which("kafkacat")
    if not binary:
        raise SystemExit("Install kcat in this environment")
    options = {"bootstrap.servers": kafka.brokers, "security.protocol": kafka.protocol}
    if kafka.protocol.startswith("SASL"):
        options.update({"sasl.mechanisms": kafka.mechanism,
                        "sasl.username": kafka.username, "sasl.password": kafka.password})
    ca = os.getenv("KAFKA_CA_LOCATION", "")
    if ca:
        options["ssl.ca.location"] = ca
    for value in options.values():
        if "\n" in value or "\r" in value:
            raise ValueError("Multiline Kafka configuration values are not supported")
    with tempfile.NamedTemporaryFile(mode="w", prefix="kcat-", delete=True) as conf:
        os.chmod(conf.name, 0o600)
        conf.write("\n".join(k + "=" + v for k, v in options.items()) + "\n")
        conf.flush()
        command = [binary, "-F", conf.name, "-m", "20"]
        if mode == "metadata":
            command += ["-L"]
        else:
            command += ["-t", kafka.input_topic if topic == "input" else kafka.output_topic]
            if mode == "produce":
                command += ["-P"]
            else:
                command += ["-C", "-o", "beginning", "-e", "-c", str(count)]
        return subprocess.call(command)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--env-file")
    commands = parser.add_subparsers(dest="command", required=True)
    init = commands.add_parser("init-db")
    init.add_argument("--demo", action="store_true", help="Seed only your own demo source database")
    client = commands.add_parser("kafka")
    client.add_argument("mode", choices=["metadata", "produce", "consume"])
    client.add_argument("--topic", choices=["input", "output"], default="input")
    client.add_argument("--count", type=int, default=20)
    args = parser.parse_args()
    load_env(args.env_file)
    if args.command == "init-db":
        initialize(args.demo)
        return 0
    return kcat(args.mode, args.topic, args.count)


if __name__ == "__main__":
    raise SystemExit(main())
