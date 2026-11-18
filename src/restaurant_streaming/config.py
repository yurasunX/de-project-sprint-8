"""Configuration from environment; passwords never appear in object reprs."""
import os
import re
import shlex
import uuid
from dataclasses import dataclass, field
from pathlib import Path


def load_env(path):
    if not path:
        return
    for line in Path(path).read_text().splitlines():
        line = line.strip()
        if not line or line.startswith("#"):
            continue
        key, separator, value = line.partition("=")
        if not separator or not re.fullmatch(r"[A-Z][A-Z0-9_]*", key.strip()):
            raise ValueError("Invalid environment-file assignment")
        parts = shlex.split(value, comments=True)
        if len(parts) > 1:
            raise ValueError("Quote environment values containing spaces")
        os.environ.setdefault(key.strip(), parts[0] if parts else "")


def required(name):
    value = os.environ.get(name, "")
    if not value or value.startswith("YOUR_"):
        raise ValueError("Set " + name)
    return value


def identifier(value):
    if not re.fullmatch(r"[a-z_][a-z0-9_]*", value):
        raise ValueError("Invalid SQL identifier")
    return value


@dataclass(frozen=True)
class Postgres:
    host: str
    port: int
    database: str
    user: str = field(repr=False)
    password: str = field(repr=False)
    sslmode: str = "prefer"

    @classmethod
    def from_env(cls, prefix):
        return cls(required(prefix + "HOST"), int(os.getenv(prefix + "PORT", "5432")),
                   os.getenv(prefix + "DATABASE", "de"), required(prefix + "USER"),
                   required(prefix + "PASSWORD"), os.getenv(prefix + "SSLMODE", "prefer"))

    def connect_options(self):
        return dict(host=self.host, port=self.port, dbname=self.database,
                    user=self.user, password=self.password, sslmode=self.sslmode,
                    connect_timeout=10)

    def jdbc_options(self, table):
        return {"url": "jdbc:postgresql://%s:%s/%s?sslmode=%s" %
                (self.host, self.port, self.database, self.sslmode),
                "driver": "org.postgresql.Driver", "dbtable": table,
                "user": self.user, "password": self.password}


@dataclass(frozen=True)
class Kafka:
    brokers: str
    input_topic: str
    output_topic: str
    protocol: str = "SASL_SSL"
    mechanism: str = "SCRAM-SHA-512"
    username: str = field(default="", repr=False)
    password: str = field(default="", repr=False)
    truststore: str = ""
    truststore_password: str = field(default="", repr=False)
    group_prefix: str = ""

    @classmethod
    def from_env(cls):
        protocol = os.getenv("KAFKA_SECURITY_PROTOCOL", "SASL_SSL")
        username = required("KAFKA_USERNAME") if protocol.startswith("SASL") else ""
        password = required("KAFKA_PASSWORD") if protocol.startswith("SASL") else ""
        return cls(required("KAFKA_BOOTSTRAP_SERVERS"), required("KAFKA_INPUT_TOPIC"),
                   required("KAFKA_OUTPUT_TOPIC"), protocol,
                   os.getenv("KAFKA_SASL_MECHANISM", "SCRAM-SHA-512"), username, password,
                   os.getenv("KAFKA_TRUSTSTORE_LOCATION", ""),
                   os.getenv("KAFKA_TRUSTSTORE_PASSWORD", ""),
                   os.getenv("KAFKA_GROUP_ID_PREFIX", ""))

    def spark_options(self):
        options = {"kafka.bootstrap.servers": self.brokers,
                   "kafka.security.protocol": self.protocol}
        if self.group_prefix:
            options["kafka.group.id"] = self.group_prefix + uuid.uuid4().hex
        if self.protocol.startswith("SASL"):
            def escape(value):
                return value.replace("\\", "\\\\").replace('"', '\\"')
            module = ("org.apache.kafka.common.security.scram.ScramLoginModule"
                      if self.mechanism.startswith("SCRAM")
                      else "org.apache.kafka.common.security.plain.PlainLoginModule")
            options.update({"kafka.sasl.mechanism": self.mechanism,
                            "kafka.sasl.jaas.config": '%s required username="%s" password="%s";' %
                            (module, escape(self.username), escape(self.password))})
        if self.truststore:
            options["kafka.ssl.truststore.location"] = self.truststore
            options["kafka.ssl.truststore.password"] = self.truststore_password
        return options


@dataclass(frozen=True)
class Settings:
    kafka: Kafka
    source: Postgres
    feedback: Postgres
    source_table: str = "public.subscribers_restaurants"
    feedback_table: str = "public.subscribers_feedback"
    checkpoint: str = "checkpoints/restaurant-notifications"
    trigger: str = "30 seconds"
    master: str = "local[2]"
    packages: str = ""
    starting_offsets: str = "earliest"

    @classmethod
    def from_env(cls):
        def table(name, default):
            value = os.getenv(name, default)
            parts = value.split(".")
            if len(parts) != 2:
                raise ValueError(name + " must be schema.table")
            return ".".join(identifier(p) for p in parts)
        return cls(Kafka.from_env(), Postgres.from_env("SOURCE_PG_"),
                   Postgres.from_env("FEEDBACK_PG_"),
                   table("SOURCE_TABLE", "public.subscribers_restaurants"),
                   table("FEEDBACK_TABLE", "public.subscribers_feedback"),
                   os.getenv("CHECKPOINT_LOCATION", "checkpoints/restaurant-notifications"),
                   os.getenv("TRIGGER_INTERVAL", "30 seconds"),
                   os.getenv("SPARK_MASTER", "local[2]"), os.getenv("SPARK_PACKAGES", ""),
                   os.getenv("KAFKA_STARTING_OFFSETS", "earliest"))
