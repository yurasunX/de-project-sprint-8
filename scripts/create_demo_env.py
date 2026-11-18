#!/usr/bin/env python3
"""Generate a private configuration for the isolated Compose demonstration."""
from pathlib import Path
import secrets

root = Path(__file__).resolve().parents[1]
target = root / ".local/demo.env"
target.parent.mkdir(exist_ok=True)
if target.exists():
    raise SystemExit(".local/demo.env already exists; existing credentials preserved")
password = secrets.token_hex(16)
text = """KAFKA_BOOTSTRAP_SERVERS=localhost:19092
KAFKA_INPUT_TOPIC=restaurant_campaigns_in
KAFKA_OUTPUT_TOPIC=restaurant_notifications_out
KAFKA_SECURITY_PROTOCOL=PLAINTEXT
SOURCE_PG_HOST=localhost
SOURCE_PG_PORT=15432
SOURCE_PG_DATABASE=de
SOURCE_PG_USER=demo
SOURCE_PG_PASSWORD={password}
SOURCE_PG_SSLMODE=disable
FEEDBACK_PG_HOST=localhost
FEEDBACK_PG_PORT=15432
FEEDBACK_PG_DATABASE=de
FEEDBACK_PG_USER=demo
FEEDBACK_PG_PASSWORD={password}
FEEDBACK_PG_SSLMODE=disable
DEMO_POSTGRES_PASSWORD={password}
SPARK_MASTER='local[2]'
TRIGGER_INTERVAL='30 seconds'
CHECKPOINT_LOCATION=checkpoints/demo-notifications
""".format(password=password)
with target.open("x") as file:
    target.chmod(0o600)
    file.write(text)
print("Created private .local/demo.env")
