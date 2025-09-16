import json
import argparse
from kafka import KafkaConsumer
from schemas import SCHEMAS

def main():
    p = argparse.ArgumentParser()
    p.add_argument("--schema", choices=SCHEMAS.keys(), default="clicks", help="Which event schema to consume from")
    args = p.parse_args()

    c = KafkaConsumer(
        args.schema,
        bootstrap_servers="localhost:9092",
        value_deserializer=lambda b: json.loads(b.decode()),
        auto_offset_reset="earliest",
        enable_auto_commit=True,
        group_id="analytics"
    )

    try:
        for msg in c:
            print(msg.value)
    except KeyboardInterrupt:
        print("Stopping consumer...")

if __name__ == "__main__":
    main()