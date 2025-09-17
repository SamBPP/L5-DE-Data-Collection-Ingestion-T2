import json
import time
import argparse
from kafka import KafkaProducer
from schemas import SCHEMAS

producer = KafkaProducer(bootstrap_servers="localhost:9092",
                  value_serializer=lambda v: json.dumps(v).encode())

def main():
    p = argparse.ArgumentParser()
    p.add_argument("--schema", choices=SCHEMAS.keys(), default="clicks", help="Which event schema to use")
    p.add_argument("--max", type=int, default=10, help="How many events to emit (0 = infinite)")
    p.add_argument("--rate", type=float, default=5.0, help="Events per second")
    args = p.parse_args()

    sleep_s = (1.0 / args.rate if args.rate > 0 else 0)

    try:
        for i in range(1, args.max + 1) if args.max > 0 else iter(int, 1):
            event = SCHEMAS[args.schema].generator(i)
            producer.send(args.schema, event)
            producer.flush()
            if sleep_s > 0:
                time.sleep(sleep_s)
    except KeyboardInterrupt:
        print("Closing producer...")

if __name__ == "__main__":
    main()
