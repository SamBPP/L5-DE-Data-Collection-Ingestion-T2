from dataclasses import dataclass
from typing import Callable, Dict, Any
import random
from datetime import datetime, timezone
from faker import Faker

fake = Faker()

@dataclass
class Schema:
    name: str
    generator: Callable [[int], Dict[str, Any]]

def _now():
    return datetime.now(timezone.utc).isoformat()

def clicks_schema() -> Schema:
    def gen(i: int) -> Dict[str, Any]:
        return {
            "event_type": "click",
            "event_id": f"clk-{i:08d}",
            "ts": _now(),
            "user_id": fake.uuid4(),
            "page": random.choice(["/", "/search", "/product", "/cart", "/checkout"]),
            "device": random.choice(["desktop", "mobile", "tablet"])
        }
    return Schema(name="clicks", generator=gen)

def orders_schema() -> Schema:
    def gen(i: int) -> Dict[str, Any]:
        return {
            "event_type": "order",
            "event_id": f"ord-{i:08d}",
            "ts": _now(),
            "customer_id": fake.uuid4(),
            "amount": round(random.uniform(5, 500), 2)
        }
    return Schema(name="orders", generator=gen)

SCHEMAS = {"clicks" : clicks_schema(), "orders": orders_schema()}
