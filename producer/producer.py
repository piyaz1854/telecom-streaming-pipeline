import json, os, random, time, uuid
from datetime import datetime, timezone
from kafka import KafkaProducer

BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "kafka:9092")
TOPIC = os.getenv("KAFKA_TOPIC", "telecom_events")
EPS = float(os.getenv("PRODUCER_EVENTS_PER_SEC", "5"))

REGIONS = ["Almaty","Astana","Shymkent","Aktobe","Karaganda","Atyrau","Pavlodar","Kostanay","Taraz","Oskemen"]
CALL_SUBTYPES = ["incoming","outgoing"]
EVENT_TYPES = ["data_session","call","sms","balance_recharge","service_activation"]
EVENT_WEIGHTS = [0.55, 0.25, 0.13, 0.06, 0.01]

def mask_msisdn(n:int)->str:
    s=f"770{n:07d}"
    return s[:-4]+"****"

def now_iso()->str:
    return datetime.now(timezone.utc).isoformat()

def gen_event():
    et = random.choices(EVENT_TYPES, weights=EVENT_WEIGHTS, k=1)[0]
    evt = {
        "event_id": str(uuid.uuid4()),
        "msisdn": mask_msisdn(random.randint(1_000_000,9_999_999)),
        "event_type": et,
        "event_subtype": None,
        "duration_seconds": None,
        "data_mb": None,
        "amount": None,
        "region": random.choice(REGIONS),
        "cell_tower_id": random.randint(1000,9999),
        "timestamp": now_iso(),
    }
    if et=="call":
        evt["event_subtype"]=random.choice(CALL_SUBTYPES)
        evt["duration_seconds"]=max(5, int(random.expovariate(1/60)))
        evt["duration_seconds"]=min(evt["duration_seconds"], 600)
    elif et=="sms":
        evt["event_subtype"]=random.choice(CALL_SUBTYPES)
    elif et=="data_session":
        sec=max(30, int(random.expovariate(1/900)))
        evt["duration_seconds"]=min(sec, 3*3600)
        mb_per_min=random.uniform(1.0,8.0)
        evt["data_mb"]=round((evt["duration_seconds"]/60)*mb_per_min, 2)
    elif et=="balance_recharge":
        evt["amount"]=random.choice([500.0,1000.0,2000.0,3000.0,5000.0,10000.0])
    return evt

def main():
    prod = KafkaProducer(
        bootstrap_servers=BOOTSTRAP,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        linger_ms=10, acks=0
    )
    print(f"[producer] -> {BOOTSTRAP} topic={TOPIC} rate={EPS} eps")
    interval = 1.0/max(EPS,0.1)
    while True:
        prod.send(TOPIC, gen_event())
        if random.random()<0.2:
            print(".", end="", flush=True)
        time.sleep(interval)

if __name__=="__main__":
    main()
