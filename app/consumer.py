import os, sys, json, time, signal, random
import requests
from confluent_kafka import Consumer, KafkaError, KafkaException

def getenv_required(name: str) -> str:
    v = os.getenv(name)
    if not v:
        raise RuntimeError(f"Missing required env var: {name}")
    return v

BOOTSTRAP = getenv_required("KAFKA_BOOTSTRAP")
TOPICS = [t.strip() for t in getenv_required("KAFKA_TOPICS").split(",") if t.strip()]
GROUP_ID = os.getenv("KAFKA_GROUP_ID", "metricas-squad-fargate")
API_URL = getenv_required("API_URL")
API_KEY = getenv_required("API_KEY")

BATCH_SIZE = int(os.getenv("BATCH_SIZE", "50"))
POLL_TIMEOUT_S = float(os.getenv("POLL_TIMEOUT_S", "1.0"))
POST_TIMEOUT_S = float(os.getenv("POST_TIMEOUT_S", "10"))
MAX_RETRIES = int(os.getenv("MAX_RETRIES", "5"))

conf = {
    "bootstrap.servers": BOOTSTRAP,
    "group.id": GROUP_ID,
    "enable.auto.commit": False,   # commit sólo tras POST ok
    "auto.offset.reset": "latest",
    "session.timeout.ms": 10000,
    "max.poll.interval.ms": 300000,
    "fetch.min.bytes": 1,
    "fetch.wait.max.ms": 500,
    "socket.timeout.ms": 20000,
}

session = requests.Session()
session.headers.update({"x-api-key": API_KEY, "Content-Type": "application/json"})

_running = True
def _sigterm_handler(*_):
    global _running
    _running = False
signal.signal(signal.SIGTERM, _sigterm_handler)
signal.signal(signal.SIGINT, _sigterm_handler)

def jlog(level: str, **fields):
    fields["level"] = level
    print(json.dumps(fields, ensure_ascii=False), flush=True)

def post_event(payload_bytes: bytes) -> bool:
    try:
        payload = json.loads(payload_bytes.decode("utf-8", errors="ignore"))
    except Exception:
        payload = {"raw": payload_bytes.decode("utf-8", errors="ignore")}
    for i in range(1, MAX_RETRIES + 1):
        try:
            r = session.post(API_URL, json=payload, timeout=POST_TIMEOUT_S)
            if r.status_code < 400:
                return True
            jlog("warn", msg="POST non-2xx", status=r.status_code)
        except Exception as e:
            jlog("warn", msg="POST exception", error=str(e))
        time.sleep((0.5 * i) + random.uniform(0, 0.5))
    return False

def main():
    jlog("info", event="boot", bootstrap=BOOTSTRAP, topics=TOPICS, api_url=API_URL, group_id=GROUP_ID)
    c = Consumer(conf)
    c.subscribe(TOPICS)
    processed = failed = 0
    try:
        while _running:
            msgs = c.consume(num_messages=BATCH_SIZE, timeout=POLL_TIMEOUT_S)
            if not msgs: 
                continue
            all_ok = True
            for m in msgs:
                if m is None:
                    continue
                if m.error():
                    if m.error().code() != KafkaError._PARTITION_EOF:
                        jlog("warn", msg="kafka error", error=str(m.error()))
                    continue
                ok = post_event(m.value())
                if not ok:
                    failed += 1
                    all_ok = False
                    jlog("warn", msg="event dropped after retries",
                         topic=m.topic(), partition=m.partition(), offset=m.offset())
                else:
                    processed += 1
                    if processed % 100 == 0:
                        jlog("info", event="progress", processed=processed, failed=failed)
            if all_ok:
                try:
                    c.commit(asynchronous=False)
                except Exception as e:
                    jlog("error", msg="commit failed", error=str(e))
    except KafkaException as e:
        jlog("error", msg="kafka exception", error=str(e))
    finally:
        try: c.close()
        except: pass
        jlog("info", event="shutdown", processed=processed, failed=failed)

if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        jlog("error", msg="fatal boot error", error=str(e))
        sys.exit(1)
