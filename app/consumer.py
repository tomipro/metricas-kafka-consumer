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
    "enable.auto.commit": False,   # commit sólo tras POST ok (at-least-once)
    "auto.offset.reset": "latest",
    "session.timeout.ms": 10000,
    "max.poll.interval.ms": 300000,
    "fetch.min.bytes": 1,
    "fetch.wait.max.ms": 500,
    "socket.timeout.ms": 20000,
}

session = requests.Session()
# API Gateway header exacto como lo usás
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

# ---- Normalización para Lambda de ingesta ----
def normalize_for_ingest(msg: dict) -> dict:
    """
    Normaliza mensajes del core al contrato de tu API:
    - Si ya trae "type" en root -> passthrough.
    - Si trae "event_type" y "payload" dict -> flatten payload al root (sin pisar claves existentes),
      setea type=event_type y deriva ts de createdAt/ts/timestamp.
    - Conserva el payload original en msg["payload"] para trazabilidad.
    """
    if not isinstance(msg, dict):
        return {"raw": msg}

    # Ya viene listo
    if "type" in msg and isinstance(msg.get("type"), str):
        # Garantizar ts si existiera en variantes
        if "ts" not in msg:
            ts = (msg.get("createdAt") or msg.get("timestamp"))
            if ts:
                msg["ts"] = ts
        return msg

    out = dict(msg)  # copiamos todo lo que ya vino
    et = out.pop("event_type", None) or out.get("type")
    if et and "type" not in out:
        out["type"] = et

    payload = out.get("payload")
    if isinstance(payload, dict):
        # Flatten: subir todas las claves del payload al root si NO existen ya
        for k, v in payload.items():
            if k not in out:
                out[k] = v

    # Derivar ts desde candidatos comunes
    if "ts" not in out:
        ts = (
            out.get("createdAt")
            or (payload.get("createdAt") if isinstance(payload, dict) else None)
            or out.get("timestamp")
            or (payload.get("timestamp") if isinstance(payload, dict) else None)
        )
        if ts:
            out["ts"] = ts

    return out

def post_event(payload_bytes: bytes) -> bool:
    # Intenta decodificar; si no, manda raw
    try:
        raw = json.loads(payload_bytes.decode("utf-8", errors="ignore"))
    except Exception:
        raw = {"raw": payload_bytes.decode("utf-8", errors="ignore")}

    data = normalize_for_ingest(raw)

    # Reintentos con backoff + jitter
    for i in range(1, MAX_RETRIES + 1):
        try:
            r = session.post(API_URL, json=data, timeout=POST_TIMEOUT_S)
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

            # Commit sólo si TODO el batch salió ok (at-least-once)
            if all_ok:
                try:
                    c.commit(asynchronous=False)
                except Exception as e:
                    jlog("error", msg="commit failed", error=str(e))
    except KafkaException as e:
        jlog("error", msg="kafka exception", error=str(e))
    finally:
        try:
            c.close()
        except Exception:
            pass
        jlog("info", event="shutdown", processed=processed, failed=failed)

if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        jlog("error", msg="fatal boot error", error=str(e))
        sys.exit(1)
