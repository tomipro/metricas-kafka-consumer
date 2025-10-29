import os, sys, json, time, signal, random
import requests
from confluent_kafka import Consumer, KafkaError, KafkaException
from typing import Any, Dict

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

def _maybe_parse_json_str(s: str) -> Any:
    s = s.strip()
    if not s:
        return s
    if s[0] not in "{[":
        return s
    try:
        return json.loads(s)
    except Exception:
        return s

def _destringify(obj: Any, depth: int = 0) -> Any:
    """Recorre dict/list y convierte strings con JSON embebido a objetos reales.
       Limita profundidad por seguridad."""
    if depth > 10:
        return obj
    if isinstance(obj, dict):
        out: Dict[str, Any] = {}
        for k, v in obj.items():
            if isinstance(v, str):
                v2 = _maybe_parse_json_str(v)
                out[k] = _destringify(v2, depth + 1)
            else:
                out[k] = _destringify(v, depth + 1)
        return out
    elif isinstance(obj, list):
        return [_destringify(v, depth + 1) for v in obj]
    elif isinstance(obj, str):
        return _maybe_parse_json_str(obj)
    else:
        return obj

def _normalize_keys(ev: Dict[str, Any]) -> Dict[str, Any]:
    """No cambia el esquema, solo agrega alias útiles si faltan."""
    norm = dict(ev)

    # event_type
    if "event_type" not in norm:
        if "eventType" in norm:
            norm["event_type"] = norm["eventType"]
        elif "name" in norm:
            norm["event_type"] = norm["name"]

    # schema_version
    if "schema_version" not in norm:
        if "schemaVersion" in norm:
            norm["schema_version"] = norm["schemaVersion"]

    # occurred_at
    if "occurred_at" not in norm:
        if "occurredAt" in norm:
            norm["occurred_at"] = norm["occurredAt"]
        elif "timestamp" in norm:
            norm["occurred_at"] = norm["timestamp"]

    # source
    if "source" not in norm:
        if "producer" in norm:
            norm["source"] = norm["producer"]

    # Si existen 'payload' o 'data' como string JSON ya fueron destringificados por _destringify.
    return norm

def post_event(payload_bytes: bytes) -> bool:
    # 1) decodificar el value del record
    try:
        raw = payload_bytes.decode("utf-8", errors="ignore")
        payload = json.loads(raw)
    except Exception:
        # No es JSON => enviar como raw encapsulado
        payload = {"raw": payload_bytes.decode("utf-8", errors="ignore")}

    # 2) des-stringificar cualquier JSON embebido (payload, data, etc.)
    payload = _destringify(payload)

    # 3) normalizar alias de campos comunes (opcional, no rompe nada)
    if isinstance(payload, dict):
        payload = _normalize_keys(payload)

    # 4) POST con reintentos exponenciales
    for i in range(1, MAX_RETRIES + 1):
        try:
            r = session.post(API_URL, json=payload, timeout=POST_TIMEOUT_S)
            if r.status_code < 400:
                return True
            jlog("warn", msg="POST non-2xx", status=r.status_code, body=r.text[:300])
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
        try:
            c.close()
        except:
            pass
        jlog("info", event="shutdown", processed=processed, failed=failed)

if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        jlog("error", msg="fatal boot error", error=str(e))
        sys.exit(1)
