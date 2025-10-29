import os, sys, json, time, signal, random
import requests
from typing import Any, Dict, List
from confluent_kafka import Consumer, KafkaError, KafkaException

# ---------- Env & config ----------
def getenv_required(name: str) -> str:
    v = os.getenv(name)
    if not v:
        raise RuntimeError(f"Missing required env var: {name}")
    return v

BOOTSTRAP = getenv_required("KAFKA_BOOTSTRAP")
TOPICS: List[str] = [t.strip() for t in getenv_required("KAFKA_TOPICS").split(",") if t.strip()]
GROUP_ID = os.getenv("KAFKA_GROUP_ID", "metricas-squad-fargate")
API_URL = getenv_required("API_URL")
API_KEY = getenv_required("API_KEY")

BATCH_SIZE = int(os.getenv("BATCH_SIZE", "50"))
POLL_TIMEOUT_S = float(os.getenv("POLL_TIMEOUT_S", "1.0"))
POST_TIMEOUT_S = float(os.getenv("POST_TIMEOUT_S", "10"))
MAX_RETRIES = int(os.getenv("MAX_RETRIES", "5"))

# Toggles de diagnóstico / offsets
DEBUG_MSGS = os.getenv("DEBUG_MSGS", "false").lower() == "true"
LOG_EVERY = int(os.getenv("LOG_EVERY", "100"))
AUTO_OFFSET_RESET = os.getenv("AUTO_OFFSET_RESET", "latest")  # "latest" (default) o "earliest"

conf = {
    "bootstrap.servers": BOOTSTRAP,
    "group.id": GROUP_ID,
    "enable.auto.commit": False,   # commit sólo tras POST ok (at-least-once)
    "auto.offset.reset": AUTO_OFFSET_RESET,
    "session.timeout.ms": 10000,
    "max.poll.interval.ms": 300000,
    "fetch.min.bytes": 1,
    "fetch.wait.max.ms": 500,
    "socket.timeout.ms": 20000,
}

session = requests.Session()
session.headers.update({"x-api-key": API_KEY, "Content-Type": "application/json"})

# ---------- Señales & logging ----------
_running = True
def _sigterm_handler(*_):
    global _running
    _running = False
signal.signal(signal.SIGTERM, _sigterm_handler)
signal.signal(signal.SIGINT, _sigterm_handler)

def jlog(level: str, **fields):
    fields["level"] = level
    print(json.dumps(fields, ensure_ascii=False), flush=True)

# ---------- Helpers de normalización ----------
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
    """Convierte strings que contienen JSON embebido a objetos reales (dict/list) de forma recursiva."""
    if depth > 12:
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
    if isinstance(obj, list):
        return [_destringify(v, depth + 1) for v in obj]
    if isinstance(obj, str):
        return _maybe_parse_json_str(obj)
    return obj

def _alias(v: Dict[str, Any], dst: str, *candidates: str):
    if dst in v:
        return
    for c in candidates:
        if c in v:
            v[dst] = v[c]
            return

def _normalize_generic(msg: Dict[str, Any]) -> Dict[str, Any]:
    """
    Normaliza eventos a un formato que tu Lambda entiende:
    - Si ya trae "type" en raíz: lo deja y deriva "ts" si falta.
    - Si trae patrón core (event_type + payload dict): type=event_type y flatten de payload.
    - Si trae patrón wrapper (name + data dict + occurred_at): type=name y flatten de data.
    - Si hay 'payload'/'data' dict sin patrón claro: flatten no destructivo.
    - Deriva 'ts' desde createdAt/updatedAt/timestamp/occurred_at.
    """
    out = dict(msg)

    # Caso: ya viene listo
    if "type" in out and isinstance(out.get("type"), str):
        if "ts" not in out:
            ts = out.get("createdAt") or out.get("updatedAt") or out.get("timestamp") or out.get("occurred_at") or out.get("occurredAt")
            if ts:
                out["ts"] = ts
        return out

    # Aliases útiles
    _alias(out, "event_type", "eventType", "name")  # name -> event_type por compat
    _alias(out, "occurred_at", "occurredAt", "timestamp")
    _alias(out, "schema_version", "schemaVersion")

    payload = out.get("payload")
    data = out.get("data")

    # Patrón A: event_type + payload dict
    if isinstance(out.get("event_type"), str) and isinstance(payload, dict):
        norm = {"type": out["event_type"]}
        # flatten de payload (non-destructive sobre norm)
        for k, v in payload.items():
            norm.setdefault(k, v)
        # ts derivado
        ts = norm.get("createdAt") or norm.get("updatedAt") or out.get("occurred_at") or out.get("timestamp")
        if ts:
            norm["ts"] = ts
        # mantener payload original por trazabilidad
        norm["payload"] = payload
        # conservar metadatos útiles si estaban
        for k in ("schema_version", "source", "producer", "correlation_id", "id"):
            if k in out:
                norm[k] = out[k]
        return norm

    # Patrón B: wrapper name/data/occurred_at
    if isinstance(out.get("name"), str) and isinstance(data, dict):
        norm = {"type": out["name"]}
        for k, v in data.items():
            norm.setdefault(k, v)
        ts = norm.get("updatedAt") or out.get("occurred_at") or out.get("timestamp")
        if ts:
            norm["ts"] = ts
        norm["data"] = data
        for k in ("schema_version", "source", "producer", "correlation_id", "id"):
            if k in out:
                norm[k] = out[k]
        return norm

    # Patrón C: flatten best-effort si hay payload/data dict aunque no haya event_type/name
    if isinstance(payload, dict):
        for k, v in payload.items():
            out.setdefault(k, v)
    if isinstance(data, dict):
        for k, v in data.items():
            out.setdefault(k, v)

    # Intentar inferir type si no existe
    _alias(out, "type", "event_type", "name")

    # Derivar ts
    if "ts" not in out:
        ts = out.get("createdAt") or out.get("updatedAt") or out.get("timestamp") or out.get("occurred_at") or out.get("occurredAt")
        if ts:
            out["ts"] = ts

    return out

# ---------- Envío a API ----------
def post_event(payload_bytes: bytes) -> bool:
    # 1) decodificar a objeto Python
    try:
        raw_str = payload_bytes.decode("utf-8", errors="ignore")
        obj = json.loads(raw_str)
    except Exception:
        obj = {"raw": payload_bytes.decode("utf-8", errors="ignore")}

    # 2) des-stringify recursivo (payload/data pueden venir como string JSON)
    obj = _destringify(obj)

    # 3) normalización genérica al contrato de tu Lambda
    if isinstance(obj, dict):
        obj = _normalize_generic(obj)

    # 4) POST con reintentos y backoff+jitter
    for i in range(1, MAX_RETRIES + 1):
        try:
            r = session.post(API_URL, json=obj, timeout=POST_TIMEOUT_S)
            if r.status_code < 400:
                return True
            jlog("warn", msg="POST non-2xx", status=r.status_code, body=r.text[:300])
        except Exception as e:
            jlog("warn", msg="POST exception", error=str(e))
        time.sleep((0.5 * i) + random.uniform(0, 0.5))
    return False

# ---------- Loop principal ----------
def main():
    jlog("info", event="boot", bootstrap=BOOTSTRAP, topics=TOPICS, api_url=API_URL, group_id=GROUP_ID, auto_offset_reset=AUTO_OFFSET_RESET)
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
                    if DEBUG_MSGS:
                        jlog("info", event="delivered", topic=m.topic(), partition=m.partition(), offset=m.offset())
                    elif processed % LOG_EVERY == 0:
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
        except Exception:
            pass
        jlog("info", event="shutdown", processed=processed, failed=failed)

if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        jlog("error", msg="fatal boot error", error=str(e))
        sys.exit(1)
