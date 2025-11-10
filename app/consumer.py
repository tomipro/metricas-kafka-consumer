# consumer.py
import os, sys, json, time, signal, random, collections
from datetime import datetime, timezone
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
AUTO_OFFSET_RESET = os.getenv("AUTO_OFFSET_RESET", "latest")
LOG_EVERY = int(os.getenv("LOG_EVERY", "100"))
MAX_QPS = float(os.getenv("POST_MAX_QPS", "12"))
MIN_INTERVAL = 1.0 / MAX_QPS if MAX_QPS > 0 else 0.0
EVENT_TYPES_WHITELIST = [s.strip() for s in os.getenv("EVENT_TYPES_WHITELIST", "").split(",") if s.strip()]

conf = {
    "bootstrap.servers": BOOTSTRAP,
    "group.id": GROUP_ID,
    "enable.auto.commit": False,
    "auto.offset.reset": AUTO_OFFSET_RESET,
    "session.timeout.ms": 10000,
    "max.poll.interval.ms": 900000,
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

def _headers_to_dict(hlist):
    out = {}
    if not hlist:
        return out
    for k, v in hlist:
        if not k:
            continue
        try:
            out[k] = v.decode("utf-8") if isinstance(v, (bytes, bytearray)) else v
        except Exception:
            out[k] = str(v) if v is not None else None
    return out

def _json_maybe(x):
    if isinstance(x, (dict, list)):
        return x
    if not isinstance(x, str):
        return x
    try:
        return json.loads(x)
    except Exception:
        return x

def _flatten_dict(d):
    """dict anidado -> pares (ruta_tuple, valor)"""
    stack = collections.deque([((), d)])
    while stack:
        path, cur = stack.popleft()
        if isinstance(cur, dict):
            for k, v in cur.items():
                stack.append((path + (k,), v))
        else:
            yield path, cur

def _find_first(d: dict, keys: list[str]):
    """Busca clave por alias en cualquier profundidad; devuelve el primero no vacío."""
    if not isinstance(d, dict):
        return None
    # 1) directa
    for k in keys:
        if k in d and d[k] not in (None, ""):
            return d[k]
    # 2) profunda
    want = set(keys)
    for path, val in _flatten_dict(d):
        if not path:
            continue
        last = path[-1]
        if last in want and val not in (None, ""):
            return val
    return None

def _coerce_iso_z(v):
    if v is None or v == "":
        return None
    # epoch?
    if isinstance(v, (int, float)) or (isinstance(v, str) and v.strip().isdigit()):
        x = float(v)
        if x > 1e12:  # ms
            x = x / 1000.0
        dt = datetime.fromtimestamp(x, tz=timezone.utc)
        return dt.strftime("%Y-%m-%dT%H:%M:%SZ")
    s = str(v).strip()
    # algunas fuentes traen Z o no; permiten fracciones
    try:
        s2 = s.rstrip("Zz")
        dt = datetime.fromisoformat(s2)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        else:
            dt = dt.astimezone(timezone.utc)
        return dt.strftime("%Y-%m-%dT%H:%M:%SZ")
    except Exception:
        return None  # mejor devolver None y aplicar fallback

def _derive_type(base: dict) -> str | None:
    return (
        base.get("type") or base.get("event_type") or base.get("eventType")
        or base.get("name") or (base.get("headers", {}) or {}).get("eventType")
    )

def _allowed_type(t: str) -> bool:
    if not EVENT_TYPES_WHITELIST:
        return True
    return any(t.startswith(prefix) for prefix in EVENT_TYPES_WHITELIST)

def _normalize_from_core_ingress(env: dict) -> dict:
    event_type = _derive_type(env)
    p1 = _json_maybe(env.get("payload"))
    if isinstance(p1, str):
        p1 = _json_maybe(p1)
    if not isinstance(p1, dict):
        p1 = {"raw": p1}

    ev = dict(p1)
    if event_type:
        ev["type"] = event_type

    # --- reservationId & newStatus (alias + búsqueda profunda) ---
    rid = _find_first(
        {"envelope": env, "payload": p1},
        ["reservationId", "reservation_id", "resId", "id"]
    )
    if isinstance(rid, dict):  # a veces viene reservation: { id: ... }
        rid = _find_first(rid, ["id", "reservationId"])
    if rid:
        ev["reservationId"] = rid

    nst = _find_first({"envelope": env, "payload": p1}, ["newStatus", "status", "reservationStatus", "state", "new_status"])
    if nst is not None:
        ev["newStatus"] = nst

    # --- timestamp robusto ---
    ts_candidates = [
        env.get("occurredAt"), env.get("timestamp"), env.get("ts"),
        _find_first(env.get("headers", {}) or {}, ["timestamp", "occurredAt"]),
        p1.get("ts"), p1.get("performedAt"), p1.get("updatedAt"),
        p1.get("createdAt"), p1.get("occurredAt"),
    ]
    ts_norm = None
    for c in ts_candidates:
        ts_norm = _coerce_iso_z(c)
        if ts_norm:
            break
    if not ts_norm:
        ts_norm = datetime.now(tz=timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
        ev.setdefault("meta", {})
        ev["meta"]["ts_source"] = "fallback_now"
    ev["ts"] = ts_norm

    # normalizar campos de tiempo comunes si existen
    for k in ("createdAt", "updatedAt", "performedAt", "occurredAt"):
        if k in ev:
            iso = _coerce_iso_z(ev[k])
            if iso:
                ev[k] = iso

    # meta útiles
    ev.setdefault("meta", {})
    ev["meta"].update({
        "coreMessageId": env.get("messageId"),
        "schemaVersion": env.get("schemaVersion"),
        "producer": env.get("producer"),
        "correlationId": env.get("correlationId"),
        "idempotencyKey": env.get("idempotencyKey"),
        "source": "core.ingress"
    })
    return ev

def _normalize_generic(env: dict) -> dict:
    p1 = _json_maybe(env.get("payload"))
    if isinstance(p1, str):
        p1 = _json_maybe(p1)

    base = p1 if isinstance(p1, dict) else env
    ev = dict(base)
    t = _derive_type(env) or _derive_type(base)
    if t:
        ev["type"] = t

    # ids comunes (profundos)
    rid = _find_first({"env": env, "base": base}, ["reservationId", "reservation_id", "resId", "id"])
    if isinstance(rid, dict):
        rid = _find_first(rid, ["id", "reservationId"])
    if rid:
        ev["reservationId"] = rid

    uid = _find_first({"env": env, "base": base}, ["userId", "user_id"])
    if uid:
        ev["userId"] = uid

    ts_candidates = [
        env.get("ts"), base.get("ts"),
        env.get("occurredAt"), env.get("createdAt"), env.get("updatedAt"), env.get("performedAt"),
        base.get("occurredAt"), base.get("createdAt"), base.get("updatedAt"), base.get("performedAt"),
        env.get("timestamp"), base.get("timestamp"),
    ]
    ts_norm = None
    for c in ts_candidates:
        ts_norm = _coerce_iso_z(c)
        if ts_norm:
            break
    if not ts_norm:
        ts_norm = datetime.now(tz=timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
        ev.setdefault("meta", {})
        ev["meta"]["ts_source"] = "fallback_now"
    ev["ts"] = ts_norm

    if t == "reservations.reservation.updated":
        nst = _find_first({"env": env, "base": base}, ["newStatus", "status", "reservationStatus", "state", "new_status"])
        if nst is not None:
            ev["newStatus"] = nst

    if isinstance(p1, dict):
        ev["payload"] = p1
    return ev

def _normalize_event(envelope: dict, topic: str) -> dict:
    if topic == "core.ingress" or ("eventType" in envelope and "payload" in envelope):
        return _normalize_from_core_ingress(envelope)
    return _normalize_generic(envelope)

_last_post = 0.0
def _pace():
    global _last_post
    if MIN_INTERVAL <= 0:
        return
    now = time.time()
    wait = _last_post + MIN_INTERVAL - now
    if wait > 0:
        time.sleep(wait)
    _last_post = time.time()

def post_event(msg) -> bool:
    try:
        env = json.loads(msg.value().decode("utf-8", errors="ignore"))
    except Exception:
        env = {"raw": msg.value().decode("utf-8", errors="ignore")}

    headers = _headers_to_dict(msg.headers())
    if headers:
        env.setdefault("headers", headers)
        if "eventType" in headers and "eventType" not in env:
            env["eventType"] = headers["eventType"]
        if "name" in headers and "name" not in env:
            env["name"] = headers["name"]
        if "timestamp" in headers and "ts" not in env:
            env["ts"] = headers["timestamp"]

    obj = _normalize_event(env, topic=msg.topic())

    if not obj.get("type"):
        obj["type"] = f"{msg.topic()}.unknown"
        jlog("warn", msg="missing type after normalization", topic=msg.topic(), sample=str(env)[:200])

    if not _allowed_type(obj.get("type", "")):
        jlog("info", msg="skipped by whitelist", type=obj.get("type"))
        return True

    obj.setdefault("meta", {})
    obj["meta"].update({
        "topic": msg.topic(),
        "partition": msg.partition(),
        "offset": msg.offset(),
        "key": (msg.key().decode("utf-8", "ignore") if msg.key() else None),
    })

    for i in range(1, MAX_RETRIES + 1):
        try:
            _pace()
            r = session.post(API_URL, json=obj, timeout=POST_TIMEOUT_S)

            if r.status_code == 429:
                retry_after = int(r.headers.get("Retry-After", "0") or 0)
                sleep_s = retry_after if retry_after > 0 else min(30, 2 ** i)
                jlog("warn", msg="rate-limited 429", sleep_s=sleep_s, body=r.text[:200])
                time.sleep(sleep_s)
                continue

            if r.status_code >= 400:
                jlog("warn", msg="POST non-2xx", status=r.status_code, body=r.text[:220], type=obj.get("type"), keys=list(obj.keys()))
                time.sleep(min(10, 0.5 * i))
                continue

            try:
                body = r.json()
            except Exception:
                body = None

            if isinstance(body, dict) and body.get("statusCode", 200) >= 400:
                jlog("warn", msg="lambda rejected", statusCode=body.get("statusCode"), body=str(body)[:300], type=obj.get("type"), keys=list(obj.keys()))
                time.sleep(min(10, 0.5 * i))
                continue

            return True

        except Exception as e:
            jlog("warn", msg="POST exception", error=str(e))
            time.sleep(min(10, 0.5 * i + random.uniform(0, 0.5)))
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

                ok = post_event(m)
                if not ok:
                    failed += 1
                    all_ok = False
                    jlog("warn", msg="event dropped after retries",
                         topic=m.topic(), partition=m.partition(), offset=m.offset())
                else:
                    processed += 1
                    if processed % LOG_EVERY == 0:
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
