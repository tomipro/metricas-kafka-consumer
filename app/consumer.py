import os, sys, json, time, signal, random
from datetime import datetime, timezone
import requests
from confluent_kafka import Consumer, KafkaError, KafkaException

# ------- util env -------
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
MAX_QPS = float(os.getenv("POST_MAX_QPS", "12"))  # cap opcional
MIN_INTERVAL = 1.0 / MAX_QPS if MAX_QPS > 0 else 0.0
LOG_EVERY = int(os.getenv("LOG_EVERY", "100"))

conf = {
    "bootstrap.servers": BOOTSTRAP,
    "group.id": GROUP_ID,
    "enable.auto.commit": False,
    "auto.offset.reset": AUTO_OFFSET_RESET,     # latest o earliest
    "session.timeout.ms": 10000,
    "max.poll.interval.ms": 900000,             # 15 min para evitar MAXPOLL en ráfagas/retries
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

# ------- normalizadores -------
def _coerce_iso_z(v):
    """Devuelve YYYY-MM-DDTHH:MM:SSZ (UTC) a partir de string ISO, epoch ms/s, o None."""
    if v is None:
        return None
    # epoch ms / s
    if isinstance(v, (int, float)) or (isinstance(v, str) and v.strip().isdigit()):
        x = float(v)
        if x > 1e12:  # ms
            x = x / 1000.0
        dt = datetime.fromtimestamp(x, tz=timezone.utc)
        return dt.strftime("%Y-%m-%dT%H:%M:%SZ")
    s = str(v).strip()
    try:
        s2 = s.rstrip("Zz")
        dt = datetime.fromisoformat(s2)  # acepta con milisegundos
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        else:
            dt = dt.astimezone(timezone.utc)
        return dt.strftime("%Y-%m-%dT%H:%M:%SZ")
    except Exception:
        return s  # si no se pudo, se manda crudo y la Lambda decidirá

def _json_maybe(s):
    if isinstance(s, (dict, list)):
        return s
    if not isinstance(s, str):
        return s
    try:
        return json.loads(s)
    except Exception:
        return s

def _normalize_event(obj: dict) -> dict:
    """
    Acepta:
      - eventos "flat" con campos en raíz
      - eventos con {"payload": "<json-string>"} o {"payload": {...}}
    Produce:
      {
        "type": <string>,
        "userId"/"reservationId"/... (alias),
        "ts": ISO-UTC-Z sin fracciones,
        "payload": <objeto original razonable>
      }
    """
    if not isinstance(obj, dict):
        return {"payload": obj}

    # 1) doble-decoding si viene como string dentro de "payload"
    lvl1 = obj
    lvl2 = _json_maybe(lvl1.get("payload"))
    base = lvl2 if isinstance(lvl2, dict) else lvl1

    # 2) detectar tipo
    t = (
        base.get("type") or base.get("event_type") or base.get("eventType")
        or base.get("name")
    )
    # algunos en core: { id, name, occurred_at, schema_version, data: {...} }
    data = base.get("data") if isinstance(base.get("data"), dict) else base

    # 3) alias de IDs comunes
    ev = dict(base)  # copia para no mutar
    ev.update({
        "event_type": t or base.get("event_type"),
        "userId": data.get("userId") or base.get("userId") or data.get("user_id"),
        "reservationId": data.get("reservationId") or base.get("reservationId")
                           or data.get("resId") or data.get("reservation_id"),
        "paymentId": data.get("paymentId") or base.get("paymentId"),
    })

    # 4) timestamp: prioriza ts / occurredAt / createdAt / updatedAt / performedAt
    ts_candidate = (
        ev.get("ts") or base.get("ts") or base.get("occurredAt") or base.get("occurred_at")
        or base.get("createdAt") or data.get("createdAt") or base.get("updatedAt")
        or data.get("updatedAt") or base.get("performedAt") or data.get("performedAt")
        or base.get("timestamp") or base.get("time") or data.get("timestamp")
    )
    ev["ts"] = _coerce_iso_z(ts_candidate)

    # 5) mappings por tipo que tu Lambda exige
    t_effective = t or ev.get("event_type")

    if t_effective == "reservations.reservation.updated":
        # newStatus desde alias
        cand = (data.get("newStatus") or data.get("status") or data.get("reservationStatus")
                or data.get("new_status") or base.get("status") or base.get("newStatus"))
        if cand is not None:
            ev["newStatus"] = cand

    # 6) normalizar timestamps conocidos dentro del objeto por consistencia
    for k in ("createdAt", "updatedAt", "performedAt", "occurredAt"):
        if k in ev:
            ev[k] = _coerce_iso_z(ev[k])
        if k in data:
            data[k] = _coerce_iso_z(data[k])

    # 7) reconstruir payload razonable que conserve el original "útil"
    ev["payload"] = data if data is not base else base
    # type canónico
    if t_effective:
        ev["type"] = t_effective

    return ev

# ------- HTTP pacing -------
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

# ------- POST con manejo de 429/400 en body -------
def post_event(payload_bytes: bytes) -> bool:
    # decode bytes → obj
    try:
        raw = json.loads(payload_bytes.decode("utf-8", errors="ignore"))
    except Exception:
        raw = {"raw": payload_bytes.decode("utf-8", errors="ignore")}

    obj = _normalize_event(raw)

    for i in range(1, MAX_RETRIES + 1):
        try:
            _pace()
            r = session.post(API_URL, json=obj, timeout=POST_TIMEOUT_S)
            # 429: rate-limit → backoff fuerte, NO commit
            if r.status_code == 429:
                retry_after = int(r.headers.get("Retry-After", "0") or 0)
                sleep_s = retry_after if retry_after > 0 else min(30, 2 ** i)
                jlog("warn", msg="rate-limited 429", sleep_s=sleep_s, body=r.text[:200])
                time.sleep(sleep_s)
                continue

            # HTTP >=400: no commit
            if r.status_code >= 400:
                jlog("warn", msg="POST non-2xx", status=r.status_code, body=r.text[:200])
                time.sleep(min(10, 0.5 * i))
                continue

            # La API devuelve 200 pero la Lambda puede responder {statusCode:400} en el body
            try:
                body = r.json()
            except Exception:
                body = None

            if isinstance(body, dict) and body.get("statusCode", 200) >= 400:
                jlog("warn", msg="lambda rejected", statusCode=body.get("statusCode"), body=str(body)[:300])
                # backoff leve y reintentar; no commitear
                time.sleep(min(10, 0.5 * i))
                continue

            return True  # OK lógico
        except Exception as e:
            jlog("warn", msg="POST exception", error=str(e))
            time.sleep(min(10, 0.5 * i + random.uniform(0, 0.5)))

    return False

# ------- loop principal -------
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
