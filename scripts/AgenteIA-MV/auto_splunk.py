#!/usr/bin/env python3
"""
auto_splunk.py
==============
Versió automàtica amb logs optimitzats.

- Per terminal: 1 línia per event processat.
- Per arxiu:    logs/auto_splunk_YYYY-MM-DD.log   (data de creació del fitxer)
                Si omple i es crea un altre el mateix dia:
                  auto_splunk_YYYY-MM-DD_2.log, _3.log ...
                Si el fitxer actiu d'un dia anterior encara té espai,
                es continua escrivint al mateix (el nom és la data d'obertura).
                Mida màxima per fitxer: LOG_MAX_BYTES (per defecte 10 MB).

Millores de rendiment vs versió anterior:
  - Comptador intern de bytes → sense os.path.getsize() per línia
  - Flush cada FLUSH_EVERY línies en lloc de per línia

Per arrencar:
    python3 auto_splunk.py

Per parar (forma neta):
    Ctrl+C
"""
import json
import os
import re
import time
import signal
import traceback
from datetime import datetime
import requests
import urllib3
import psycopg2
from psycopg2.extras import RealDictCursor

# Silenciem el warning d'urllib3 a cada request a Splunk (causat per VERIFY_TLS=False)
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# ============================================================
# CONFIG
# ============================================================
SPLUNK_BASE_URL = "https://es.eymsp.splunkcloud.com:8089"
SPLUNK_TOKEN = "eyJraWQiOiJzcGx1bmsuc2VjcmV0IiwiYWxnIjoiSFM1MTIiLCJ2ZXIiOiJ2MiIsInR0eXAiOiJzdGF0aWMifQ.eyJpc3MiOiJwYWJsby5nb21lei5jYWx2b0Blcy5leS5jb20gZnJvbSBzaC1pLTA4MGQ5MTE2OWQ5NTgxYmY2Iiwic3ViIjoicGFibG8uZ29tZXouY2Fsdm9AZXMuZXkuY29tIiwiYXVkIjoiQUdFTlRfRlBUUCIsImlkcCI6IlNwbHVuayIsImp0aSI6IjNlNTA4NjlmM2QxZTk0OGNlYzBiZjlhYTY5M2M2YzIzZmU0MjdkMzBhM2VjYzg1MTFmYjY5ZWYxYTI2ZTFhOGQiLCJpYXQiOjE3NzEzMjYwMjksImV4cCI6MTgzNDQ0MTE0MywibmJyIjoxNzcxMzI2MDI5fQ.I4nWFbEadyiRePGIU1sueIm2ybrevFOB9KUZyLQSd9QtyGexhuTcVKtD7Kh4yHC_ezk8Xn0rFYTIl9_eqduJEQ"
VERIFY_TLS = False

DB_CONFIG = {
    "host": "localhost",
    "port": 5432,
    "database": "alertas_soc",
    "user": "postgres",
    "password": "toor1"
}

# Tunables del bucle
MAX_ATTEMPTS = 3
RETRY_INTERVAL_SECONDS = 30
SLEEP_WHEN_BUSY = 1
SLEEP_WHEN_IDLE = 5
SLEEP_AFTER_ERROR = 30

# Logs
LOG_DIR = "logs"
LOG_PREFIX = "auto_splunk"
LOG_MAX_BYTES = 10 * 1024 * 1024   # 10 MB per fitxer
FLUSH_EVERY = 20                    # flush cada N línies (en lloc de per línia)

# ============================================================
# SISTEMA DE LOGS
# ============================================================
class FileLogger:
    """
    Escriu a logs/auto_splunk_YYYY-MM-DD.log on la data és la d'obertura
    del fitxer (no la d'avui necessàriament).

    En arrencar:
      - Busca el fitxer existent més recent que no hagi superat LOG_MAX_BYTES.
      - Si existeix, continua escrivint-hi (manté la data original al nom).
      - Si no n'hi ha cap, crea un nou amb la data d'avui.

    En rotar (fitxer ple):
      - Tanca el fitxer actual (que ja té la data correcta al nom).
      - Obre un nou fitxer amb la data d'avui.
      - Si ja existeix (rotació múltiple el mateix dia), afegeix _2, _3...

    Millores de rendiment:
      - Comptador intern de bytes → 0 syscalls getsize() durant l'execució.
      - Flush cada FLUSH_EVERY línies en lloc de per línia.
    """

    def __init__(self, log_dir: str, prefix: str, max_bytes: int, flush_every: int = 20):
        self.log_dir = log_dir
        self.prefix = prefix
        self.max_bytes = max_bytes
        self.flush_every = flush_every
        self._line_count = 0
        self._byte_count = 0
        self.fp = None
        self.current_path = None

        os.makedirs(log_dir, exist_ok=True)

        # Intenta reutilitzar el fitxer actiu més recent que no estigui ple
        existing = self._find_resumable()
        if existing:
            self.current_path = existing
            self._byte_count = os.path.getsize(existing)   # una sola vegada a l'inici
            self.fp = open(existing, "a", encoding="utf-8")
        else:
            self._open_new()

    def _all_log_files(self) -> list:
        """Retorna tots els fitxers de log d'aquest prefix, ordenats per data de modificació."""
        try:
            files = [
                os.path.join(self.log_dir, f)
                for f in os.listdir(self.log_dir)
                if f.startswith(self.prefix + "_") and f.endswith(".log")
            ]
            files.sort(key=lambda p: os.path.getmtime(p), reverse=True)
            return files
        except OSError:
            return []

    def _find_resumable(self) -> str | None:
        """Retorna el fitxer més recent que té espai, o None."""
        for path in self._all_log_files():
            try:
                if os.path.getsize(path) < self.max_bytes:
                    return path
            except OSError:
                continue
        return None

    def _new_path(self) -> str:
        """Calcula el nom del nou fitxer amb la data d'avui (afegint _2, _3 si cal)."""
        date_str = datetime.now().strftime("%Y-%m-%d")
        path = os.path.join(self.log_dir, f"{self.prefix}_{date_str}.log")
        n = 2
        while os.path.exists(path):
            path = os.path.join(self.log_dir, f"{self.prefix}_{date_str}_{n}.log")
            n += 1
        return path

    def _open_new(self):
        """Obre un fitxer nou amb la data d'avui."""
        self.current_path = self._new_path()
        self._byte_count = 0
        self.fp = open(self.current_path, "a", encoding="utf-8")

    def _rotate_if_needed(self):
        """Rota si el comptador intern ha superat el límit."""
        if self._byte_count >= self.max_bytes:
            if self.fp:
                try:
                    self.fp.flush()
                    self.fp.close()
                except Exception:
                    pass
            self._open_new()
            self._line_count = 0

    def _flush_if_needed(self):
        if self._line_count >= self.flush_every:
            try:
                self.fp.flush()
            except Exception:
                pass
            self._line_count = 0

    def _write_raw(self, text: str):
        """Escriu text cru al fitxer i actualitza el comptador intern."""
        encoded_len = len(text.encode("utf-8"))
        self.fp.write(text)
        self._byte_count += encoded_len
        self._line_count += text.count("\n")
        self._flush_if_needed()
        self._rotate_if_needed()

    def write(self, level: str, msg: str):
        ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        self._write_raw(f"{ts} | {level} | {msg}\n")

    def close(self):
        if self.fp:
            try:
                self.fp.flush()
                self.fp.close()
            except Exception:
                pass


LOGGER: FileLogger | None = None


def _setup_logger():
    global LOGGER
    LOGGER = FileLogger(LOG_DIR, LOG_PREFIX, LOG_MAX_BYTES, FLUSH_EVERY)


def log(msg: str):
    if LOGGER:
        LOGGER.write("INFO ", msg)


def log_warn(msg: str):
    if LOGGER:
        LOGGER.write("WARN ", msg)


def log_error(msg: str):
    if LOGGER:
        LOGGER.write("ERROR", msg)


def term(line: str):
    """Imprimeix UNA línia per terminal amb timestamp curt."""
    ts = datetime.now().strftime("%H:%M:%S")
    print(f"[{ts}] {line}", flush=True)


# ============================================================
# Maneig de Ctrl+C net
# ============================================================
_running = True


def _signal_handler(signum, frame):
    global _running
    log(f"Senyal {signum} rebuda, aturant després de la iteració actual")
    _running = False


signal.signal(signal.SIGINT, _signal_handler)
signal.signal(signal.SIGTERM, _signal_handler)


# ============================================================
# BD: alertas_soc
# ============================================================
def get_db_connection():
    return psycopg2.connect(**DB_CONFIG)


def get_next_event_id():
    conn = get_db_connection()
    try:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(
                """
                SELECT id, source_event_id, event_time, received_at,
                       attempt_count, processing_started_at
                FROM ids.eventos
                WHERE last_attempt_at IS NULL
                   OR last_attempt_at < NOW() - (%s * INTERVAL '1 second')
                ORDER BY received_at ASC
                LIMIT 1;
                """,
                (RETRY_INTERVAL_SECONDS,)
            )
            return cur.fetchone()
    finally:
        conn.close()


def mark_processing_started(row_id: int, existing):
    """
    Marca l'inici del processament d'una alerta (cronòmetre del pipeline).

    Es crida quan auto_splunk.py agafa l'alerta de ids.eventos per processar-la.
    NOMÉS s'escriu la primera vegada (quan processing_started_at IS NULL); en
    reintents posteriors es conserva el timestamp original, de manera que el temps
    de cua previ NO compta i els reintents sí formen part del temps de processament.

    Retorna el timestamp efectiu (datetime) que cal propagar pel pipeline.
    """
    if existing is not None:
        return existing

    conn = get_db_connection()
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                UPDATE ids.eventos
                SET processing_started_at = NOW()
                WHERE id = %s AND processing_started_at IS NULL
                RETURNING processing_started_at;
                """,
                (row_id,)
            )
            updated = cur.fetchone()
            if updated is None:
                # Cas excepcional (carrera): ja estava marcat, el rellegim.
                cur.execute(
                    "SELECT processing_started_at FROM ids.eventos WHERE id = %s;",
                    (row_id,)
                )
                fetched = cur.fetchone()
                started = fetched[0] if fetched else None
            else:
                started = updated[0]
        conn.commit()
        return started
    finally:
        conn.close()



    conn = get_db_connection()
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                UPDATE ids.eventos
                SET attempt_count = %s,
                    last_attempt_at = NOW()
                WHERE id = %s;
                """,
                ((current_attempts or 0) + 1, row_id)
            )
        conn.commit()
    finally:
        conn.close()


def move_to_fallidas(row: dict, error_msg: str):
    conn = get_db_connection()
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO ids.eventos_fallidos
                    (source_event_id, event_time, received_at, attempt_count, last_error)
                VALUES (%s, %s, %s, %s, %s);
                """,
                (
                    row["source_event_id"],
                    row["event_time"],
                    row["received_at"],
                    (row["attempt_count"] or 0) + 1,
                    (error_msg or "")[:1000]
                )
            )
            cur.execute("DELETE FROM ids.eventos WHERE id = %s;", (row["id"],))
        conn.commit()
    finally:
        conn.close()


def delete_event_id(row_id: int):
    conn = get_db_connection()
    try:
        with conn.cursor() as cur:
            cur.execute("DELETE FROM ids.eventos WHERE id = %s;", (row_id,))
        conn.commit()
    finally:
        conn.close()


# ============================================================
# BD: encoded
# ============================================================
def save_event(event_id: str, event_dict: dict):
    conn = get_db_connection()
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO encoded.eventos (
                    source_event_id, orig_client, orig_index, orig_rule_title,
                    rule_description, severity, src_ip, dest_ip, dest_port,
                    action, signature, country, "user", hostname, sender,
                    subject, recipient, url, processing_started_at
                )
                VALUES (
                    %(source_event_id)s, %(orig_client)s, %(orig_index)s, %(orig_rule_title)s,
                    %(rule_description)s, %(severity)s, %(src_ip)s, %(dest_ip)s, %(dest_port)s,
                    %(action)s, %(signature)s, %(country)s, %(user)s, %(hostname)s, %(sender)s,
                    %(subject)s, %(recipient)s, %(url)s, %(processing_started_at)s
                )
                ON CONFLICT (source_event_id) DO UPDATE
                SET orig_client = EXCLUDED.orig_client,
                    orig_index = EXCLUDED.orig_index,
                    orig_rule_title = EXCLUDED.orig_rule_title,
                    rule_description = EXCLUDED.rule_description,
                    severity = EXCLUDED.severity,
                    src_ip = EXCLUDED.src_ip,
                    dest_ip = EXCLUDED.dest_ip,
                    dest_port = EXCLUDED.dest_port,
                    action = EXCLUDED.action,
                    signature = EXCLUDED.signature,
                    country = EXCLUDED.country,
                    "user" = EXCLUDED."user",
                    hostname = EXCLUDED.hostname,
                    sender = EXCLUDED.sender,
                    subject = EXCLUDED.subject,
                    recipient = EXCLUDED.recipient,
                    url = EXCLUDED.url,
                    processing_started_at = COALESCE(encoded.eventos.processing_started_at, EXCLUDED.processing_started_at),
                    created_at = now();
                """,
                event_dict
            )
        conn.commit()
    finally:
        conn.close()


# ============================================================
# Splunk
# ============================================================
def build_query(event_id: str) -> str:
    return f"""search index=notable earliest=0 latest=now
| where source_event_id="{event_id}" OR search_event_id="{event_id}" OR notable_event_id="{event_id}"
| rex field=rule_title "^(?<orig_client>[^-]+)"
| head 1
""".strip()


def _looks_like_splunk_error(text: str) -> bool:
    return ('"type":"ERROR"' in text) or ('"type":"FATAL"' in text)


def splunk_export(event_id: str, query: str, max_attempts: int = 3,
                  connect_timeout: int = 20, read_timeout: int = 240):
    url = f"{SPLUNK_BASE_URL}/services/search/jobs/export"
    headers = {
        "Authorization": f"Splunk {SPLUNK_TOKEN}",
        "Content-Type": "application/x-www-form-urlencoded",
    }
    data = {
        "search": query,
        "output_mode": "json",
        "count": "0",
        "preview": "false",
        "earliest_time": "0",
        "latest_time": "now",
    }

    last_err = None

    for attempt in range(1, max_attempts + 1):
        if attempt > 1:
            log(f"{event_id} | HTTP retry {attempt}/{max_attempts}")

        t0 = time.time()
        try:
            r = requests.post(
                url, headers=headers, data=data,
                timeout=(connect_timeout, read_timeout),
                verify=VERIFY_TLS
            )
            elapsed = time.time() - t0

            log(f"{event_id} | Splunk HTTP {r.status_code} ({elapsed:.2f}s)")
            r.raise_for_status()

            if _looks_like_splunk_error(r.text or ""):
                raise RuntimeError("Splunk ERROR/FATAL en resposta")

            results = []
            for line in (r.text or "").splitlines():
                line = line.strip()
                if not line:
                    continue
                try:
                    obj = json.loads(line)
                except Exception:
                    continue
                if isinstance(obj, dict) and isinstance(obj.get("result"), dict):
                    results.append(obj["result"])

            log(f"{event_id} | Resultats Splunk: {len(results)}")
            return results

        except requests.exceptions.ReadTimeout as e:
            last_err = f"ReadTimeout: {e}"
            log_warn(f"{event_id} | {last_err}")
        except requests.exceptions.ConnectTimeout as e:
            last_err = f"ConnectTimeout: {e}"
            log_warn(f"{event_id} | {last_err}")
        except requests.exceptions.RequestException as e:
            last_err = f"RequestException: {e}"
            log_warn(f"{event_id} | {last_err}")
        except Exception as e:
            last_err = str(e)
            log_warn(f"{event_id} | {last_err}")

        time.sleep(2)

    raise RuntimeError(f"Splunk fallat després de {max_attempts} intents: {last_err}")


# ============================================================
# Helpers de parsejat
# ============================================================
def parse_raw_kv(raw_text: str) -> dict:
    result = {}
    pattern = re.compile(r'(\w+(?:\.\w+)*)=(".*?"|[^,]+)')
    for match in pattern.finditer(raw_text):
        key = match.group(1)
        value = match.group(2).strip()
        if value.startswith('"') and value.endswith('"'):
            value = value[1:-1].replace('\\"', '"')
        result[key] = value
    return result


def safe_get(event, *keys):
    for k in keys:
        if k in event and event[k] not in (None, "", "None"):
            return event[k]
    return None


# ============================================================
# Processat d'UNA ID
# ============================================================
def process_one(row: dict):
    event_id = row["source_event_id"]
    row_id = row["id"]
    attempts = row["attempt_count"] or 0

    # Cronòmetre del pipeline: marquem l'inici real de processament (1r intent).
    # Si ja venia marcat (reintent), es conserva el timestamp original.
    processing_started_at = mark_processing_started(row_id, row.get("processing_started_at"))

    # Una línia d'inici per event (en lloc del separador ====)
    log(f"{event_id} | INICI intent {attempts + 1}/{MAX_ATTEMPTS}")

    # 1) Crida a Splunk
    try:
        rows = splunk_export(event_id, build_query(event_id))
    except Exception as e:
        err_msg = str(e)
        if attempts + 1 >= MAX_ATTEMPTS:
            move_to_fallidas(row, err_msg)
            log_error(f"{event_id} | FAIL Splunk ({MAX_ATTEMPTS} intents): {err_msg}")
            term(f"FAIL {event_id} → fallidas (Splunk no respon)")
        else:
            mark_attempt_failed(row_id, attempts)
            log_warn(f"{event_id} | WARN Splunk intent {attempts + 1}/{MAX_ATTEMPTS}: {err_msg}")
            term(f"WARN {event_id} → reintent {attempts + 1}/{MAX_ATTEMPTS}")
        return

    # 2) Splunk ha respost però no ha trobat l'alerta
    if not rows:
        err_msg = "Alerta no trobada a Splunk (índex notable)"
        if attempts + 1 >= MAX_ATTEMPTS:
            move_to_fallidas(row, err_msg)
            log_error(f"{event_id} | FAIL no trobat ({MAX_ATTEMPTS} intents)")
            term(f"FAIL {event_id} → fallidas (no apareix a Splunk)")
        else:
            mark_attempt_failed(row_id, attempts)
            log_warn(f"{event_id} | WARN no trobat, reintent {attempts + 1}/{MAX_ATTEMPTS}")
            term(f"WARN {event_id} → reintent {attempts + 1}/{MAX_ATTEMPTS}")
        return

    # 3) Èxit: parsejar i guardar
    event = rows[0]
    if "_raw" in event and event["_raw"]:
        parsed_fields = parse_raw_kv(event["_raw"])
        event.update(parsed_fields)

    event_dict = {
        "source_event_id": event_id,
        "orig_client":     safe_get(event, "client_sig", "orig_client"),
        "orig_index":      safe_get(event, "orig_index", "index"),
        "orig_rule_title": safe_get(event, "orig_rule_title"),
        "rule_description":safe_get(event, "orig_rule_description", "description"),
        "severity":        safe_get(event, "severity", "severity_name"),
        "src_ip":          safe_get(event, "src_ip", "ip"),
        "dest_ip":         safe_get(event, "dest_ip"),
        "dest_port":       safe_get(event, "dest_port"),
        "action":          safe_get(event, "action"),
        "signature":       safe_get(event, "signature", "name"),
        "country":         safe_get(event, "country"),
        "user":            safe_get(event, "user", "user_name"),
        "hostname":        safe_get(event, "dest", "device.hostname"),
        "sender":          safe_get(event, "sender"),
        "subject":         safe_get(event, "subject"),
        "recipient":       safe_get(event, "recipient"),
        "url":             safe_get(event, "url", "falcon_host_link"),
        # Cronòmetre del pipeline: propaguem l'inici de processament cap a encoded.
        "processing_started_at": processing_started_at,
    }

    # Una línia amb els camps clau en lloc d'una línia per camp
    log(f"{event_id} | Camps: client={event_dict['orig_client']} "
        f"severity={event_dict['severity']} "
        f"src={event_dict['src_ip']} dest={event_dict['dest_ip']} "
        f"rule={str(event_dict['orig_rule_title'])[:60]}")

    try:
        save_event(event_id, event_dict)
    except Exception as e:
        log_error(f"{event_id} | ERROR INSERT alertas_encoded: {e}")
        if attempts + 1 >= MAX_ATTEMPTS:
            move_to_fallidas(row, f"DB error: {e}")
            term(f"FAIL {event_id} → fallidas (error BD)")
        else:
            mark_attempt_failed(row_id, attempts)
            term(f"WARN {event_id} → reintent {attempts + 1}/{MAX_ATTEMPTS} (error BD)")
        return

    delete_event_id(row_id)
    log(f"{event_id} | OK → alertas_encoded")
    term(f"OK   {event_id} → guardat a alertas_encoded")


# ============================================================
# Bucle principal
# ============================================================
def main():
    _setup_logger()

    log(f"=== auto_splunk.py ARRENCAT === "
        f"MAX_ATTEMPTS={MAX_ATTEMPTS} RETRY={RETRY_INTERVAL_SECONDS}s "
        f"BUSY={SLEEP_WHEN_BUSY}s IDLE={SLEEP_WHEN_IDLE}s "
        f"LOG_MAX={LOG_MAX_BYTES // (1024*1024)}MB FLUSH_EVERY={FLUSH_EVERY}")
    term(f"auto_splunk arrencat. Logs a: {LOGGER.current_path}")

    while _running:
        try:
            row = get_next_event_id()
            if row is None:
                time.sleep(SLEEP_WHEN_IDLE)
                continue

            process_one(row)
            time.sleep(SLEEP_WHEN_BUSY)

        except KeyboardInterrupt:
            break
        except Exception as e:
            log_error(f"ERROR INESPERAT: {e} | {''.join(traceback.format_exc().splitlines())}")
            term(f"ERROR inesperat: {e} (veure log)")
            time.sleep(SLEEP_AFTER_ERROR)

    log("=== auto_splunk.py ATURAT NETAMENT ===")
    if LOGGER:
        LOGGER.close()
    term("auto_splunk aturat netament.")


if __name__ == "__main__":
    main()