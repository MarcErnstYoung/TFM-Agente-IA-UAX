#!/usr/bin/env python3
"""
auto_predict.py
===============
Versió automàtica amb logs optimitzats.

- Per terminal: 1 línia per event processat.
- Per arxiu:    logs/auto_predict_YYYY-MM-DD.log   (data de creació del fitxer)
                Si omple i es crea un altre el mateix dia:
                  auto_predict_YYYY-MM-DD_2.log, _3.log ...
                Si el fitxer actiu d'un dia anterior encara té espai,
                es continua escrivint al mateix (el nom és la data d'obertura).
                Mida màxima per fitxer: LOG_MAX_BYTES (per defecte 10 MB).

Millores de rendiment vs versió anterior:
  - Comptador intern de bytes → sense os.path.getsize() per línia
  - Flush cada FLUSH_EVERY línies en lloc de per línia

Per arrencar:
    python3 auto_predict.py

Per parar (forma neta):
    Ctrl+C
"""
import os
import re
import time
import signal
import tempfile
import subprocess
import traceback
from datetime import datetime
import psycopg2
from psycopg2.extras import RealDictCursor
from encoder import FIELDS

# ============================================================
# CONFIG
# ============================================================
ENCODER_PATH = os.path.join("encoder.py")
PREDICTOR_SINGLE_PATH = os.path.join("predictor_single.py")
PREPROCESSOR_PATH = os.path.join("preprocessor.joblib")
MODEL_PATH = os.path.join("Model_V1.h5")

THRESHOLD = 0.29352656

DB_CONFIG = {
    "host": "localhost",
    "port": 1234,
    "database": "alertas_soc",
    "user": "postgres_user",
    "password": "DB_PASSWORD",
}

# Tunables del bucle
MAX_ATTEMPTS = 3
RETRY_INTERVAL_SECONDS = 30
SLEEP_WHEN_BUSY = 1
SLEEP_WHEN_IDLE = 5
SLEEP_AFTER_ERROR = 30

# Logs
LOG_DIR = "logs"
LOG_PREFIX = "auto_predict"
LOG_MAX_BYTES = 10 * 1024 * 1024   # 10 MB per fitxer
FLUSH_EVERY = 20                    # flush cada N línies (en lloc de per línia)

# ============================================================
# SISTEMA DE LOGS
# ============================================================
class FileLogger:
    """
    Escriu a logs/auto_predict_YYYY-MM-DD.log on la data és la d'obertura
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

        existing = self._find_resumable()
        if existing:
            self.current_path = existing
            self._byte_count = os.path.getsize(existing)
            self.fp = open(existing, "a", encoding="utf-8")
        else:
            self._open_new()

    def _all_log_files(self) -> list:
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
        for path in self._all_log_files():
            try:
                if os.path.getsize(path) < self.max_bytes:
                    return path
            except OSError:
                continue
        return None

    def _new_path(self) -> str:
        date_str = datetime.now().strftime("%Y-%m-%d")
        path = os.path.join(self.log_dir, f"{self.prefix}_{date_str}.log")
        n = 2
        while os.path.exists(path):
            path = os.path.join(self.log_dir, f"{self.prefix}_{date_str}_{n}.log")
            n += 1
        return path

    def _open_new(self):
        self.current_path = self._new_path()
        self._byte_count = 0
        self.fp = open(self.current_path, "a", encoding="utf-8")

    def _rotate_if_needed(self):
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
# BD
# ============================================================
def get_db_connection():
    return psycopg2.connect(**DB_CONFIG)


def get_next_encoded_event():
    conn = get_db_connection()
    try:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(
                """
                SELECT *
                FROM encoded.eventos
                WHERE last_attempt_at IS NULL
                   OR last_attempt_at < NOW() - (%s * INTERVAL '1 second')
                ORDER BY created_at ASC
                LIMIT 1;
                """,
                (RETRY_INTERVAL_SECONDS,)
            )
            return cur.fetchone()
    finally:
        conn.close()


def mark_attempt_failed(event_id: str, current_attempts: int):
    conn = get_db_connection()
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                UPDATE encoded.eventos
                SET attempt_count = %s,
                    last_attempt_at = NOW()
                WHERE source_event_id = %s;
                """,
                ((current_attempts or 0) + 1, event_id)
            )
        conn.commit()
    finally:
        conn.close()


def move_to_fallidos(event: dict, error_msg: str):
    """
    Predict-stage failure: insert into encoded.eventos_fallidos
    (the event never made it past the encoded stage because the model failed),
    then remove the event from encoded.eventos.
    """
    conn = get_db_connection()
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO encoded.eventos_fallidos (
                    source_event_id, orig_client, orig_index, orig_rule_title,
                    rule_description, severity, src_ip, dest_ip, dest_port,
                    action, signature, country, "user", hostname, sender,
                    subject, recipient, url, created_at,
                    attempt_count, last_attempt_at, last_error
                )
                VALUES (
                    %(source_event_id)s, %(orig_client)s, %(orig_index)s, %(orig_rule_title)s,
                    %(rule_description)s, %(severity)s, %(src_ip)s, %(dest_ip)s, %(dest_port)s,
                    %(action)s, %(signature)s, %(country)s, %(user)s, %(hostname)s, %(sender)s,
                    %(subject)s, %(recipient)s, %(url)s, %(created_at)s,
                    %(attempt_count)s, %(last_attempt_at)s, %(last_error)s
                );
                """,
                {
                    "source_event_id": event["source_event_id"],
                    "orig_client":     event.get("orig_client"),
                    "orig_index":      event.get("orig_index"),
                    "orig_rule_title": event.get("orig_rule_title"),
                    "rule_description":event.get("rule_description"),
                    "severity":        event.get("severity"),
                    "src_ip":          event.get("src_ip"),
                    "dest_ip":         event.get("dest_ip"),
                    "dest_port":       event.get("dest_port"),
                    "action":          event.get("action"),
                    "signature":       event.get("signature"),
                    "country":         event.get("country"),
                    "user":            event.get("user"),
                    "hostname":        event.get("hostname"),
                    "sender":          event.get("sender"),
                    "subject":         event.get("subject"),
                    "recipient":       event.get("recipient"),
                    "url":             event.get("url"),
                    "created_at":      event.get("created_at"),
                    "attempt_count":   (event.get("attempt_count") or 0) + 1,
                    "last_attempt_at": event.get("last_attempt_at"),
                    "last_error":      (error_msg or "")[:1000],
                }
            )
            cur.execute(
                "DELETE FROM encoded.eventos WHERE source_event_id = %s;",
                (event["source_event_id"],)
            )
        conn.commit()
    finally:
        conn.close()


def delete_encoded_event(event_id: str):
    conn = get_db_connection()
    try:
        with conn.cursor() as cur:
            cur.execute(
                "DELETE FROM encoded.eventos WHERE source_event_id = %s;",
                (event_id,)
            )
        conn.commit()
    finally:
        conn.close()


def save_prediction_event(event_dict: dict, result: int, score: float):
    """
    Desa el veredicte a predicted.eventos i tanca el cronòmetre del pipeline.
    """
    conn = get_db_connection()
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO predicted.eventos (
                    source_event_id, orig_client, orig_index, orig_rule_title,
                    rule_description, severity, src_ip, dest_ip, dest_port,
                    action, signature, country, "user", hostname, sender,
                    subject, recipient, url, prediction, client, score,
                    processing_started_at, processing_seconds
                )
                VALUES (
                    %(source_event_id)s, %(orig_client)s, %(orig_index)s, %(orig_rule_title)s,
                    %(rule_description)s, %(severity)s, %(src_ip)s, %(dest_ip)s, %(dest_port)s,
                    %(action)s, %(signature)s, %(country)s, %(user_val)s, %(hostname)s, %(sender)s,
                    %(subject)s, %(recipient)s, %(url)s, %(prediction)s, %(client)s, %(score)s,
                    %(processing_started_at)s,
                    CASE WHEN %(processing_started_at)s IS NOT NULL
                         THEN EXTRACT(EPOCH FROM (now() - %(processing_started_at)s))
                         ELSE NULL END
                )
                ON CONFLICT (source_event_id) DO UPDATE SET
                    orig_client = EXCLUDED.orig_client,
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
                    prediction = EXCLUDED.prediction,
                    client = EXCLUDED.client,
                    score = EXCLUDED.score,
                    processing_started_at = EXCLUDED.processing_started_at,
                    processing_seconds = CASE
                        WHEN EXCLUDED.processing_started_at IS NOT NULL
                        THEN EXTRACT(EPOCH FROM (now() - EXCLUDED.processing_started_at))
                        ELSE NULL END,
                    created_at = now()
                RETURNING processing_seconds;
                """,
                {
                    "source_event_id":   event_dict.get("source_event_id"),
                    "orig_client":       event_dict.get("orig_client"),
                    "orig_index":        event_dict.get("orig_index"),
                    "orig_rule_title":   event_dict.get("orig_rule_title"),
                    "rule_description":  event_dict.get("rule_description"),
                    "severity":          event_dict.get("severity"),
                    "src_ip":            event_dict.get("src_ip"),
                    "dest_ip":           event_dict.get("dest_ip"),
                    "dest_port":         event_dict.get("dest_port"),
                    "action":            event_dict.get("action"),
                    "signature":         event_dict.get("signature"),
                    "country":           event_dict.get("country"),
                    "user_val":          event_dict.get("user"),  # <--- Cambiado a user_val para evitar colisión SQL
                    "hostname":          event_dict.get("hostname"),
                    "sender":            event_dict.get("sender"),
                    "subject":           event_dict.get("subject"),
                    "recipient":         event_dict.get("recipient"),
                    "url":               event_dict.get("url"),
                    "processing_started_at": event_dict.get("processing_started_at"),
                    "prediction":        bool(result),
                    "client":            False,
                    "score":             float(score)
                }
            )
            returned = cur.fetchone()
        conn.commit()
        if returned and returned[0] is not None:
            return float(returned[0])
        return None
    finally:
        conn.close()


# ============================================================
# Encoder + model
# ============================================================
def dict_to_kv_blob(event: dict) -> str:
    oc = str(event.get("orig_client", "") or "null")
    kv = dict(event)
    kv["search_name"] = f'Threat - {oc}-DUMMY: Generic Splunk Alert - Rule'
    parts = []
    for k, v in kv.items():
        if v is None:
            continue
        s = str(v)
        if any(ch in s for ch in [",", '"', "\n", "\r"]):
            s = s.replace('"', '\\"')
            s = f'"{s}"'
        parts.append(f"{k}={s}")
    return ", ".join(parts)


def run_encoder(kv_blob: str) -> str:
    proc = subprocess.run(
        ["python3", ENCODER_PATH],
        input=kv_blob,
        text=True,
        capture_output=True
    )
    if proc.returncode != 0:
        raise RuntimeError(proc.stderr.strip() or "encoder.py error")
    return (proc.stdout or "").strip()


def write_one_row_csv(row_csv_line: str) -> str:
    fd, path = tempfile.mkstemp(prefix="soc_", suffix=".csv")
    os.close(fd)
    with open(path, "w", encoding="utf-8", newline="") as f:
        f.write(",".join(FIELDS) + "\n")
        f.write(row_csv_line + "\n")
    return path


def run_predictor_single(csv_path: str) -> tuple:
    proc = subprocess.run(
        [
            "python3",
            PREDICTOR_SINGLE_PATH,
            csv_path,
            "--preprocessor", PREPROCESSOR_PATH,
            "--model", MODEL_PATH,
        ],
        text=True,
        capture_output=True
    )
    if proc.returncode != 0:
        raise RuntimeError(proc.stderr.strip() or "predictor_single.py error")

    out = (proc.stdout or "").strip()
    nums = re.findall(r"[-+]?\d*\.\d+|\d+", out)
    if not nums:
        raise RuntimeError("predictor_single.py no ha retornat cap número. stdout=" + out[:200])
    return float(nums[-1]), out


# ============================================================
# Processat d'UN event
# ============================================================
def process_one(event: dict):
    event_id = event["source_event_id"]
    attempts = event.get("attempt_count") or 0
    csv_path = None

    log(f"{event_id} | INICI intent {attempts + 1}/{MAX_ATTEMPTS}")

    try:
        # 1) KV blob + Encoder
        t0 = time.time()
        kv_blob = dict_to_kv_blob(dict(event))
        row_csv_line = run_encoder(kv_blob)
        log(f"{event_id} | Encoder OK ({time.time() - t0:.2f}s) | CSV {len(row_csv_line)} chars")

        # 2) CSV temporal + Model
        csv_path = write_one_row_csv(row_csv_line)

        t0 = time.time()
        score, _ = run_predictor_single(csv_path)
        elapsed = time.time() - t0

        # 3) Threshold i resultat
        result = 1 if score >= THRESHOLD else 0
        verdict = "TP" if result else "FP"
        op = ">=" if score >= THRESHOLD else "<"
        log(f"{event_id} | Predictor OK ({elapsed:.2f}s) | "
            f"score={score:.6f} {op} {THRESHOLD} → {verdict}")

        # 4) Guardar i esborrar
        proc_seconds = save_prediction_event(dict(event), result, score)
        delete_encoded_event(event_id)
        if proc_seconds is not None:
            log(f"{event_id} | OK → alertas_predicted (prediction={verdict}, "
                f"score={score:.4f}, temps_pipeline={proc_seconds:.2f}s)")
            term(f"OK   {event_id} → score={score:.4f} {verdict} ({proc_seconds:.2f}s)")
        else:
            log(f"{event_id} | OK → alertas_predicted (prediction={verdict}, score={score:.4f})")
            term(f"OK   {event_id} → score={score:.4f} {verdict}")

    except Exception as e:
        err_msg = str(e)
        log_error(f"{event_id} | ERROR: {err_msg}")
        # Stack trace condensat en una línia
        tb = " | ".join(traceback.format_exc().splitlines()[-4:])
        log_error(f"{event_id} | Traceback: {tb}")

        if attempts + 1 >= MAX_ATTEMPTS:
            try:
                move_to_fallidos(dict(event), err_msg)
                log_error(f"{event_id} | FAIL → encoded.eventos_fallidos ({MAX_ATTEMPTS} intents exhaurits)")
                term(f"FAIL {event_id} → fallidos ({MAX_ATTEMPTS} intents)")
            except Exception as e2:
                log_error(f"{event_id} | ERROR move_to_fallidos: {repr(e2)}")
                log_error(traceback.format_exc())
                term(f"ERROR {event_id} → move_to_fallidos: {e2}")
        else:
            try:
                mark_attempt_failed(event_id, attempts)
                log_warn(f"{event_id} | WARN reintent {attempts + 1}/{MAX_ATTEMPTS}: {err_msg[:80]}")
                term(f"WARN {event_id} → reintent {attempts + 1}/{MAX_ATTEMPTS}")
            except Exception as e2:
                log_error(f"{event_id} | ERROR mark_attempt_failed: {e2}")
                term(f"ERROR {event_id} → no s'ha pogut marcar com a fallat")
    finally:
        if csv_path and os.path.exists(csv_path):
            try:
                os.remove(csv_path)
            except Exception:
                pass


# ============================================================
# Bucle principal
# ============================================================
def main():
    _setup_logger()

    log(f"=== auto_predict.py ARRENCAT === "
        f"THRESHOLD={THRESHOLD} MODEL={MODEL_PATH} "
        f"MAX_ATTEMPTS={MAX_ATTEMPTS} RETRY={RETRY_INTERVAL_SECONDS}s "
        f"LOG_MAX={LOG_MAX_BYTES // (1024*1024)}MB FLUSH_EVERY={FLUSH_EVERY}")
    term(f"auto_predict arrencat. Logs a: {LOGGER.current_path}")

    while _running:
        try:
            event = get_next_encoded_event()
            if event is None:
                time.sleep(SLEEP_WHEN_IDLE)
                continue

            process_one(event)
            time.sleep(SLEEP_WHEN_BUSY)

        except KeyboardInterrupt:
            break
        except Exception as e:
            log_error(f"ERROR INESPERAT: {e} | {''.join(traceback.format_exc().splitlines())}")
            term(f"ERROR inesperat: {e} (veure log)")
            time.sleep(SLEEP_AFTER_ERROR)

    log("=== auto_predict.py ATURAT NETAMENT ===")
    if LOGGER:
        LOGGER.close()
    term("auto_predict aturat netament.")


if __name__ == "__main__":
    main()