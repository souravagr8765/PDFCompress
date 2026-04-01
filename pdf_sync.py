# =============================================================
# pdf_sync.py — PDF Compression & Google Drive Sync
# =============================================================
# Required .env keys:
#
# NHOST_CONNECTION_STRING  — PostgreSQL connection string from Nhost
# SMTP_HOST                — SMTP server hostname
# SMTP_PORT                — SMTP port (default: 587)
# SMTP_USER                — SMTP login username
# SMTP_PASSWORD            — SMTP login password
# REPORT_RECIPIENT         — Email address to receive run reports
# LOKI_URL                 — Loki server URL (optional)
# LOKI_USERNAME            — Loki username (optional)
# LOKI_PASSWORD            — Loki password (optional)
# JOB_NAME                 — Loki job label (optional)
# TELEGRAM_BOT_API         — Telegram Bot token (optional)
# CHAT_ID                  — Comma-separated Telegram chat IDs (optional)
# =============================================================
import sys
import os
import loki_logger as logger
from datetime import datetime
import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
import requests
from dotenv import load_dotenv

load_dotenv(os.path.join(os.path.dirname(os.path.abspath(__file__)), '.env'))

# =============================================================================
# CONFIGURATION
# =============================================================================

# --- Paths & Local Storage ---
WATCH_FOLDER = os.getenv("WATCH_FOLDER", "")
LOCAL_DB = os.path.join(os.path.dirname(os.path.abspath(__file__)), os.getenv("LOCAL_DB", "local_cache.db"))
LOCK_FILE = os.path.join(os.path.dirname(os.path.abspath(__file__)), ".pdf_sync.lock")
LOG_FILE = os.path.join(os.path.dirname(os.path.abspath(__file__)), os.getenv("LOG_FILE", "compressor.log"))
TEMP_SUFFIX = "_compressed_tmp.pdf"

# --- Ghostscript Settings ---
IMAGE_DPI = os.getenv("IMAGE_DPI", "150")
JPEG_QUALITY = os.getenv("JPEG_QUALITY", "75")

# --- Google Drive / rclone ---
GDRIVE_REMOTE = os.getenv("GDRIVE_REMOTE", "")
GDRIVE_FOLDER = os.getenv("GDRIVE_FOLDER", "")

# --- Nhost Online Database ---
NHOST_CONNECTION_STRING = os.getenv("NHOST_CONNECTION_STRING", "")

# --- SMTP Email Report ---
SMTP_HOST = os.getenv("SMTP_HOST", "")
SMTP_PORT = int(os.getenv("SMTP_PORT", "587"))
SMTP_USER = os.getenv("SMTP_USER", "")
SMTP_PASSWORD = os.getenv("SMTP_PASSWORD", "")
REPORT_RECIPIENT = os.getenv("REPORT_RECIPIENT", "")

# --- Loki Logging ---
LOKI_URL = os.getenv("LOKI_URL", "")
LOKI_USERNAME = os.getenv("LOKI_USERNAME", "")
LOKI_PASSWORD = os.getenv("LOKI_PASSWORD", "")
JOB_NAME = os.getenv("JOB_NAME", "")

# --- Telegram Notification ---
TELEGRAM_BOT_API = os.getenv("TELEGRAM_BOT_API", "")
CHAT_IDS = [cid.strip() for cid in os.getenv("CHAT_ID", "").split(",") if cid.strip()]
# =============================================================================

run_stats = {
    "start_time": None,
    "end_time": None,
    "files_compressed": 0,
    "files_skipped_larger": 0,
    "files_upload_failed": 0,
    "retry_recovered": 0,
    "retry_still_failed": 0,
    "run_original_bytes": 0,
    "run_compressed_bytes": 0
}

def send_report_email(stats):
    if not SMTP_HOST or not SMTP_USER or not SMTP_PASSWORD or not REPORT_RECIPIENT:
        if 'logger' in globals():
            logger.warning("Email report skipped — SMTP credentials not configured")
        return

    try:
        conn = sqlite3.connect(LOCAL_DB)
        cursor = conn.cursor()
        
        cursor.execute("SELECT COUNT(*) FROM processed_files")
        total_rows = cursor.fetchone()[0] or 0
        
        cursor.execute("SELECT COUNT(*) FROM processed_files WHERE status = 'compressed'")
        total_compressed = cursor.fetchone()[0] or 0
        
        cursor.execute("SELECT COUNT(*) FROM processed_files WHERE status = 'skipped_larger'")
        total_skipped = cursor.fetchone()[0] or 0
        
        cursor.execute("SELECT SUM(original_size), SUM(compressed_size) FROM processed_files")
        sums = cursor.fetchone()
        alltime_orig = sums[0] or 0
        alltime_comp = sums[1] or 0
        conn.close()
        
        start_time = stats.get('start_time')
        end_time = stats.get('end_time')
        duration_sec = int((end_time - start_time).total_seconds()) if start_time and end_time else 0
        duration_str = f"{duration_sec // 3600}h {(duration_sec % 3600) // 60}m {duration_sec % 60}s"
        
        run_orig = stats.get('run_original_bytes', 0)
        run_comp = stats.get('run_compressed_bytes', 0)
        run_saved = run_orig - run_comp
        run_pct = (run_saved / run_orig * 100) if run_orig > 0 else 0
        
        alltime_saved = alltime_orig - alltime_comp
        
        body = f"""================================================
PDF Sync — Run Report
================================================

RUN SUMMARY
  Start Time    : {start_time.strftime('%Y-%m-%d %H:%M:%S') if start_time else ''}
  End Time      : {end_time.strftime('%Y-%m-%d %H:%M:%S') if end_time else ''}
  Duration      : {duration_str}

THIS RUN
  Compressed         : {stats.get('files_compressed', 0)} files
  Skipped (optimal)  : {stats.get('files_skipped_larger', 0)} files
  Upload Failed      : {stats.get('files_upload_failed', 0)} files
  Retried & Fixed    : {stats.get('retry_recovered', 0)} files
  Still Failed       : {stats.get('retry_still_failed', 0)} files
  Original Size      : {format_size(run_orig)}
  Compressed Size    : {format_size(run_comp)}
  Space Saved        : {format_size(run_saved)} ({run_pct:.1f}% reduction)

ALL TIME
  Total Files Tracked : {total_rows}
  Total Compressed    : {total_compressed}
  Total Skipped       : {total_skipped}
  Total Space Saved   : {format_size(alltime_saved)}

================================================"""
        
        msg = MIMEMultipart()
        msg['From'] = SMTP_USER
        msg['To'] = REPORT_RECIPIENT
        msg['Subject'] = f"PDF Sync Report — {datetime.now().strftime('%Y-%m-%d')}"
        msg.attach(MIMEText(body, 'plain'))
        
        server = smtplib.SMTP(SMTP_HOST, SMTP_PORT)
        server.starttls()
        server.login(SMTP_USER, SMTP_PASSWORD)
        server.send_message(msg)
        server.quit()
        
        if 'logger' in globals():
            logger.info("Email report sent successfully")
    except Exception as e:
        if 'logger' in globals():
            logger.error(f"Failed to send email report: {e}")

def acquire_lock():
    if os.path.exists(LOCK_FILE):
        try:
            with open(LOCK_FILE, "r") as f:
                content = f.read().strip()
                if "PID:" in content:
                    parts = content.split("STARTED:")
                    pid_str = parts[0].replace("PID:", "").strip()
                    if pid_str.isdigit():
                        pid = int(pid_str)
                        try:
                            os.kill(pid, 0)
                        except OSError:
                            print(f"WARNING: Stale lock file found (PID {pid} is not running). Removing stale lock and continuing.")
                            os.remove(LOCK_FILE)
                        else:
                            print(f"CRITICAL: Another instance is already running (lock file found at {LOCK_FILE}). Exiting.")
                            sys.exit(1)
                    else:
                        print(f"CRITICAL: Another instance is already running (lock file found at {LOCK_FILE}). Exiting.")
                        sys.exit(1)
                else:
                    print(f"CRITICAL: Another instance is already running (lock file found at {LOCK_FILE}). Exiting.")
                    sys.exit(1)
        except SystemExit:
            raise
        except Exception:
            print(f"CRITICAL: Another instance is already running (lock file found at {LOCK_FILE}). Exiting.")
            sys.exit(1)
            
    try:
        pid = os.getpid()
        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        with open(LOCK_FILE, "w") as f:
            f.write(f"PID:{pid} STARTED:{timestamp}")
        print(f"INFO: Lock acquired (PID {pid})")
    except Exception as e:
        print(f"ERROR: Failed to acquire lock: {e}")
        sys.exit(1)

def release_lock():
    if os.path.exists(LOCK_FILE):
        try:
            os.remove(LOCK_FILE)
            if 'logger' in globals():
                logger.info("Lock released")
            else:
                print("INFO: Lock released")
        except Exception as e:
            if 'logger' in globals():
                logger.warning(f"Failed to release lock: {e}")
            else:
                print(f"WARNING: Failed to release lock: {e}")

acquire_lock()

import shutil
import subprocess
import traceback
import time
import sqlite3
import atexit
import psycopg2
from psycopg2.extras import execute_values

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
# Setup logging
# logging.basicConfig(
#     level=logging.INFO,
#     format='%(asctime)s [%(levelname)s] %(message)s',
#     handlers=[
#         logging.FileHandler(LOG_FILE, encoding='utf-8'),
#         logging.StreamHandler(sys.stdout)
#    ] 
# )
# logger = logging.getLogger(__name__)

# Global reference to the background loki logger process
_loki_process = None

def init_local_db():
    conn = sqlite3.connect(LOCAL_DB)
    cursor = conn.cursor()
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS processed_files (
            file_path TEXT PRIMARY KEY,
            original_size INTEGER,
            compressed_size INTEGER,
            status TEXT,
            processed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)
    conn.commit()
    conn.close()
    logger.info("Local SQLite database initialised")

def get_nhost_conn():
    if not NHOST_CONNECTION_STRING:
        logger.warning("NHOST_CONNECTION_STRING not set \u2014 online DB unavailable")
        return None
    try:
        conn = psycopg2.connect(NHOST_CONNECTION_STRING)
        return conn
    except Exception as e:
        logger.error(f"Error connecting to Nhost DB: {e}")
        return None

def ensure_nhost_table(conn):
    try:
        cursor = conn.cursor()
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS processed_files (
                file_path TEXT PRIMARY KEY,
                original_size BIGINT,
                compressed_size BIGINT,
                status TEXT,
                processed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)
        conn.commit()
    except Exception as e:
        logger.error(f"Error creating Nhost table: {e}")

def reconcile_databases():
    try:
        local_conn = sqlite3.connect(LOCAL_DB)
        local_cursor = local_conn.cursor()
        local_cursor.execute("SELECT COUNT(*) FROM processed_files")
        local_count = local_cursor.fetchone()[0]
        
        nhost_conn = get_nhost_conn()
        if not nhost_conn:
            logger.warning("Nhost is unavailable, skipping reconciliation.")
            local_conn.close()
            return
            
        ensure_nhost_table(nhost_conn)
        nhost_cursor = nhost_conn.cursor()
        
        nhost_cursor.execute("SELECT COUNT(*) FROM processed_files")
        nhost_count = nhost_cursor.fetchone()[0]
        
        if local_count == nhost_count:
            logger.info(f"Databases in sync ({local_count} records)")
            local_conn.close()
            nhost_conn.close()
            return
            
        local_cursor.execute("SELECT file_path FROM processed_files")
        local_paths = set(row[0] for row in local_cursor.fetchall())
        
        nhost_cursor.execute("SELECT file_path FROM processed_files")
        nhost_paths = set(row[0] for row in nhost_cursor.fetchall())
        
        only_in_local = local_paths - nhost_paths
        only_in_nhost = nhost_paths - local_paths
        
        if only_in_local:
            local_cursor.execute("SELECT file_path, original_size, compressed_size, status, processed_at FROM processed_files")
            local_all = {row[0]: row for row in local_cursor.fetchall()}
            push_batch = [local_all[p] for p in only_in_local]
            execute_values(
                nhost_cursor,
                "INSERT INTO processed_files (file_path, original_size, compressed_size, status, processed_at) VALUES %s ON CONFLICT (file_path) DO NOTHING",
                push_batch
            )
            nhost_conn.commit()
            logger.info(f"Pushed {len(push_batch)} missing records to Nhost")
            
        if only_in_nhost:
            nhost_cursor.execute("SELECT file_path, original_size, compressed_size, status, processed_at FROM processed_files")
            nhost_all = {row[0]: row for row in nhost_cursor.fetchall()}
            pull_batch = [nhost_all[p] for p in only_in_nhost]
            local_cursor.executemany(
                "INSERT OR REPLACE INTO processed_files (file_path, original_size, compressed_size, status, processed_at) VALUES (?, ?, ?, ?, ?)",
                pull_batch
            )
            local_conn.commit()
            logger.info(f"Pulled {len(pull_batch)} missing records from local DB")
            
        logger.info("Reconciliation complete")
        
        local_conn.close()
        nhost_conn.close()
    except Exception as e:
        logger.error(f"Reconciliation failed: {e}")

def cleanup():
    """Terminate background processes on exit."""
    global _loki_process
    if _loki_process is not None:
        logger.info("Terminating background Loki logger...")
        try:
             # Send termination signal via log file
             logger.info("LOKI_LOGGER_TERMINATE")
             _loki_process.wait(timeout=5)
        except subprocess.TimeoutExpired:
            _loki_process.kill()
        except Exception as e:
            logger.error(f"Error terminating Loki logger: {e}")
        logger.info("Loki logger terminated.")

atexit.register(cleanup)

def compress_pdf(input_path, output_path):
    command = [
        "gs",
        "-sDEVICE=pdfwrite",
        "-dCompatibilityLevel=1.4",
        "-dPDFSETTINGS=/ebook",
        "-dNOPAUSE",
        "-dQUIET",
        "-dBATCH",
        f"-sOutputFile={output_path}",
        input_path
   ] 
    logger.info(f"Executing Ghostscript command: {' '.join(command)}")
    result = subprocess.run(command, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    if result.returncode != 0:
        error_msg = result.stderr.decode()
        logger.error(f"Ghostscript failed for {input_path}. Error: {error_msg}")
        raise RuntimeError(f"Ghostscript failed: {error_msg}")
    logger.info(f"Ghostscript compression completed for {input_path}")

def format_size(size_val):
    if size_val is None:
        return "0.0B"
    if size_val < 1024:
        return f"{size_val}B"
    elif size_val < 1024 * 1024:
        return f"{size_val / 1024:.1f}KB"
    elif size_val < 1024 * 1024 * 1024:
        return f"{size_val / (1024 * 1024):.1f}MB"
    else:
        return f"{size_val / (1024 * 1024 * 1024):.1f}GB"

def update_file_status(file_key, orig_size, final_size, status):
    """Helper to update file status in both local SQLite and Nhost DBs."""
    try:
        conn = sqlite3.connect(LOCAL_DB)
        cursor = conn.cursor()
        cursor.execute("""
            INSERT OR REPLACE INTO processed_files 
            (file_path, original_size, compressed_size, status, processed_at)
            VALUES (?, ?, ?, ?, CURRENT_TIMESTAMP)
        """, (file_key, orig_size, final_size, status))
        conn.commit()
        conn.close()
    except Exception as e:
        logger.error(f"Failed to update local DB status for {file_key}: {e}")

    try:
        nhost_conn = get_nhost_conn()
        if nhost_conn:
            nhost_cursor = nhost_conn.cursor()
            nhost_cursor.execute("""
                INSERT INTO processed_files (file_path, original_size, compressed_size, status, processed_at)
                VALUES (%s, %s, %s, %s, CURRENT_TIMESTAMP)
                ON CONFLICT (file_path) DO UPDATE SET
                    original_size = EXCLUDED.original_size,
                    compressed_size = EXCLUDED.compressed_size,
                    status = EXCLUDED.status,
                    processed_at = CURRENT_TIMESTAMP
            """, (file_key, orig_size, final_size, status))
            nhost_conn.commit()
            nhost_conn.close()
    except Exception as e:
        logger.warning(f"Failed to sync record {os.path.basename(file_key)} to Nhost: {e}")

def retry_failed_uploads():
    """Dedicated retry pass for uploads that failed."""
    stats = {"retried": 0, "recovered": 0, "still_failed": 0}
    try:
        conn = sqlite3.connect(LOCAL_DB)
        cursor = conn.cursor()
        cursor.execute('SELECT file_path, original_size, compressed_size, status FROM processed_files WHERE status = "upload_failed"')
        rows = cursor.fetchall()
        conn.close()
        
        for row in rows:
            db_file_path, orig_size, comp_size, status = row
            stats["retried"] += 1
            
            if os.path.isabs(db_file_path):
                full_path = db_file_path
            else:
                full_path = os.path.abspath(os.path.join(WATCH_FOLDER, db_file_path))
                
            if not os.path.exists(full_path):
                logger.warning(f"Failed upload file missing locally, cannot retry: {full_path}")
                stats["still_failed"] += 1
                continue
                
            upload_path = full_path
            rel_path = os.path.relpath(upload_path, WATCH_FOLDER)
            rel_dir = os.path.dirname(rel_path).replace("\\", "/")
            dest_folder = f"{GDRIVE_FOLDER}/{rel_dir}" if rel_dir and rel_dir != "." else GDRIVE_FOLDER
            
            cmd = ["rclone", "copy", upload_path, f"{GDRIVE_REMOTE}:{dest_folder}", "--no-traverse"]
            logger.info(f"Retrying rclone upload for failed file: {' '.join(cmd)}")
            proc = subprocess.run(cmd, capture_output=True, text=True)
            
            if proc.returncode == 0:
                stats["recovered"] += 1
                new_status = "skipped_larger" if orig_size == comp_size else "compressed"
                update_file_status(db_file_path, orig_size, comp_size, new_status)
                logger.info(f"Retry upload succeeded: {full_path}")
            else:
                stats["still_failed"] += 1
                error_msg = proc.stderr.strip() or 'Unknown error'
                logger.warning(f"Retry upload failed again: {full_path}")
    except Exception as e:
        logger.error(f"Error in retry_failed_uploads: {e}")
        
    return stats

def cleanup_temp_files():
    count = 0
    if not os.path.exists(WATCH_FOLDER):
        return 0
    for root, dirs, files in os.walk(WATCH_FOLDER):
        for file in files:
            if file.lower().endswith("_temp.pdf"):
                full_path = os.path.join(root, file)
                try:
                    os.remove(full_path)
                    logger.info(f"Removed stale temp file: {full_path}")
                    count += 1
                except Exception as e:
                    logger.warning(f"Failed to remove stale temp file {full_path}: {e}")
    logger.info(f"Temp file cleanup complete. Removed {count} file(s).")
    return count

def send_telegram_file_list(uploaded_files):
    """Send the list of successfully uploaded files to all Telegram chat IDs.
    
    Args:
        uploaded_files: list of tuples (filename, orig_size, final_size, reduction_pct)
    """
    if not TELEGRAM_BOT_API or not CHAT_IDS:
        logger.warning("Telegram notification skipped — bot token or chat IDs not configured")
        return

    if not uploaded_files:
        logger.info("No successfully uploaded files to report via Telegram")
        return

    header = f"✅ *PDF Sync — Uploaded Files*\n_{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}_\n\n"
    lines = [f"{i+1}. `{name}` ({format_size(orig)} → {format_size(final)}, {pct}% reduced)" for i, (name, orig, final, pct) in enumerate(uploaded_files)]
    body = header + "\n".join(lines) + f"\n\n*Total: {len(uploaded_files)} file(s)*"

    # Telegram has a 4096 char limit per message; split if needed
    chunks = [body[i:i+4096] for i in range(0, len(body), 4096)]

    url = f"https://api.telegram.org/bot{TELEGRAM_BOT_API}/sendMessage"
    for chat_id in CHAT_IDS:
        for chunk in chunks:
            try:
                resp = requests.post(url, json={
                    "chat_id": chat_id,
                    "text": chunk,
                    "parse_mode": "Markdown"
                }, timeout=15)
                if resp.status_code == 200:
                    logger.info(f"Telegram file list sent to chat {chat_id}")
                else:
                    logger.warning(f"Telegram API returned {resp.status_code} for chat {chat_id}: {resp.text}")
            except Exception as e:
                logger.error(f"Failed to send Telegram message to chat {chat_id}: {e}")

def main():
    # global _loki_process
    # run_stats["start_time"] = datetime.now()

    # start_pos = 0
    # if os.path.exists(LOG_FILE):
    #     start_pos = os.path.getsize(LOG_FILE)

    # logger.info("Starting PDF Sync Script")
    init_local_db()
    
    # # Start Loki Logger if environment variables are set
    # loki_url = os.environ.get("LOKI_URL")
    # if loki_url:
    #     loki_script_path = os.path.join(BASE_DIR, "loki_logger.py")
    #     if os.path.exists(loki_script_path):
    #         logger.info(f"Starting background Loki logger streaming to {loki_url}")
    #         try:
    #             # Start as a separate process group so it doesn't receive signals meant for the parent
    #             _loki_process = subprocess.Popen(
    #                 [sys.executable, loki_script_path, LOG_FILE, str(start_pos)],
    #                 stdout=subprocess.DEVNULL,
    #                 stderr=subprocess.DEVNULL,
    #                 start_new_session=True # Windows equivalent of os.setsid or preexec_fn=os.setsid
    #             )
    #         except Exception as e:
    #             logger.error(f"Failed to start Loki logger subprocess: {e}")
    #     else:
    #          logger.warning("loki_logger.py not found. Loki logging will not be available.")
             
    reconcile_databases()
    cleanup_temp_files()

    if not WATCH_FOLDER or not GDRIVE_REMOTE or not GDRIVE_FOLDER:
        logger.error("WATCH_FOLDER, GDRIVE_REMOTE, or GDRIVE_FOLDER is not configured.")
        sys.exit(1)

    if not os.path.exists(WATCH_FOLDER):
        logger.error(f"WATCH_FOLDER '{WATCH_FOLDER}' does not exist.")
        sys.exit(1)

    if shutil.which("rclone") is None:
        logger.error("rclone not found in PATH.")
        sys.exit(1)

    if not shutil.which("gs"):
        logger.error("Ghostscript (gs) not found. Run: pkg install ghostscript")
        sys.exit(1)

    processed_count = 0
    skipped_count = 0
    error_count = 0

    scan_files = []
    try:
        logger.info(f"Scanning WATCH_FOLDER '{WATCH_FOLDER}' for PDF files...")
        for root, dirs, files in os.walk(WATCH_FOLDER):
            for file in files:
                if file.lower().endswith(".pdf") and TEMP_SUFFIX not in file:
                    full_path = os.path.abspath(os.path.join(root, file))
                    scan_files.append(full_path)
        logger.info(f"Found {len(scan_files)} PDF files to process.")
    except Exception as e:
        logger.error(f"Could not read WATCH_FOLDER: {e}")
        sys.exit(1)

    uploaded_files = []  # Track successfully uploaded files for Telegram notification

    for full_path in scan_files:
        file_name = os.path.basename(full_path)
        
        if not os.path.exists(full_path):
            continue
        orig_size = os.path.getsize(full_path)
        
        file_key = os.path.relpath(full_path, WATCH_FOLDER).replace("\\", "/")

        conn = sqlite3.connect(LOCAL_DB)
        cursor = conn.cursor()
        cursor.execute("""
            SELECT file_path, original_size, compressed_size, status 
            FROM processed_files 
            WHERE file_path = ? OR file_path LIKE ? OR file_path LIKE ?
        """, (file_key, '%/' + file_key, '%\\' + file_key.replace('/', '\\')))
        row = cursor.fetchone()
        conn.close()
        
        db_key_to_update = row[0] if row else file_key
        
        retry_upload_only = False
        status = None
        if row:
            db_orig_size = row[1]
            db_compressed_size = row[2]
            db_status = row[3]
            
            if db_status == "upload_failed":
                logger.info(f"Retrying upload for previously failed file: {full_path}")
                retry_upload_only = True
                orig_size = db_orig_size
                final_size = db_compressed_size
                status = "skipped_larger" if db_orig_size == db_compressed_size else "compressed"
            elif orig_size == db_compressed_size:
                skipped_count += 1
                continue
            
        logger.info(f"Processing {file_name}...")
        temp_path = full_path[:-4] + TEMP_SUFFIX
        
        try:
            if not retry_upload_only:
                # a. Compress it using Ghostscript
                logger.info(f"Original file size {file_name}: {format_size(orig_size)}")
                compress_pdf(full_path, temp_path)
                
                # b. Validate the compressed file
                if not os.path.exists(temp_path) or os.path.getsize(temp_path) == 0:
                    raise Exception("Compressed temp file is missing or 0 bytes")
                    
                new_size = os.path.getsize(temp_path)
                
                # c. Replace the original
                if new_size < orig_size:
                    logger.info(f"Replacing original file. Compressed size: {format_size(new_size)}")
                    os.remove(full_path)
                    os.rename(temp_path, full_path)
                    final_size = new_size
                    status = "compressed"
                else:
                    # Compressed file larger or equal; keep original
                    logger.info(f"Compression did not reduce size. Keeping original: {format_size(orig_size)}")
                    os.remove(temp_path)
                    final_size = orig_size
                    status = "skipped_larger"
                    
                # d & e. Write to both DBs immediately after compression
                update_file_status(db_key_to_update, orig_size, final_size, status)

                if status == "compressed":
                    run_stats["files_compressed"] += 1
                elif status == "skipped_larger":
                    run_stats["files_skipped_larger"] += 1
                
                run_stats["run_original_bytes"] += orig_size
                run_stats["run_compressed_bytes"] += final_size

            reduction_pct = 0
            if orig_size > 0:
                reduction_pct = int(((orig_size - final_size) / orig_size) * 100)
                
            upload_path = full_path
                
            # f. Upload to Google Drive using rclone
            rel_path = os.path.relpath(upload_path, WATCH_FOLDER)
            rel_dir = os.path.dirname(rel_path).replace("\\", "/")
            dest_folder = f"{GDRIVE_FOLDER}/{rel_dir}" if rel_dir and rel_dir != "." else GDRIVE_FOLDER
            
            cmd = [
                "rclone", "copy", upload_path, 
                f"{GDRIVE_REMOTE}:{dest_folder}", "--no-traverse"
           ] 
            logger.info(f"Executing rclone upload: {' '.join(cmd)}")
            proc = subprocess.run(cmd, capture_output=True, text=True)
            if proc.returncode != 0:
                error_msg = proc.stderr.strip() or 'Unknown error'
                # h. If rclone fails: update status to upload_failed in DBs, log error, continue
                update_file_status(db_key_to_update, orig_size, final_size, "upload_failed")
                logger.error(f"rclone upload failed for {full_path}. stderr: {error_msg}")
                error_count += 1
                run_stats["files_upload_failed"] += 1
                continue # continue to next file without raising exception
                
            # g. If rclone succeeds: no DB change needed unless we were retrying
            if retry_upload_only:
                update_file_status(db_key_to_update, orig_size, final_size, status)
                
            logger.info(f"Upload successful for {file_name}")
            uploaded_files.append((file_name, orig_size, final_size, reduction_pct))
            
            # write a log entry
            log_msg = f"{file_name} [SUCCESS] {format_size(orig_size)} \u2192 {format_size(final_size)} ({reduction_pct}% reduction)] uploaded to {GDRIVE_REMOTE}:{dest_folder}"
            logger.info(log_msg)
            
            processed_count += 1
            
        except Exception as e:
            # error handling block
            error_count += 1
            if os.path.exists(temp_path):
                try:
                    os.remove(temp_path)
                except:
                    pass
            logger.error(f"{file_name} [ERROR] {str(e)}{traceback.format_exc()}")
            
    retry_results = retry_failed_uploads()
    run_stats["retry_recovered"] = retry_results.get("recovered", 0)
    run_stats["retry_still_failed"] = retry_results.get("still_failed", 0)
    
    if retry_results["retried"] > 0:
        logger.info(f"Retry pass completed: {retry_results}")
        
    run_summary = {
        "processed": processed_count,
        "skipped": skipped_count,
        "errors": error_count,
        "retried": retry_results["retried"],
        "recovered": retry_results["recovered"],
        "still_failed": retry_results["still_failed"]
    }
        
    summary_msg = f"PDF Sync complete. Processed: {run_summary['processed']} [Skipped: {run_summary['skipped']} [Errors: {run_summary['errors']}] Retried: {run_summary['retried']} (Recovered: {run_summary['recovered']}, Still Failed: {run_summary['still_failed']})"
    logger.info(summary_msg)

    # Send the list of successfully uploaded files to Telegram
    send_telegram_file_list(uploaded_files)

if __name__ == "__main__":
    try:
        main()
    finally:
        run_stats["end_time"] = datetime.now()
        if run_stats["files_compressed"]>0 or run_stats["files_skipped_larger"]>0 or run_stats["files_upload_failed"]>0 or run_stats["retry_recovered"]>0 or run_stats["retry_still_failed"]>0:
            send_report_email(run_stats)
        release_lock()
