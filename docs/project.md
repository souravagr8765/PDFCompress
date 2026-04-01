# Project Overview
This project is an automation script (`pdf_sync.py`) designed to monitor a specific folder for new PDF files, compress them using Ghostscript to reduce file size, upload the compressed files to Google Drive via `rclone`, and replace the original local files with the compressed versions. Detailed execution logs are recorded locally to ensure full observability.

# System Architecture
- `pdf_sync.py`: The main orchestrating Python script that handles directory scanning, compression tracking, and cloud uploading.
- **Ghostscript (`gs`)**: External dependency used under the hood to perform the actual PDF optimizations and reductions.
- **rclone**: External dependency utilized for synchronizing and uploading final output files to a remote destination (Google Drive).
- `loki_logger.py`: A background daemon script that continuously pushes runtime logs from `compressor.log` to a centralized Loki server.

# Database Schema
The script uses a local SQLite database (`local_cache.db`) to track the state of processed files, which is also synchronized with a remote Nhost PostgreSQL database for state consistency.
- **Table `processed_files`**: Stores `file_path` (PRIMARY KEY, uses cross-platform relative paths for seamless Termux/Windows sync while falling back to absolute where historically used), `original_size`, `compressed_size`, `status` (`compressed`, `skipped_larger`, or `upload_failed`), and `processed_at` timestamp. This prevents infinite loops, redundant recompression tasks, gracefully handles interrupted or failed uploads, and ensures files modified after compression are correctly re-evaluated. Both local SQLite and Nhost PostgreSQL maintain this same schema.

# Environment Configuration
Configuration is currently declared statically within the script body, simplifying the single-file portability.
Constants located within `pdf_sync.py`:
- `LOCK_FILE`: Process lock file to prevent concurrent execution.
- `NHOST_CONNECTION_STRING`: The connection string for the remote PostgreSQL database.
- `SMTP_HOST`: The SMTP server host for email reporting.
- `SMTP_PORT`: The SMTP server port (default: 587).
- `SMTP_USER`: The SMTP login username.
- `SMTP_PASSWORD`: The SMTP login password.
- `REPORT_RECIPIENT`: Destination email address for the execution report.
- `WATCH_FOLDER`: The target directory monitored for new PDFs.
- `GDRIVE_REMOTE`: Configuration name of the rclone remote (e.g., `"gdrive"`).
- `GDRIVE_FOLDER`: Expected upstream folder in the Google Drive remote.
- `LOCAL_DB`: The full path for the local SQLite database cache.
- `LOG_FILE`: Points to `compressor.log`, handling general runtime logging.
- `IMAGE_DPI` & `JPEG_QUALITY`: Controls for Ghostscript tuning.

# Configuration Management
Currently, there is no separated `config/` directory. All environment variables, defaults, and configuration constants are managed directly at the head of `pdf_sync.py`. Environment variables such as `LOKI_URL`, `LOKI_USERNAME`, `LOKI_PASSWORD`, `JOB_NAME`, `NHOST_CONNECTION_STRING`, `SMTP_HOST`, `SMTP_PORT`, `SMTP_USER`, `SMTP_PASSWORD`, `REPORT_RECIPIENT`, `TELEGRAM_BOT_API`, and `CHAT_ID` can be stored in a `.env` file in the same directory as `pdf_sync.py`, which is loaded automatically at runtime.

# Code Workflow
1. **Lock Verification**: The script immediately acquires an exclusive `.pdf_sync.lock` file to prevent concurrent executions. Stale locks are automatically cleared. Execution halts immediately if another active instance is detected.
2. **Initialize script context**: Initialize `logging` (to `compressor.log`), check if `WATCH_FOLDER` exists, and verify `rclone` and `gs` binaries exist in the system's execution PATH. Read `.env` for Loki configuration; if found, spawn the `loki_logger.py` subprocess in the background to handle remote log aggregation.
3. **Database Initialization**: Initialize the local SQLite database `local_cache.db` and the `processed_files` table if they do not exist. Following this, the script runs a reconciliation routine (`reconcile_databases()`) to bi-directionally sync records between the local SQLite database and the Nhost PostgreSQL database.
4. **Cleanup Orphaned Temp Files**: Run `cleanup_temp_files()` to recursively scan `WATCH_FOLDER` and remove any stale `_temp.pdf` files left over from interrupted runs.
5. **Scan Files**: Walk `WATCH_FOLDER` tree recursively to identify `.pdf` files.
6. **Telegram File List Notification**: Before compression begins, the complete list of scanned PDF files (with filenames and sizes) is sent to all Telegram chat IDs configured in `CHAT_ID`, using the bot token from `TELEGRAM_BOT_API`. Messages exceeding Telegram's 4096-character limit are automatically chunked.
7. **Iterative Processing**: For every target file:
   - Query the `processed_files` SQLite table. If the file exists and its current size matches the `compressed_size` in the database, skip it (unless status is `upload_failed`).
   - If status is `upload_failed`, skip Ghostscript processing entirely and immediately retry the `rclone copy` upload directly.
   - Run Ghostscript on the input file, outputting to a temporary suffix.
   - Validate target output dimensions vs original size.
   - If output is smaller, swap files. Otherwise, retain original and drop temp file.
   - Immediately update both the local SQLite database and the remote Nhost PostgreSQL `processed_files` tables using `UPSERT` semantics.
   - Run a subprocess calling `rclone copy` to ship the final payload to Google Drive.
   - If the upload fails, do not throw an exception. Instead, flag the record in both databases with `upload_failed` and continue to the next file.
   - Append detailed outcomes utilizing Python's robust `logging` mechanisms.
8. **Retry Failed Uploads Pass**: After completing the main file iteration, query the database for all remaining `upload_failed` files and attempt a dedicated retry pass. Statistics are reported on success or persistent failure.
9. **Complete**: Print a total summary execution string containing processed counts, skipped counts, error events, and retry recovery rates. The global lock is released via a top-level try/finally block before script exit.

