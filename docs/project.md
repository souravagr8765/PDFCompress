# Project Overview
This project is an automation script (`pdf_sync.py`) designed to monitor a specific folder for new PDF files, compress them using Ghostscript to reduce file size, upload the compressed files to Google Drive via `rclone`, and replace the original local files with the compressed versions. Detailed execution logs are recorded locally to ensure full observability.

# System Architecture
- `pdf_sync.py`: The main orchestrating Python script that handles directory scanning, compression tracking, and cloud uploading.
- **Ghostscript (`gs`)**: External dependency used under the hood to perform the actual PDF optimizations and reductions.
- **rclone**: External dependency utilized for synchronizing and uploading final output files to a remote destination (Google Drive).

# Database Schema
There is no traditional relational database. 
- **Manifest File**: The script uses a local flat-file database (a `.processed_manifest.txt` text file) to track paths of successfully processed PDF files. This prevents infinite loops or redundant recompression tasks.

# Environment Configuration
Configuration is currently declared statically within the script body, simplifying the single-file portability.
Constants located within `pdf_sync.py`:
- `WATCH_FOLDER`: The target directory monitored for new PDFs.
- `GDRIVE_REMOTE`: Configuration name of the rclone remote (e.g., `"gdrive"`).
- `GDRIVE_FOLDER`: Expected upstream folder in the Google Drive remote.
- `MANIFEST_FILE`: The full path for the processed log txt.
- `LOG_FILE`: Points to `compressor.log`, handling general runtime logging.
- `IMAGE_DPI` & `JPEG_QUALITY`: Controls for Ghostscript tuning.

# Configuration Management
Currently, there is no separated `config/` directory. All environment variables, defaults, and configuration constants are managed directly at the head of `pdf_sync.py`. Environment variables such as `LOKI_URL`, `LOKI_USERNAME`, `LOKI_PASSWORD`, and `JOB_NAME` can be stored in a `.env` file in the same directory as `pdf_sync.py`, which is loaded automatically at runtime.

# Code Workflow
1. **Initialize script context**: Initialize `logging` (to `compressor.log`), check if `WATCH_FOLDER` exists, and verify `rclone` and `gs` binaries exist in the system's execution PATH.
2. **Read Manifest**: Retrieve cache of processed files by reading `.processed_manifest.txt`.
3. **Scan Files**: Walk `WATCH_FOLDER` tree recursively to identify unprocessed `.pdf` files.
4. **Iterative Processing**: For every target file:
   - Check if path exists in the loaded manifest cache. If so, skip.
   - Run Ghostscript on the input file, outputting to a temporary suffix.
   - Validate target output dimensions vs original size.
   - If output is smaller, swap files. Otherwise, retain original and drop temp file.
   - Run a subprocess calling `rclone copy` to ship the final payload to Google Drive.
   - Update and flush the manifest with the newly finalized file path to prevent future repetitions.
   - Append detailed outcomes utilizing Python's robust `logging` mechanisms.
5. **Complete**: After the final iteration sequence, print a total summary execution string.
