# pkg install ghostscript
# rclone must be installed and configured separately

import sys
import os
import shutil
import subprocess
import traceback
import logging
from datetime import datetime

# =============================================================================
# CONFIGURATION
# =============================================================================
WATCH_FOLDER = "/sdcard/Books/Foundation/"
GDRIVE_REMOTE = "gdrivestudent.sourav.agarwal"
GDRIVE_FOLDER = "Foundation2"
MANIFEST_FILE = "./processed_manifest.txt"
LOG_FILE = "./logs.log"
TEMP_SUFFIX = "_compressed_tmp.pdf"
IMAGE_DPI = 150
JPEG_QUALITY = 75
# =============================================================================
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
# Setup logging
logging.basicConfig(
    level=logging.DEBUG,
    format='%(asctime)s | %(levelname)s | %(message)s',
    handlers=[
        logging.FileHandler(LOG_FILE, encoding='utf-8'),
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger(__name__)

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
    logger.debug(f"Executing Ghostscript command: {' '.join(command)}")
    result = subprocess.run(command, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    if result.returncode != 0:
        error_msg = result.stderr.decode()
        logger.error(f"Ghostscript failed for {input_path}. Error: {error_msg}")
        raise RuntimeError(f"Ghostscript failed: {error_msg}")
    logger.debug(f"Ghostscript compression completed for {input_path}")

def format_size(size_val):
    return f"{size_val / (1024 * 1024):.1f}MB"

def main():
    env_path = os.path.join(BASE_DIR, '.env')
    if os.path.exists(env_path):
        with open(env_path, 'r', encoding='utf-8') as f:
            for line in f:
                line = line.strip()
                if line and not line.startswith('#') and '=' in line:
                    key, val = line.split('=', 1)
                    os.environ[key.strip()] = val.strip(' "\'')

    loki_url = os.getenv('LOKI_URL')
    loki_process = None
    if loki_url:
        loki_user = os.getenv('LOKI_USERNAME', '')
        loki_pass = os.getenv('LOKI_PASSWORD', '')
        
        log_file = os.path.abspath(LOG_FILE)
        current_size = os.path.getsize(log_file) if os.path.exists(log_file) else 0
        loki_cmd = [sys.executable, os.path.join(BASE_DIR, 'loki_pusher.py'), log_file, loki_url, str(current_size)]
        if loki_user and loki_pass:
            loki_cmd.extend([loki_user, loki_pass])
            
        loki_process = subprocess.Popen(loki_cmd)
        
        def stop_loki():
            if loki_process and loki_process.poll() is None:
                loki_process.terminate()
                try:
                    loki_process.wait(timeout=5)
                except subprocess.TimeoutExpired:
                    loki_process.kill()
        atexit.register(stop_loki)

    logger.info("Starting PDF Sync Script")
    if not os.path.exists(WATCH_FOLDER):
        logger.error(f"WATCH_FOLDER '{WATCH_FOLDER}' does not exist.")
        sys.exit(1)

    if shutil.which("rclone") is None:
        logger.error("rclone not found in PATH.")
        sys.exit(1)

    if not shutil.which("gs"):
        logger.error("Ghostscript (gs) not found. Run: pkg install ghostscript")
        sys.exit(1)

    manifest = set()
    if os.path.exists(MANIFEST_FILE):
        with open(MANIFEST_FILE, "r", encoding="utf-8") as f:
            for line in f:
                path = line.strip()
                if path:
                    manifest.add(path)

    processed_count = 0
    skipped_count = 0
    error_count = 0

    scan_files = []
    try:
        logger.debug(f"Scanning WATCH_FOLDER '{WATCH_FOLDER}' for PDF files...")
        for root, dirs, files in os.walk(WATCH_FOLDER):
            for file in files:
                if file.lower().endswith(".pdf") and TEMP_SUFFIX not in file:
                    full_path = os.path.abspath(os.path.join(root, file))
                    scan_files.append(full_path)
        logger.info(f"Found {len(scan_files)} PDF files to process.")
    except Exception as e:
        logger.error(f"Could not read WATCH_FOLDER: {e}")
        sys.exit(1)

    for full_path in scan_files:
        file_name = os.path.basename(full_path)
        
        if full_path in manifest:
            logger.debug(f"Skipping {file_name} - already inside manifest.")
            skipped_count += 1
            continue
            
        logger.info(f"Processing {file_name}...")
        temp_path = full_path[:-4] + TEMP_SUFFIX
        
        try:
            # a. Compress it using Ghostscript
            orig_size = os.path.getsize(full_path)
            logger.debug(f"Original file size {file_name}: {format_size(orig_size)}")
            compress_pdf(full_path, temp_path)
            
            # b. Validate the compressed file
            if not os.path.exists(temp_path) or os.path.getsize(temp_path) == 0:
                raise Exception("Compressed temp file is missing or 0 bytes")
                
            new_size = os.path.getsize(temp_path)
            upload_path = full_path
            
            # c. Replace the original
            if new_size < orig_size:
                logger.debug(f"Replacing original file. Compressed size: {format_size(new_size)}")
                os.remove(full_path)
                os.rename(temp_path, full_path)
                final_size = new_size
            else:
                # Compressed file larger or equal; keep original
                logger.debug(f"Compression did not reduce size. Keeping original: {format_size(orig_size)}")
                os.remove(temp_path)
                final_size = orig_size
                
            reduction_pct = 0
            if orig_size > 0:
                reduction_pct = int(((orig_size - final_size) / orig_size) * 100)
                
            # d. Upload to Google Drive using rclone
            rel_path = os.path.relpath(upload_path, WATCH_FOLDER)
            rel_dir = os.path.dirname(rel_path).replace("\\", "/")
            dest_folder = f"{GDRIVE_FOLDER}/{rel_dir}" if rel_dir and rel_dir != "." else GDRIVE_FOLDER
            
            cmd = [
                "rclone", "copy", upload_path, 
                f"{GDRIVE_REMOTE}:{dest_folder}", "--no-traverse"
            ]
            logger.debug(f"Executing rclone upload: {' '.join(cmd)}")
            proc = subprocess.run(cmd, capture_output=True, text=True)
            if proc.returncode != 0:
                error_msg = proc.stderr.strip() or 'Unknown error'
                logger.error(f"rclone upload failed for {file_name}. Error: {error_msg}")
                raise Exception(f"rclone upload failed: {error_msg}")
            
            logger.debug(f"Upload successful for {file_name}")
            
            # e. Update the manifest
            with open(MANIFEST_FILE, "a", encoding="utf-8") as f:
                f.write(full_path + "\n")
            manifest.add(full_path)
            logger.debug(f"{file_name} added to manifest.")
            
            # f. Write a log entry
            log_msg = f"{file_name} | SUCCESS | {format_size(orig_size)} \u2192 {format_size(final_size)} ({reduction_pct}% reduction) | uploaded to {GDRIVE_REMOTE}:{dest_folder}"
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
            logger.error(f"{file_name} | ERROR | {str(e)}\n{traceback.format_exc()}")
            
    summary_msg = f"PDF Sync complete. Processed: {processed_count} | Skipped: {skipped_count} | Errors: {error_count}"
    logger.info(summary_msg)

if __name__ == "__main__":
    main()
