# pkg install ghostscript
# rclone must be installed and configured separately

import sys
import os
import shutil
import subprocess
import traceback
from datetime import datetime

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
    result = subprocess.run(command, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    if result.returncode != 0:
        raise RuntimeError(f"Ghostscript failed: {result.stderr.decode()}")

# =============================================================================
# CONFIGURATION
# =============================================================================
WATCH_FOLDER = "/sdcard/YourFolderName"
GDRIVE_REMOTE = "gdrive"
GDRIVE_FOLDER = "YourFolderName"
MANIFEST_FILE = "/sdcard/YourFolderName/.processed_manifest.txt"
LOG_FILE = "/sdcard/YourFolderName/.pdf_sync.log"
TEMP_SUFFIX = "_compressed_tmp.pdf"
IMAGE_DPI = 150
JPEG_QUALITY = 75
# =============================================================================

def format_size(size_val):
    return f"{size_val / (1024 * 1024):.1f}MB"

def write_log(filename, status, message):
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    log_line = f"[{timestamp}] {status} | {filename} | {message}"
    
    try:
        with open(LOG_FILE, "a", encoding="utf-8") as f:
            f.write(log_line + "\n")
    except Exception as e:
        print(f"Could not write to log file: {e}", file=sys.stderr)

def main():
    if not os.path.exists(WATCH_FOLDER):
        print(f"ERROR: WATCH_FOLDER '{WATCH_FOLDER}' does not exist.")
        sys.exit(1)

    if shutil.which("rclone") is None:
        print("ERROR: rclone not found in PATH.")
        sys.exit(1)

    if not shutil.which("gs"):
        print("ERROR: Ghostscript (gs) not found. Run: pkg install ghostscript")
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
        for root, dirs, files in os.walk(WATCH_FOLDER):
            for file in files:
                if file.lower().endswith(".pdf") and TEMP_SUFFIX not in file:
                    full_path = os.path.abspath(os.path.join(root, file))
                    scan_files.append(full_path)
    except Exception as e:
        print(f"ERROR: Could not read WATCH_FOLDER: {e}")
        sys.exit(1)

    for full_path in scan_files:
        file_name = os.path.basename(full_path)
        
        if full_path in manifest:
            skipped_count += 1
            continue
            
        temp_path = full_path[:-4] + TEMP_SUFFIX
        
        try:
            # a. Compress it using Ghostscript
            orig_size = os.path.getsize(full_path)
            compress_pdf(full_path, temp_path)
            
            # b. Validate the compressed file
            if not os.path.exists(temp_path) or os.path.getsize(temp_path) == 0:
                raise Exception("Compressed temp file is missing or 0 bytes")
                
            new_size = os.path.getsize(temp_path)
            upload_path = full_path
            
            # c. Replace the original
            if new_size < orig_size:
                os.remove(full_path)
                os.rename(temp_path, full_path)
                final_size = new_size
            else:
                # Compressed file larger or equal; keep original
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
            proc = subprocess.run(cmd, capture_output=True, text=True)
            if proc.returncode != 0:
                raise Exception(f"rclone upload failed: {proc.stderr.strip() or 'Unknown error'}")
                
            # e. Update the manifest
            with open(MANIFEST_FILE, "a", encoding="utf-8") as f:
                f.write(full_path + "\n")
            manifest.add(full_path)
            
            # f. Write a log entry
            log_msg = f"{format_size(orig_size)} \u2192 {format_size(final_size)} ({reduction_pct}% reduction) | uploaded to {GDRIVE_REMOTE}:{dest_folder}"
            write_log(rel_path, "SUCCESS", log_msg)
            
            processed_count += 1
            
        except Exception as e:
            # error handling block
            error_count += 1
            if os.path.exists(temp_path):
                try:
                    os.remove(temp_path)
                except:
                    pass
            write_log(file_name, "ERROR", str(e))
            
    print(f"\nPDF Sync complete. Processed: {processed_count} | Skipped (already done): {skipped_count} | Errors: {error_count}")

if __name__ == "__main__":
    main()
