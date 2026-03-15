import os
import sys
import time
import json
import socket
import urllib.request
import urllib.error
import base64
import re
from datetime import datetime

LOG_TIMESTAMP_REGEX = re.compile(r'^(\d{4}-\d{2}-\d{2}\s\d{2}:\d{2}:\d{2}(?:,\d+)?)\s*\|')

def parse_log_timestamp(line, default_ts_ns):
    match = LOG_TIMESTAMP_REGEX.match(line)
    if match:
        timestamp_str = match.group(1)
        try:
            if ',' in timestamp_str:
                dt = datetime.strptime(timestamp_str, '%Y-%m-%d %H:%M:%S,%f')
            else:
                dt = datetime.strptime(timestamp_str, '%Y-%m-%d %H:%M:%S')
            return str(int(dt.timestamp() * 1e9))
        except ValueError:
            pass
    return default_ts_ns

def push_to_loki(log_lines, url, username, password, job_name):
    """Pushes a list of log lines to Loki."""
    if not log_lines:
        return
        
    try:
        # Prepare the payload for Loki
        # Loki expects timestamp in nanoseconds as strings
        values = []
        last_ts = str(int(time.time() * 1e9))
        
        for line in log_lines:
            ts = parse_log_timestamp(line, last_ts)
            values.append([ts, line.strip()])
            last_ts = ts
        
        payload = {
            "streams": [
                {
                    "stream": {
                        "job": job_name,
                        "host": socket.gethostname()
                    },
                    "values": values
                }
            ]
        }
        
        data = json.dumps(payload).encode('utf-8')
        
        req = urllib.request.Request(url + "/loki/api/v1/push", data=data)
        req.add_header('Content-Type', 'application/json')
        
        if username and password:
            auth_str = f"{username}:{password}"
            b64_auth_str = base64.b64encode(auth_str.encode('ascii')).decode('ascii')
            req.add_header('Authorization', f'Basic {b64_auth_str}')
            
        with urllib.request.urlopen(req, timeout=10) as response:
            if response.getcode() != 204:
                print(f"[{time.strftime('%Y-%m-%d %H:%M:%S')}] loki_logger: Failed to push to Loki. Status: {response.getcode()}", file=sys.stderr)
            
    except urllib.error.URLError as e:
         print(f"[{time.strftime('%Y-%m-%d %H:%M:%S')}] loki_logger: Network error sending to Loki: {e}", file=sys.stderr)
    except Exception as e:
         print(f"[{time.strftime('%Y-%m-%d %H:%M:%S')}] loki_logger: Error sending to Loki: {e}", file=sys.stderr)

def main():
    if len(sys.argv) < 2:
        print("Usage: python loki_logger.py <log_file_path> [<start_pos>]", file=sys.stderr)
        sys.exit(1)
        
    log_file = sys.argv[1]
    start_pos = int(sys.argv[2]) if len(sys.argv) > 2 else -1
    
    url = os.environ.get("LOKI_URL")
    username = os.environ.get("LOKI_USERNAME")
    password = os.environ.get("LOKI_PASSWORD")
    job_name = os.environ.get("JOB_NAME", "pdf_compressor_agent")
    
    if not url:
        print("loki_logger: LOKI_URL environment variable is required.", file=sys.stderr)
        sys.exit(1)
        
    print(f"loki_logger: Starting to watch {log_file} and pushing to {url} as job '{job_name}'")
    
    # Wait for the log file to exist before starting to tail it
    while not os.path.exists(log_file):
        time.sleep(1)
        
    log_buffer = []
    last_push_time = time.time()
    
    try:
        with open(log_file, 'r', encoding='utf-8') as f:
            if start_pos >= 0:
                f.seek(start_pos)
            else:
                f.seek(0, os.SEEK_END)
                
            while True:
                line = f.readline()
                current_time = time.time()
                
                if line:
                    if "LOKI_LOGGER_TERMINATE" in line:
                        break  # Stop processing upon receiving termination signal
                    log_buffer.append(line)
                    
                # Push logs every 5 seconds or when buffer gets large
                if len(log_buffer) >= 100 or (log_buffer and current_time - last_push_time >= 5.0):
                    push_to_loki(log_buffer, url, username, password, job_name)
                    log_buffer = []
                    last_push_time = current_time
                    
                if not line:
                    time.sleep(0.5)
                    
    except KeyboardInterrupt:
        print("loki_logger: Terminating due to KeyboardInterrupt...", file=sys.stderr)
    except Exception as e:
        print(f"loki_logger: Error reading file: {e}", file=sys.stderr)
    finally:
        if log_buffer:
             push_to_loki(log_buffer, url, username, password, job_name)
        print("loki_logger: Exited nicely.", file=sys.stderr)

if __name__ == "__main__":
    main()
