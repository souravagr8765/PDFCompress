import os
import sys
import time
import requests
import logging

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger("loki_pusher")

JOB_NAME = os.getenv("JOB_NAME")

def push_to_loki(lines, loki_url, job_name, auth=None):
    if not lines:
        return
        
    try:
        # Loki expects timestamp in nanoseconds as string
        timestamp_ns = str(int(time.time() * 1e9))
        
        values = [[timestamp_ns, line.strip()] for line in lines]
        
        payload = {
            "streams": [
                {
                    "stream": {
                        "job": job_name
                    },
                    "values": values
                }
            ]
        }
        
        headers = {'Content-type': 'application/json'}
        url = f"{loki_url.rstrip('/')}/loki/api/v1/push"
        
        # HTTP Basic Auth support
        kw = {"json": payload, "headers": headers, "timeout": 10}
        if auth:
            kw["auth"] = auth
            
        response = requests.post(url, **kw)
        if response.status_code >= 400:
            logger.error(f"Failed to push to Loki: {response.status_code} - {response.text}")
            
    except Exception as e:
        logger.error(f"Exception pushing to Loki: {e}")

def tail_and_push(log_file, loki_url, start_pos=0, auth=None):
    if not os.path.exists(log_file):
        logger.warning(f"Log file {log_file} does not exist yet. Waiting...")
        while not os.path.exists(log_file):
            time.sleep(1)
            
    with open(log_file, 'r') as f:
        f.seek(start_pos)
        
        buffer = []
        last_push = time.time()
        
        while True:
            line = f.readline()
            if line:
                buffer.append(line)
            else:
                time.sleep(0.5)
                
            current_time = time.time()
            if current_time - last_push >= 5.0:
                if buffer:
                    push_to_loki(buffer, loki_url, JOB_NAME, auth=auth)
                    buffer = []
                last_push = current_time

if __name__ == "__main__":
    if len(sys.argv) < 3:
        print("Usage: python loki_pusher.py <log_file> <loki_url> [start_pos]")
        sys.exit(1)
        
    log_file = sys.argv[1]
    loki_url = sys.argv[2]
    try:
        start_pos = int(sys.argv[3]) if len(sys.argv) > 3 else 0
    except ValueError:
        start_pos = 0
    
    logger.info(f"Starting loki_pusher for {log_file} pushing to {loki_url} from position {start_pos}")
    
    auth = None
    if len(sys.argv) >= 6:
        auth = (sys.argv[4], sys.argv[5])
        logger.info("Using HTTP Basic Auth for Loki.")
    
    try:
        tail_and_push(log_file, loki_url, start_pos, auth=auth)
    except KeyboardInterrupt:
        logger.info("Loki pusher stopped.")
    except Exception as e:
        logger.error(f"Loki pusher fatal error: {e}")
