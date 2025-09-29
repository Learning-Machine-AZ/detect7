import logging
import json
import traceback
import time
import signal
import socket
from datetime import datetime
import pandas as pd
import multiprocessing as mp
from configs import EngineConfig, RabbitMQ_Config
from detector import AdvancedDDoSDetector
from syslog_server import RabbitMQ_Client
from redis_ts_queue import TimeseriesQueue
from redis_ip_queue import IPQueue
from syslog_server import IP2Hostname_V2


def flatten_dataframe_to_dict(df, row_index=0):
    """
    Convert a single row of a pandas DataFrame into a flattened dictionary.
    
    Args:
        df (pandas.DataFrame): Input DataFrame
        row_index (int): Index of the row to convert (default: 0)
    
    Returns:
        dict: Flattened dictionary with original column names
    """
    # Get the first row as a dictionary
    row_dict = df.iloc[row_index].to_dict()
    
    # Initialize the flattened dictionary
    flat_dict = {}
    
    # Flatten any nested dictionaries
    for key, value in row_dict.items():
        if isinstance(value, dict):
            flat_dict.update(value)
        else:
            flat_dict[key] = value
            
    return flat_dict


def build_ip_data_preview(df, target_max_bytes=24000):
    """
    Build a compact preview of ip_data that stays below target_max_bytes when JSON-serialized.
    Keeps essential numeric fields and truncated previews of long strings.
    Returns (preview_dict, total_rows, preview_rows).
    """
    try:
        cols_keep = [
            'timestamp', 'status', 'bytes_sent', 'request_time', 'request_length'
        ]
        # Start with numeric/time columns
        compact = df[cols_keep].copy()
        # Add truncated previews for potentially large string fields
        compact['request_preview'] = df['request'].astype(str).str.slice(0, 200)
        compact['ua_preview'] = df['user_agent'].astype(str).str.slice(0, 200)

        total_rows = len(compact)
        preview = compact.fillna(0).to_dict()

        # Ensure size budget
        serialized = json.dumps(preview, ensure_ascii=False)
        size_bytes = len(serialized.encode('utf-8'))
        if size_bytes <= target_max_bytes:
            preview_rows = total_rows
            return preview, total_rows, preview_rows

        # Downsample rows if needed to fit the target budget
        if total_rows > 0:
            # Estimate a stride to reduce size below target
            reduction_factor = max(2, int(size_bytes / max(1, target_max_bytes)) + 1)
            compact2 = compact.iloc[::reduction_factor]
            preview2 = compact2.to_dict()
            preview_rows = len(compact2)
            return preview2, total_rows, preview_rows

    except Exception:
        pass

    # Fallback: empty preview
    return {}, len(df), 0

def ip_lookup (ip: str):
    try:
        hostname = socket.gethostbyaddr(ip)[0]
        return hostname
    except socket.herror as e:
        logging.debug(f"Error resolving hostname: {e}")
    return None
    
    
def init_worker():
    """
    Initialize worker process with its own model instance.
    This runs once per worker process.
    """
    # Create model instance in each worker process
    global detector, redis_queue, cipl, ip2hostname
    
    detector = AdvancedDDoSDetector(EngineConfig.DETECTOR_MODEL_PATH)
    redis_queue = TimeseriesQueue()
    cipl = IPQueue()
    ip2hostname = IP2Hostname_V2()


def process_record(ip):
    """
    Process a single record. Override this method with your processing logic.
    """
    global detector, redis_queue, cipl, ip_cache, ip2hostname
    
    last_feature = None

    try:
        ip_records = redis_queue.get_last_seconds(seconds=EngineConfig.DETECTOR_WINDOW_SIZE, ip=ip)
        
        if len(ip_records) < EngineConfig.DETECTOR_MIN_RECORDS:
            logging.debug(f"DETECTOR_MIN_RECORDS not met: {ip}")
            return None
        
        ip_data = []
        for r in ip_records:
            # TIMESTAMP
            timestamp = datetime.fromtimestamp(r['ts'], tz=EngineConfig.TZ_OFFSET)
            
            # COLLECT
            ip_data.append({
                'ip': ip,
                'timestamp': timestamp,
                'domain': str(r['data']['domain']),
                'request': str(r['data']['request']),
                'request_length': int(r['data']['request_length']),
                'status': int(r['data']['response_status']),
                'bytes_sent': int(r['data']['body_bytes_sent']),
                'request_time': float(r['data']['request_time']),
                'user_agent': str(r['data']['http_user_agent']),
            })
            
        logging.debug(f"Processing request from IP {ip} with ip_data: {len(ip_data)}")
        
        start = time.time()
        is_ddos, (normal_probability, ddos_probability), last_feature = detector.predict_ip(ip_data)
        processing_time = time.time() - start

        last_feature_flat = flatten_dataframe_to_dict(last_feature.fillna(0))
        
        log = {
            'ip': ip,
            'req_count': last_feature_flat['request_count'],
            'rps': round(last_feature_flat['requests_per_second'], 2),
            'req_rate': round(last_feature_flat['request_rate'], 2),
            'err_rate': round(last_feature_flat['error_rate'], 2),
            'problem': round(ddos_probability * 100),
            'normal': round(normal_probability * 100),
            'speed': round(processing_time, 3),
        }
        
        log_string = ', '.join(f'{k}: {v}' for k, v in log.items())
        
        test_ips = [
            '100.100.100.100' # send-logs-test.sh, WASN'T IN ANY DATASET
        ]
        
        if ip in test_ips:
            logging.info('#'*50)
            logging.info(f"{ip} -> {len(ip_records)} records")
            logging.info(datetime.fromtimestamp(ip_records[0]['ts'], tz=EngineConfig.TZ_OFFSET).isoformat())
            logging.info(datetime.fromtimestamp(ip_records[-1]['ts'], tz=EngineConfig.TZ_OFFSET).isoformat())
            logging.info(log_string)
            logging.info('#'*50)
            # logging.info(f"last_feature: {last_feature_flat}")
        
        # if is_ddos and (last_feature_flat['requests_per_second'] >= EngineConfig.DETECTOR_MIN_RPS):
        if is_ddos and (last_feature_flat['request_rate'] >= EngineConfig.DETECTOR_MIN_RPS):
            logging.warning(log_string)
            
            # ADD IP TO STOP LIST
            if not cipl.contains(ip):
                cipl.add(ip)
                logging.info(f'CIPL: add {ip}')
                
            # DELETE FROM QUEUE TO PREVENT RE-PROCESSING
            ts_deleted = redis_queue.delete_by_ip(ip)
            
            if ts_deleted > 0:
                logging.info(f"TS QUEUE: delete {ip} -> {ts_deleted} records")
        
            # RABBITMQ #
            
            # LESS DATA
            df = pd.DataFrame.from_records(data=ip_data)
            df = df.drop(columns=["ip"]) if 'ip' in df.columns else df
            df = df.fillna(0)

            # Build compact ip_data preview to avoid oversized OpenSearch terms
            ip_data_preview, ip_data_total_rows, ip_data_preview_rows = build_ip_data_preview(df)
            
            ip2location = ip_records[0]['data']['ip2location']
            # ptr = ip_lookup(ip)
            ptr = ip2hostname.detect(ip)
            
            message_data = {
                'ptr': ptr,
                # "ptr": ip_records[0]['data']['ptr'],
                # "timestamp": last_feature['timestamp'].iloc[0].timestamp(), # v 1.1
                "timestamp": last_feature_flat['timestamp'], # v 1.0
                "domain": ip_data[0]['domain'],
                "message": log_string,
                "priority": EngineConfig.DETECTOR_WORKER_GRAYLOG_PRIORITY,
                "tag": EngineConfig.DETECTOR_WORKER_GRAYLOG_TAG,
                'ip_data': ip_data_preview,
                'ip_data_rows_total': int(ip_data_total_rows),
                'ip_data_rows_preview': int(ip_data_preview_rows),
                'last_feature': last_feature_flat,
                'ip2location': ip2location,
                'country': ip2location['country'] if ip2location else 'NA',
                'city': ip2location['city'] if ip2location else 'NA',
            }
            
            message_data.update(log)
            
            return message_data
        else:
            logging.debug(log_string)

    except Exception as e:
        logging.error(f"ERROR: {str(e)}")
        logging.error(f"IP DATA: {ip_data}")
        logging.error(f"LAST FEATURE: {last_feature_flat}")
        logging.error(traceback.format_exc())
    
    return None


class QueueConsumer:
    def __init__(self):
        self.num_workers = EngineConfig.DETECTOR_WORKER_NUM_WORKERS
        self.batch_size = EngineConfig.DETECTOR_WORKER_BATCH_SIZE
        self.window_seconds = EngineConfig.DETECTOR_WORKER_WINDOW_SECONDS
        self.sleep_seconds = EngineConfig.DETECTOR_WORKER_SLEEP_SECONDS
        self.redis_queue = TimeseriesQueue()
        self.should_stop = False
        self.cipl = IPQueue()
        
        rabbitmq_config = RabbitMQ_Config(
            url=EngineConfig.RABBITMQ_URL,
            exchange=EngineConfig.RABBITMQ_CHALLENGE_EXCHANGE,
            queue=EngineConfig.RABBITMQ_CHALLENGE_QUEUE,
            routing_key=EngineConfig.RABBITMQ_CHALLENGE_ROUTING_KEY
        )
        
        self.rabbitmq_client = RabbitMQ_Client(rabbitmq_config)
        
        # Set up process pool
        self.pool = mp.Pool(
            processes=self.num_workers,
            initializer=init_worker
        )
        
        # Set up signal handling for graceful shutdown
        signal.signal(signal.SIGINT, self.handle_shutdown)
        signal.signal(signal.SIGTERM, self.handle_shutdown)

    def handle_shutdown(self, signum, frame):
        """Handle shutdown signals gracefully"""
        logging.info("Shutdown signal received. Cleaning up...")
        self.should_stop = True

    def process_batch(self, ips):
        """
        Process a batch of records in parallel using the process pool
        """
        try:
            # Process records in parallel
            results = self.pool.map(process_record, ips)
            
            # Split by categories
            normal = [res for ip, res in zip(ips, results) if not res]
            problems = [res for ip, res in zip(ips, results) if res]
            
            return normal, problems
        except Exception as e:
            logging.error(f"Error in batch processing: {str(e)}")
            return [], ips

    def run(self):
        logging.info(f"Starting consumer with {self.num_workers} workers ...")
        logging.info(f"Fetching records from last {self.window_seconds} seconds every {self.sleep_seconds} seconds")
        
        while not self.should_stop:
            try:
                # Fetch records from the last window_seconds
                records = self.redis_queue.get_last_seconds(self.window_seconds)
                
                if not records:
                    # logging.info("No new records...")
                    continue
                
                # logging.info(f"Fetched {len(records)} records")
                
                df = pd.DataFrame.from_dict(records)
                ips = df['ip'].unique()

                batch_count = len(ips) // self.batch_size
                logging.info(f"Processing {len(ips)} records in {batch_count} batches of {self.batch_size} items")
                
                # Process records in batches
                for i in range(0, len(ips), self.batch_size):
                    batch = ips[i:i + self.batch_size]
                    logging.debug(f"Processing batch #{i} of {len(batch)} records")
                    
                    # Process the batch
                    normal, problems = self.process_batch(batch)
                    
                    if len(problems) > 0:
                        # Delete successfully processed records
                        # deleted = self.redis_queue.delete_fetched_records(successful)
                        logging.debug(f"Detected {len(problems)} problems")
                        
                        for problem in problems:
                            logging.debug(problem)
                            self.rabbitmq_client.send_message(problem)
                    
                    # if failed:
                    #     logging.info(f"Failed to process {len(failed)} records")
                        
            except KeyboardInterrupt:
                logging.error("Interrupt received. Shutting down...")
                break
            except Exception as e:
                logging.error(f"Error in consumer loop: {str(e)}")
            finally:
                # CIPL CLEAN-UP
                cipl_purged = self.cipl.purge_old()
                if cipl_purged > 0:
                    logging.info(f"CIPL: purged {cipl_purged}")
                    
                # TS QUEUE CLEAN-UP
                ts_queue_purged = self.redis_queue.purge_old()
                if ts_queue_purged > 0:
                    logging.info(f"TS QUEUE: purged {ts_queue_purged}")
                    
                time.sleep(self.sleep_seconds)

        # Cleanup
        logging.info("Shutting down worker pool...")
        self.pool.close()
        self.pool.join()
        logging.info("Consumer stopped")
        

def main():
    consumer = QueueConsumer()
    consumer.run()

if __name__ == "__main__":
    main()