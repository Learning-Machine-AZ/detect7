import redis
import time
from datetime import datetime
import json
import logging
from configs import EngineConfig


class TimeseriesQueue:
    def __init__(self):
        self.prefix = EngineConfig.REDIS_TS_PREFIX
        self.max_records = EngineConfig.REDIS_TS_MAX_RECORDS
        self.max_age_in_seconds = EngineConfig.REDIS_TS_MAX_AGE_IN_SECONDS
        self.timeline_key = f"{self.prefix}:{EngineConfig.REDIS_TS_TIMELINE_KEY}"
        self.client = redis.Redis(
            host=EngineConfig.REDIS_TS_SERVER,
            port=EngineConfig.REDIS_TS_PORT,
            username=EngineConfig.REDIS_TS_USER,
            password=EngineConfig.REDIS_TS_PASS,
            db=EngineConfig.REDIS_TS_DB
        )
       
    def _purge_stale_records(self, key, timestamp):
        # Remove records older than max_age_in_seconds
        min_timestamp = timestamp - self.max_age_in_seconds
        self.client.zremrangebyscore(key, 0, min_timestamp)

        # Remove old records if exceeding max_records
        # count = self.client.zcard(key)
        # if count > self.max_records:
        #     self.client.zremrangebyrank(key, 0, count - self.max_records - 1)


    def push(self, ip, data):
        key = f"{self.prefix}:{ip}"
        timestamp = data['timestamp']
        # timestamp = datetime.now().timestamp()
        value = json.dumps({'ts': timestamp, 'data': data, 'ip': ip})
       
        self.client.zadd(key, {value: timestamp})
        self.client.zadd(self.timeline_key, {value: timestamp})
       
        # self._purge_stale_records(key, timestamp)
        # self._purge_stale_records(self.timeline_key, timestamp)

           
    def batch_push(self, records):
        """
        Push multiple records efficiently in a batch.
        
        Args:
            records: List of tuples (ip, data) to be pushed
        """
        if not records:
            logging.info("No records to push")
            return

        current_timestamp = datetime.now(tz=EngineConfig.TZ_OFFSET).timestamp()
        pipeline = self.client.pipeline(transaction=True)  # Create a transactional pipeline

        try:
            # First stage: Add all records
            for ip, data in records:
                key = f"{self.prefix}:{ip}"
                value = json.dumps({'ts': current_timestamp, 'data': data, 'ip': ip})
                
                # Queue the ZADD commands in the pipeline
                pipeline.zadd(key, {value: current_timestamp})
                pipeline.zadd(self.timeline_key, {value: current_timestamp})

            # Execute the pipeline and get results
            results = pipeline.execute()  # Returns list of responses for each command
            
            if len(results) != len(records) * 2:  # 2 commands per record
                raise Exception(f"Expected {len(records) * 2} results, got {len(results)}")

            # Second stage: Cleanup old records
            pipeline = self.client.pipeline(transaction=True)  # New pipeline for cleanup
            
            # Add cleanup commands for each unique IP
            unique_keys = {f"{self.prefix}:{ip}" for ip, _ in records}
            for key in unique_keys:
                # Remove records older than max_age_in_seconds
                min_timestamp = current_timestamp - self.max_age_in_seconds
                pipeline.zremrangebyscore(key, 0, min_timestamp)
                
                # Remove excess records
                pipeline.zcard(key)  # Get current count
            
            # Add timeline cleanup
            pipeline.zremrangebyscore(self.timeline_key, 0, min_timestamp)
            
            # Execute cleanup pipeline
            cleanup_results = pipeline.execute()
            
            # Process results to handle max_records limit
            # i = 0
            # for key in unique_keys:
            #     # Skip the zremrangebyscore result
            #     i += 1
            #     # Get the zcard result
            #     count = cleanup_results[i]
            #     if count > self.max_records:
            #         # If over limit, remove oldest records
            #         self.client.zremrangebyrank(key, 0, count - self.max_records - 1)
            #     i += 1

        except redis.RedisError as e:
            logging.error(f"Redis error during batch push: {str(e)}")
            raise
        except Exception as e:
            logging.error(f"Error during batch push: {str(e)}")
            raise
        
        # Return number of records successfully added
        return len(results) // 2


    def get_by_ip(self, ip, start_ts=0, end_ts=float('inf')):
       key = f"{self.prefix}:{ip}"
       return self.client.zrangebyscore(key, start_ts, end_ts)


    def show_ip(self, ip):
       key = f"{self.prefix}:{ip}"
       logs = self.client.zrange(key, 0, -1)
       return [json.loads(log.decode()) for log in logs]


    def get_last_n(self, n=100):
       logs = self.client.zrange(self.timeline_key, -n, -1)
       return [json.loads(log.decode()) for log in logs]
    
    
    def get_first_n(self, n=100):
        logs = self.client.zrange(self.timeline_key, 0, n-1)
        return [json.loads(log.decode()) for log in logs]
    
    
    def get_last_seconds(self, seconds, ip=None):
        """
        Get records from the last X seconds, optionally filtered by IP
        
        Args:
            seconds (int): Number of seconds to look back
            ip (str, optional): If provided, only return records for this IP
            
        Returns:
            list: Records from the specified time window
        """
        current_timestamp = datetime.now(tz=EngineConfig.TZ_OFFSET).timestamp()
        min_timestamp = current_timestamp - seconds
        
        if ip:
            # Get records for specific IP
            key = f"{self.prefix}:{ip}"
            logs = self.client.zrangebyscore(key, min_timestamp, current_timestamp)
        else:
            # Get records across all IPs from timeline
            logs = self.client.zrangebyscore(self.timeline_key, min_timestamp, current_timestamp)
            
        return [json.loads(log.decode()) for log in logs]

    
    def show_all (self):
        # Get all IP keys
        keys = self.client.keys(f"{self.prefix}:*")
        result = {}
       
        for key in keys:
            if key.decode() != self.timeline_key:  # Skip timeline key
                ip = key.decode().split(':')[1]
                logs = self.client.zrange(key, 0, -1)
                result[ip] = [json.loads(log.decode()) for log in logs]
           
        return result
    
    
    def delete_by_ip_old (self, ip):
       key = f"{self.prefix}:{ip}"
       self.client.delete(key)
       
    
    def delete_by_ip(self, ip):
        """
        Delete all records for a specific IP from both IP-specific set and timeline.
        
        Args:
            ip: IP address to delete records for
            
        Returns:
            int: Number of records deleted
        """
        key = f"{self.prefix}:{ip}"
        
        try:
            # Get all records for this IP first
            records = self.client.zrange(key, 0, -1)
            if not records:
                return 0
                
            pipeline = self.client.pipeline(transaction=True)
            
            # Delete from timeline
            for record in records:
                pipeline.zrem(self.timeline_key, record)
                
            # Delete the IP's sorted set
            pipeline.delete(key)
            
            # Execute all commands
            results = pipeline.execute()
            
            # First n results are from timeline deletions, last result is from key deletion
            timeline_deleted = sum(results[:-1])
            return timeline_deleted
            
        except redis.RedisError as e:
            logging.error(f"Redis error during IP deletion: {str(e)}")
            raise
        except Exception as e:
            logging.error(f"Error during IP deletion: {str(e)}")
            raise
          
    
    def delete_fetched_records(self, records):
        """
        Delete specific records that were fetched by any getter method.
        Takes plain records (no need for _meta information).
        
        Args:
            records: List of record dictionaries returned by any getter method
            
        Returns:
            int: Number of records successfully deleted
        """
        if not records:
            return 0

        try:
            pipeline = self.client.pipeline(transaction=True)
            
            # Group records by IP for efficient deletion
            ip_groups = {}
            
            for record in records:
                if not isinstance(record, dict) or 'ip' not in record or 'ts' not in record:
                    raise ValueError("Invalid record format")
                
                ip = record['ip']
                key = f"{self.prefix}:{ip}"
                value = json.dumps(record)  # Reconstruct original value
                
                if key not in ip_groups:
                    ip_groups[key] = []
                ip_groups[key].append(value)
                
                # Also add to timeline deletion
                pipeline.zrem(self.timeline_key, value)
            
            # Remove from IP-specific sets
            for key, values in ip_groups.items():
                pipeline.zrem(key, *values)
            
            # Execute all deletions
            results = pipeline.execute()
            
            # Sum up all successful deletions (excluding timeline deletion results)
            total_deleted = sum(results[len(records):])  # Skip timeline results
            return total_deleted

        except redis.RedisError as e:
            logging.error(f"Redis error during record deletion: {str(e)}")
            raise
        except Exception as e:
            logging.error(f"Error during record deletion: {str(e)}")
            raise
        
        
    def flush_all(self):
        keys = self.client.keys(f"{self.prefix}:*")
        if keys:
            self.client.delete(*keys)


    def purge_old(self):
        """
        Purge all records older than specified seconds across all IP keys and timeline.
                
        Returns:
            # dict: Number of records deleted per key
            dict: Number of records deleted
        """
        try:
            min_timestamp = datetime.now(tz=EngineConfig.TZ_OFFSET).timestamp() - self.max_age_in_seconds
            
            # Get all IP keys
            keys = self.client.keys(f"{self.prefix}:*")
            
            pipeline = self.client.pipeline(transaction=True)
            
            # Add delete commands for old records in each key
            for key in keys:
                key = key.decode()
                if key == self.timeline_key:
                    pipeline.zremrangebyscore(self.timeline_key, 0, min_timestamp)
                else:
                    pipeline.zremrangebyscore(key, 0, min_timestamp)
                
            # Execute all commands
            results = pipeline.execute()
            
            deletion_total = 0
            
            for count in results:
                deletion_total += count
                
            return deletion_total
        
            # Create result dictionary mapping keys to number of records deleted
            # deletion_counts = {}
            # for key, count in zip(keys, results):
                # deletion_counts[key.decode()] = count
                
            # return deletion_counts
                
        except redis.RedisError as e:
            logging.error(f"Redis error during purge: {str(e)}")
            raise
        except Exception as e:
            logging.error(f"Error during purge: {str(e)}")
            raise
    
def tests ():
    queue = TimeseriesQueue()
    test_ip = '1.1.1.0'
    batch_max_size = 10_000   # process when size reaches X records
    batch_max_wait = 2        # process when batch collecting time reaches X seconds

    while True:
        queue.flush_all()
        
        start_batch = time.time()
        batch = []
        
        for i in range(10_000):
            ip = f'1.1.1.{i}'
            start = time.time()
            for s in range(10):
                data = {'path': '/api', 'num': s}
                # BATCH PUSH
                batch.append((ip, data))
                
                # SINGLE PUSH
                # queue.push(ip, data)
            # logging.info(f'push -> {ip}: {time.time() - start:.2f}')
            
            batch_size = len(batch)
            # batch_wait = int(time.time() - start_batch)
            
            if batch_size >= batch_max_size:
                # or batch_wait % batch_max_wait == 0:
                # logging.info(f'batch_wait: {batch_wait}')
                logging.info(f'batch_push -> {batch_size}: {time.time() - start:.2f}')
                queue.batch_push(batch)
                batch = []
                # time.sleep(.1)
                
        logging.info(f'batch total: {time.time() - start_batch:.2f}')
        
        # logging.info(f'--- QUEUE: {ip} ----')
        # logging.info(queue.show_ip(ip))

        start = time.time()
        test_records = queue.get_first_n(30)
        logging.info(f'get_first_n: {time.time() - start:.2f}')
        for i in test_records:
            logging.info(i)
        
        start = time.time()
        test_records = queue.get_last_n(30)
        logging.info(f'get_last_n: {time.time() - start:.2f}')
        for i in test_records:
            logging.info(i)
            
        # Test getting records of last X seconds
        start = time.time()
        recent_records = queue.get_last_seconds(5)
        logging.info(f'time_recent_records: {time.time() - start:.2f} -> {len(recent_records)}')
        # to_delete = [r for r in all_records if r['data']['num'] == 1]  # Delete records with num=1
        # deleted = queue.delete_fetched_records(recent_records)
        # for i, record in enumerate(recent_records):
        #     logging.info(record)
        #     if i == 10:
        #         break
            
        # Test getting records of last X seconds for specific IP
        start = time.time()
        ip_recent_records = queue.get_last_seconds(30, ip=test_ip)
        logging.info(f'ip_recent_records: {time.time() - start:.2f} -> {len(ip_recent_records)}')
        for i, record in enumerate(ip_recent_records):
            logging.info(record)
            if i == 20:
                break

        # logging.info('--- QUEUE ----')
        # logging.info(queue.show_all())

        logging.info(f'--- QUEUE: {test_ip} BEFORE ----')
        logging.info(len(queue.show_ip(test_ip)))
        # logging.info(queue.show_ip(test_ip))

        logging.info(f'--- DELETE: {test_ip} ----')
        start = time.time()
        queue.delete_by_ip(test_ip)
        logging.info(f'delete_by_ip: {time.time() - start:.2f}')

        logging.info(f'--- QUEUE: {test_ip} AFTER ----')
        start = time.time()
        logging.info(len(queue.show_ip(test_ip)))
        # logging.info(queue.show_ip(test_ip))
        logging.info(f'show_ip: {time.time() - start:.2f}')

        # logging.info('--- QUEUE ----')
        # logging.info(queue.show_all())

        break
    
if __name__ == "__main__":
    # tests()
    pass