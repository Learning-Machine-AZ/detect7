import redis
from datetime import datetime
import logging
from configs import EngineConfig


class IPQueue:
    def __init__(self):
        self.queue_name = EngineConfig.REDIS_IPQ_QUEUE_NAME
        self.max_age_in_seconds = EngineConfig.REDIS_IPQ_MAX_AGE_IN_SECONDS
        
        self.client = redis.Redis(
            host=EngineConfig.REDIS_IPQ_SERVER,
            port=EngineConfig.REDIS_IPQ_PORT,
            username=EngineConfig.REDIS_IPQ_USER,
            password=EngineConfig.REDIS_IPQ_PASS,
            db=EngineConfig.REDIS_IPQ_DB
        )
        

    def add(self, ip_address):
        """
        Add an IP address to the queue
        
        Args:
            ip_address (str): IP address to add
        
        Returns:
            bool: True if successful, False otherwise
        """
        try:
            timestamp = datetime.now(tz=EngineConfig.TZ_OFFSET).timestamp()
            self.client.zadd(self.queue_name, {ip_address: timestamp})
            return True
        except Exception as e:
            logging.error(f"Error adding IP: {e}")
            return False


    def remove(self, ip_address):
        """
        Remove an IP address from the queue
        
        Args:
            ip_address (str): IP address to remove
        
        Returns:
            bool: True if IP was found and removed, False otherwise
        """
        try:
            result = self.client.zrem(self.queue_name, ip_address)
            return bool(result)
        except Exception as e:
            logging.error(f"Error removing IP {ip_address}: {e}")
            return False


    def contains(self, ip_address):
        """
        Check if an IP address is in the queue
        
        Args:
            ip_address (str): IP address to check
        
        Returns:
            bool: True if IP is in queue, False otherwise
        """
        try:
            score = self.client.zscore(self.queue_name, ip_address)
            return score is not None
        except Exception as e:
            logging.error(f"Error checking IP {ip_address}: {e}")
            return False


    def purge_old(self):
        """
        Remove entries older than specified seconds
        
        Returns:
            int: Number of entries removed
        """
        try:
            min_timestamp = datetime.now(tz=EngineConfig.TZ_OFFSET).timestamp() - self.max_age_in_seconds
            removed = self.client.zremrangebyscore(self.queue_name, 0, min_timestamp)
            return removed
        except Exception as e:
            logging.error(f"Error purging old entries: {e}")
            return 0


    def clear(self):
        """
        Clear all entries from the queue
        
        Returns:
            bool: True if successful, False otherwise
        """
        try:
            self.client.delete(self.queue_name)
            return True
        except Exception as e:
            logging.error(f"Error clearing queue: {e}")
            return False


def tests ():
    # Initialize queue
    ip_queue = IPQueue(queue_name='ip_tracking')
    
    # Add some IPs
    ip_queue.add('192.168.1.1')
    ip_queue.add('10.10.0.1')
    
    # Check if IP exists
    exists = ip_queue.contains('192.168.1.1')
    logging.info(f"192.168.1.1 exists in queue: {exists}")
    
    # Remove specific IP
    ip_queue.remove('192.168.1.1')
    
    # Purge old entries
    removed_count = ip_queue.purge_old(seconds=60)
    logging.info(f"Purged {removed_count} old entries")
    
    # Clear the queue
    # ip_queue.clear()
    
if __name__ == "__main__":
    # tests()
    pass