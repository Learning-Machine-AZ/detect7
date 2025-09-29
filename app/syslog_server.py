import time
import json
import logging
import sys
import re
from pathlib import Path
from datetime import datetime
import numpy as np
import queue as MessageQueue
import socketserver
import threading
from threading import Lock
from typing import Dict, Any, Optional, Tuple, Deque
from collections import defaultdict, deque
import pika
from pika.exchange_type import ExchangeType
from configs import EngineConfig, RabbitMQ_Config, RabbitMQ_URLParameters
from redis_ip_queue import IPQueue
from redis_ts_queue import TimeseriesQueue
import ipaddress
import IP2Location
# import pandas as pd
import socket
import traceback
import pickle
import radix
import dns.resolver
import dns.reversename


def is_ipv4 (ip_str):
    try:
        return isinstance(ipaddress.ip_address(ip_str), ipaddress.IPv4Address)
    except ValueError:
        pass
    return False


class IP2Hostname:
    def __init__(self, maxsize=10000):
        self.maxsize = maxsize
        self.cache = {}  # {ip: hostname}
        self.insertion_order = {}  # {ip: counter}
        self.counter = 0
        self.lock = Lock()
    
    def add(self, ip: str, hostname: str) -> None:
        """
        Add or update IP-hostname mapping.
        """
        with self.lock:
            if ip not in self.cache:
                if len(self.cache) >= self.maxsize:
                    # Find and remove oldest entry using counter
                    oldest_ip = min(self.insertion_order.items(), key=lambda x: x[1])[0]
                    del self.cache[oldest_ip]
                    del self.insertion_order[oldest_ip]
                
                self.insertion_order[ip] = self.counter
                self.counter += 1
                
            self.cache[ip] = hostname
    
    def exists(self, ip: str) -> bool:
        """
        Check if IP exists in cache.
        """
        return ip in self.cache
    
    def detect (self, ip: str) -> Optional[str]:
        """
        Get hostname for IP.
        """
        hostname = self.cache.get(ip)
        
        if not hostname:
            hostname = self.ip_lookup(ip)
            self.add(ip, hostname)
            
        return hostname
    
    def ip_lookup (self, ip: str):
        try:            
            hostname = socket.gethostbyaddr(ip)[0]
            return hostname
        except socket.herror as e:
            logging.debug(f"Error resolving hostname: {e}")
        return None
    
    def __len__(self) -> int:
        return len(self.cache)

    def clear(self) -> None:
        """
        Clear the entire cache.
        Synchronized with lock since it modifies state.
        """
        with self.lock:
            self.cache.clear()
            self.insertion_order.clear()
            self.counter = 0
            

class IP2Hostname_V2:
    def __init__(self, maxsize=10000, timeout=300):
        """
        Initialize IP to hostname resolver with caching
        
        Args:
            maxsize (int): Maximum number of entries in cache
            timeout (int): Cache entry timeout in seconds
        """
        # Initialize DNS resolver with custom settings
        # https://dnspython.readthedocs.io/en/latest/resolver-class.html
        self.resolver = dns.resolver.Resolver()
        
        if EngineConfig.IS_LOCAL:
            self.resolver.nameservers = [
                '8.8.8.8',    # Google 1
                '8.8.4.4',    # Google 2
            ]
        else:
            self.resolver.nameservers = [
                '10.10.0.250', # LOCAL DNS
            ]
        
        self.resolver.timeout = 1.0  # 1 second timeout for each nameserver
        self.resolver.lifetime = 1.0  # 1 second total timeout
        self.resolver.cache = dns.resolver.Cache(cleaning_interval=3600)  # DNS level cache
        
        # Log DNS configuration
        logging.info(f"DNS Resolver configured with nameservers: {self.resolver.nameservers}")
        
        # Application level cache for faster lookups
        self.cache = {}  # {ip: (hostname, timestamp)}
        self.negative_cache = {}  # {ip: timestamp} for failed lookups
        self.maxsize = maxsize
        self.timeout = timeout
        self.lock = Lock()
        self.last_cleanup = time.time()
        self.cleanup_interval = 60  # Cleanup every 60 seconds
        
        # Test DNS resolution
        self._test_dns_resolution()
        
    def _cleanup_if_needed(self) -> None:
        """Perform cleanup only if interval has passed"""
        now = time.time()
        if now - self.last_cleanup > self.cleanup_interval:
            self._cleanup_expired()
            self.last_cleanup = now

                
    def _test_dns_resolution(self):
        """Test DNS resolution with configured nameservers"""
        test_ip = '8.8.8.8'  # Google DNS IP for testing
        try:
            hostname = self._ip_lookup(test_ip)
            if hostname:
                logging.info(f"DNS resolution test successful: {test_ip} -> {hostname}")
            else:
                logging.warning("DNS resolution test returned no hostname")
        except Exception as e:
            logging.error(f"DNS resolution test failed: {str(e)}")
            
    
    def get_nameserver_stats(self):
        """Get statistics about nameserver response times"""
        stats = {}
        for ns in self.resolver.nameservers:
            try:
                start_time = time.time()
                self._ip_lookup('8.8.8.8')  # Test lookup
                response_time = time.time() - start_time
                stats[ns] = {
                    'response_time': response_time,
                    'status': 'ok'
                }
            except Exception as e:
                stats[ns] = {
                    'response_time': None,
                    'status': f'error: {str(e)}'
                }
        return stats
            

    def _cleanup_expired(self) -> None:
        """Remove expired entries from cache"""
        now = time.time()
        
        # Cleanup main cache
        expired = [ip for ip, (_, timestamp) in self.cache.items() 
                  if now - timestamp > self.timeout]
        for ip in expired:
            del self.cache[ip]
            
        # Cleanup negative cache
        expired = [ip for ip, timestamp in self.negative_cache.items() 
                  if now - timestamp > self.timeout]
        for ip in expired:
            del self.negative_cache[ip]
    
    
    def detect(self, ip: str) -> Optional[str]:
        """
        Get hostname for IP with caching and negative caching
        
        Args:
            ip (str): IP address to resolve
            
        Returns:
            Optional[str]: Hostname if found, None otherwise
        """
        try:
            now = time.time()
            
            with self.lock:
            # Check negative cache first
                if ip in self.negative_cache:
                    if now - self.negative_cache[ip] < self.timeout:
                        return None
                    del self.negative_cache[ip]
            
                # Check main cache
                if ip in self.cache:
                    hostname, timestamp = self.cache[ip]
                    if now - timestamp < self.timeout:
                        return hostname
                    del self.cache[ip]
                
            # Periodic cleanup (only if needed)
            self._cleanup_if_needed()
            
            # Perform reverse DNS lookup
            hostname = self._ip_lookup(ip)
            
            # Update caches
            with self.lock:
                if hostname:
                    # Simple cache management - if full, remove oldest entry
                    if len(self.cache) >= self.maxsize:
                        oldest_ip = min(self.cache.items(), key=lambda x: x[1][1])[0]
                        del self.cache[oldest_ip]
                    self.cache[ip] = (hostname, now)
                else:
                    self.negative_cache[ip] = now
            
            return hostname
            
        except Exception as e:
            logging.error(f"Error in detect() for IP {ip}: {str(e)}")
            return None
    
    def _ip_lookup(self, ip: str) -> Optional[str]:
        """
        Perform reverse DNS lookup with timeout
        
        Args:
            ip (str): IP address to resolve
            
        Returns:
            Optional[str]: Hostname if found, None otherwise
        """
        try:
            # Convert IP to reverse pointer
            rev_name = dns.reversename.from_address(ip)
            
            # Perform PTR lookup
            answers = self.resolver.resolve(rev_name, 'PTR')
            
            if answers:
                # Return first PTR record
                return str(answers[0].target).rstrip('.')
                
        except (dns.resolver.NXDOMAIN, dns.resolver.NoAnswer):
            # Domain doesn't exist or no PTR record
            logging.debug(f"No PTR record for {ip}")
        except dns.resolver.Timeout:
            # Query timed out
            logging.debug(f"DNS lookup timeout for {ip}")
        except dns.exception.DNSException as e:
            # Other DNS-related errors
            logging.debug(f"DNS lookup failed for {ip}: {str(e)}")
        except Exception as e:
            # Unexpected errors
            logging.error(f"Unexpected error in DNS lookup for {ip}: {str(e)}")
        
        return None
    
    def __len__(self) -> int:
        """Get current cache size"""
        return len(self.cache)

    def clear(self) -> None:
        """Clear all caches"""
        with self.lock:
            self.cache.clear()
            self.negative_cache.clear()
            
    def clear_dns_cache(self):
        """Clear the DNS resolver cache"""
        self.resolver.cache.flush()
        logging.info("DNS resolver cache cleared")


"""
    limiter = RateLimiter(requests_per_second=3)
    
    if limiter.is_allowed(ip):
        print(f"Request from {ip} allowed")
        remaining, reset_time = limiter.get_remaining_tokens(ip)
        print(f"Remaining tokens: {remaining}, Reset in: {reset_time:.2f}s")
    else:
        print(f"Request from {ip} blocked (rate limit exceeded)")
"""
class RateLimiter:
    def __init__(self, requests_per_second: int = 10):
        """
        Initialize the rate limiter with a specified rate limit.
        
        Args:
            requests_per_second (int): Maximum number of requests allowed per second per IP
        """
        self.rate_limit = requests_per_second
        self.window_size = 1  # 1 second window
        self.requests: Dict[str, Deque[float]] = defaultdict(deque)
        self.lock = Lock()

    def is_allowed(self, ip: str) -> bool:
        """
        Check if a request from the given IP is allowed based on the rate limit.
        
        Args:
            ip (str): The IP address making the request
            
        Returns:
            bool: True if request is allowed, False if rate limit is exceeded
        """
        with self.lock:
            current_time = time.time()
            
            # Remove timestamps older than our window
            self._clean_old_requests(ip, current_time)
            
            # Check if we're at the rate limit
            if len(self.requests[ip]) >= self.rate_limit:
                return False
            
            # Add current request timestamp
            self.requests[ip].append(current_time)
            return True
    
    def _clean_old_requests(self, ip: str, current_time: float) -> None:
        """
        Remove timestamps that are outside of the current window.
        
        Args:
            ip (str): The IP address to clean
            current_time (float): Current timestamp
        """
        while self.requests[ip] and current_time - self.requests[ip][0] > self.window_size:
            self.requests[ip].popleft()

    def get_remaining_tokens(self, ip: str) -> Tuple[int, float]:
        """
        Get the number of remaining requests allowed and time until reset.
        
        Args:
            ip (str): The IP address to check
            
        Returns:
            Tuple[int, float]: (remaining requests, seconds until window reset)
        """
        with self.lock:
            current_time = time.time()
            self._clean_old_requests(ip, current_time)
            
            remaining = self.rate_limit - len(self.requests[ip])
            if not self.requests[ip]:
                return remaining, 0.0
                
            reset_time = self.requests[ip][0] + self.window_size - current_time
            return remaining, max(0.0, reset_time)


class IP2LocationDetector:
    def __init__(self, db_path_ipv4, db_path_ipv6):
        """
        Initialize IP2Location detector with database path
        
        Args:
            db_path_ipv4 (str): Path to IP2Location IPv4 database file
            db_path_ipv6 (str): Path to IP2Location IPv6 database file
        """
        self.lock = Lock()
        
        try:
            self.ip2location = IP2Location.IP2Location(db_path_ipv4)
            self.ip2location_ipv6 = IP2Location.IP2Location(db_path_ipv6)
            # Query a test IP to determine available fields
            test_record = self.ip2location.get_all("8.8.8.8")
            self.available_fields = dir(test_record)
        except Exception as e:
            logging.error(f"Error loading database: {e}")
            sys.exit(1)

    def _is_valid_ip(self, ip_address):
        """
        Validate IP address format
        
        Args:
            ip_address (str): IP address to validate
            
        Returns:
            bool: True if valid IP, False otherwise
        """
        try:
            socket.inet_aton(ip_address)
            return True
        except socket.error:
            return False

    def detect (self, ip_address):
        """
        Detect the type of IP address (VPN, Proxy, Hosting, etc)
        
        Args:
            ip_address (str): IP address to check
            
        Returns:
            dict: Dictionary containing detection results
        """
        # if not self._is_valid_ip(ip_address):
        #     return {"error": "Invalid IP address format"}

        try:
            with self.lock:
                # Query the database
                if is_ipv4(ip_address):
                    result = self.ip2location.get_all(ip_address)
                else:
                    result = self.ip2location_ipv6.get_all(ip_address)

                if not result:
                    return None

                # Create base detection results
                detection_results = {
                    "country": result.country_short,
                    "region": result.region,
                    "city": result.city,
                    # "country_name": result.country_long,
                    # "isp": result.isp if 'isp' in self.available_fields else "Unknown"
                }

                # Add advanced detection if ISP information is available
                # if 'isp' in self.available_fields and result.isp:
                #     detection_results.update({
                #         "is_hosting": self._check_hosting(result),
                #         "is_tor": self._check_tor(result),
                #         "is_vpn": self._check_vpn(result),
                #         "is_vps": self._check_vps(result)
                #     })

                # Add proxy detection if available in database
                # if 'is_proxy' in self.available_fields:
                #     detection_results["is_proxy"] = result.is_proxy

                # Add datacenter detection if available in database
                # if 'is_datacenter' in self.available_fields:
                #     detection_results["is_datacenter"] = result.is_datacenter

                return detection_results

        except Exception as e:
            logging.error(f"IP2Location fail ({ip_address}): {str(e)}")
            

    def _check_hosting(self, result):
        """Check if IP belongs to hosting provider"""
        hosting_keywords = [
            'host', 'datacenter', 'data center', 'hosting', 
            'amazon', 'google', 'microsoft', 'azure', 'aws', 
            'cloud', 'digitalocean', 'linode', 'vultr'
        ]
        return any(keyword.lower() in result.isp.lower() for keyword in hosting_keywords)

    def _check_tor(self, result):
        """Check if IP is a Tor exit node"""
        tor_keywords = ['tor', 'exit node', 'torproject']
        return any(keyword.lower() in result.isp.lower() for keyword in tor_keywords)

    def _check_vpn(self, result):
        """Check if IP belongs to VPN service"""
        vpn_keywords = [
            'vpn', 'virtual private network', 'nordvpn', 'expressvpn', 
            'private internet access', 'protonvpn', 'cyberghost', 
            'tunnelbear', 'mullvad', 'privateVPN'
        ]
        return any(keyword.lower() in result.isp.lower() for keyword in vpn_keywords)

    def _check_vps(self, result):
        """Check if IP belongs to VPS provider"""
        vps_keywords = [
            'vps', 'virtual private server', 'vultr', 'linode', 
            'digitalocean', 'scaleway', 'ovh', 'hetzner'
        ]
        return any(keyword.lower() in result.isp.lower() for keyword in vps_keywords)


# https://github.com/mjschultz/py-radix
class FastIPSubnetChecker:
    def __init__(self, ips_directory: str = "ips", cache_file: str = "ip_cache.bin"):
        self.ips_directory = Path(ips_directory)
        self.cache_file = Path(cache_file)
        self.rtree = radix.Radix()
        self.lock = Lock()
        
        self.categories_supported = {
            'social': ['applebot', 'facebookbot', 'linkedin', 'pinterest', 'telegrambot', 'twitterbot'],
            'search': ['bingbot', 'googlebot', 'yahoo', 'yandex'],
            'minor_search': ['baidu', 'duckduckbot'],
            'analytics': ['ahrefsbot', 'semrush'],
            'cdn': ['bunnycdn', 'cloudflare'],
            'ai': ['chatgpt', 'claude'],
            'lan': ['internal'],
        }
            
        # TO-DO
        self.categories_unsupported = {
            'monitoring': ['outageowl', 'betteruptimebot', 'pingdombot', 'uptimerobot', 'webpagetestbot'],
            'minor_crawler': ['freshpingbot', 'imagekit', 'imgix', 'marginalia'],
            'minor_payment': ['molliewebhook', 'stripewebhook'],
            'minor_search': ['blekko', 'mojeekbot'],
            'minor_aggregator': ['rssapi'],
            'datacenter': ['datacenter'],
            'anonymizer': ['vpn'],
        }
        
        if self.cache_file.exists():
            self._load_from_cache()
        else:
            self._build_radix_trees()
            self._save_to_cache()


    def category_supported (self, network: str):
        return next((k for k, v in self.categories_supported.items() if network in v), None)
    
    
    def category_unsupported (self, network: str):
        return next((k for k, v in self.categories_unsupported.items() if network in v), None)
    

    def _build_radix_trees(self) -> None:
        logging.info('Building radix database')
        
        if not self.ips_directory.exists():
            raise FileNotFoundError(f"Directory {self.ips_directory} does not exist")

        for file_path in self.ips_directory.glob("*.ips"):
            subnet_name = file_path.stem
            category_supported = self.category_supported(subnet_name)
            category_unsupported = self.category_unsupported(subnet_name)
            category = category_supported if category_supported else category_unsupported
            support = 1 if category_supported else 0

            if not category:
                continue
            
            with open(file_path) as f:
                for line in set(f.readlines()):
                    line = line.strip()
                    if not line or line.startswith('#'):
                        continue
                        
                    try:
                        if '/' in line:
                            network = ipaddress.ip_network(line, strict=False)
                        else:
                            ip = ipaddress.ip_address(line)
                            network = ipaddress.ip_network(f"{ip}/128" if ip.version == 6 else f"{ip}/32", strict=False)
                        
                        node = self.rtree.add(str(network))
                        node.data['data'] = {'name': subnet_name, 'category': category, 'support': support}
                        
                    except Exception as e:
                        logging.error(f"Invalid IP/CIDR in {file_path}: {line} - {str(e)}")
                        logging.error(traceback.format_exc())
        
        logging.info('Building radix database complete')


    def _save_to_cache(self) -> None:
        with open(self.cache_file, 'wb') as f:
            pickle.dump(self.rtree, f)


    def _load_from_cache(self) -> None:
        with open(self.cache_file, 'rb') as f:
            self.rtree = pickle.load(f)


    def detect (self, ip: str) -> Tuple[Optional[str], Optional[str]]:
        try:
            addr = ipaddress.ip_address(ip)
            with self.lock:
                node = self.rtree.search_best(str(addr))
                if node:
                    data = node.data['data']
                    return data['name'], data['category'], data['support']
        except ValueError as e:
            logging.error(f"Invalid IP address {ip} - {str(e)}")
        
        return None, None, 0
    

class NumpyEncoder(json.JSONEncoder):
    """ Custom encoder for numpy data types """
    def default(self, obj):
        if isinstance(obj, (np.int_, np.intc, np.intp, np.int8,
                            np.int16, np.int32, np.int64, np.uint8,
                            np.uint16, np.uint32, np.uint64)):
            return int(obj)

        elif isinstance(obj, (np.float16, np.float32, np.float64)):
            return float(obj)
        
        elif isinstance(obj, (np.complex64, np.complex128)):
            return {'real': obj.real, 'imag': obj.imag}
        
        elif isinstance(obj, (np.ndarray,)):
            return obj.tolist()
    
        elif isinstance(obj, (np.bool_)):
            return bool(obj)

        elif isinstance(obj, (np.void)): 
            return None

        elif isinstance(obj, datetime):
            return obj.isoformat()

        return super().default(self, obj)


class RabbitMQ_Client:
    """RabbitMQ worker for processing messages and forwarding to Graylog"""
    
    def __init__(self, rabbitmq_config: RabbitMQ_Config):
        self.config = rabbitmq_config
        self.connection = None
        self.channel = None
        self.parameters = None
        self.large_message_log_threshold = 25 * 1024
        self.setup_rabbitmq()


    def setup_rabbitmq(self):
        """Setup RabbitMQ connection and channel"""
        try:            
            # Create connection
            self.parameters = RabbitMQ_URLParameters(self.config.url)
            self.connection = pika.BlockingConnection(self.parameters)
            self.channel = self.connection.channel()
            # Enable publisher confirms and returned message callback for unroutable messages
            try:
                self.channel.confirm_delivery()
            except Exception:
                # confirm_delivery may not be available in some pika versions; continue without it
                pass
            try:
                self.channel.add_on_return_callback(self._on_returned_message)
            except Exception:
                pass

            # Declare exchange and queue
            self.channel.exchange_declare(
                exchange=self.config.exchange,
                exchange_type=ExchangeType.direct,
                durable=True,
            )

            self.channel.queue_declare(
                queue=self.config.queue,
                durable=True
            )

            self.channel.queue_bind(
                exchange=self.config.exchange,
                queue=self.config.queue,
                routing_key=self.config.routing_key
            )

            logging.debug(f"#{sys._getframe().f_lineno} Successfully connected to RabbitMQ")
            return True
        
        # except (AMQPConnectionError, AMQPChannelError, StreamLostError) as e:
        except Exception as e:
            logging.error(f"#{sys._getframe().f_lineno} Failed to setup RabbitMQ connection failed: {e}")
            self.cleanup()
            raise
        

    def send_message(self, message: Dict[str, Any]):
        """Serialize and publish a message to RabbitMQ with retries."""
        # Serialize to UTF-8 bytes; RabbitMQ handles framing, so no manual chunking
        message_str = json.dumps(message, cls=NumpyEncoder, ensure_ascii=False)
        message_bytes = message_str.encode('utf-8')

        # Log large messages for debugging
        if len(message_bytes) >= self.large_message_log_threshold:
            try:
                preview = message_str[:1024]
            except Exception:
                preview = '<unavailable>'
            logging.debug(
                f"Large RabbitMQ publish: size={len(message_bytes)} bytes, "
                f"exchange={self.config.exchange}, routing_key={self.config.routing_key}, preview={preview}"
            )

        attempts = 0
        max_attempts = 3
        last_error = None
        while attempts < max_attempts:
            try:
                self._publish_message(message_bytes)
                return
            except Exception as e:
                last_error = e
                attempts += 1
                try:
                    # Recreate connection/channel before retrying
                    self.cleanup()
                    self.setup_rabbitmq()
                except Exception:
                    pass
                time.sleep(0.1 * attempts)

        logging.error(f"#{sys._getframe().f_lineno} Failed to publish message after {max_attempts} attempts: {last_error}")
        raise last_error
        
                
    def _publish_message(self, message):
        try:
            if not self.connection or not self.connection.is_open:
                self.setup_rabbitmq()
            
            # Publish message
            self.channel.basic_publish(
                exchange=self.config.exchange,
                routing_key=self.config.routing_key,
                body=message,
                properties=pika.BasicProperties(
                    delivery_mode=2,
                    content_type='application/json',
                    content_encoding='utf-8'
                ),
                mandatory=True
            )
            
            logging.debug(f"#{sys._getframe().f_lineno} Successfully sent message to RabbitMQ")
            
        except Exception as e:
            logging.error(f"#{sys._getframe().f_lineno} Failed to publish message to RabbitMQ: {e}")
            try:
                # Avoid logging entire huge payloads; log size instead
                logging.error(f"Message size: {len(message)} bytes")
            except Exception:
                pass
            raise

    def _on_returned_message(self, channel, method, properties, body):
        try:
            logging.error(
                f"Returned (unroutable) message from exchange '{method.exchange}' with routing_key '{method.routing_key}'. "
                f"Reply code {method.reply_code}: {method.reply_text}. Size: {len(body)} bytes"
            )
        except Exception:
            logging.error("Returned (unroutable) message received but failed to log details")
        

    def cleanup(self):
        """Careful cleanup of channel and connection"""
        try:
            if self.channel is not None:
                if self.channel.is_open:
                    self.channel.close()
                self.channel = None
        except Exception as e:
            logging.error(f"Error closing channel: {e}")
            self.channel = None

        try:
            if self.connection is not None:
                if self.connection.is_open:
                    self.connection.close()
                self.connection = None
                # logging.info("Closed RabbitMQ connection")
        except Exception as e:
            # logging.error(f"Error closing connection: {e}")
            self.connection = None


class SyslogMessage:
    """Class to parse and store syslog message components"""
    
    def __init__(self, raw_message: str):
        self.raw_message = raw_message
        self.priority: Optional[int] = None
        self.timestamp: Optional[datetime] = None
        self.hostname: str = ""
        self.message: str = ""
        self.client_address: str = ""
        self.json_data: Optional[Dict[str, Any]] = None
        self.parse_message()


    def parse_message(self) -> None:
        """Parse the raw syslog message into components"""
        try:
            # Basic syslog pattern
            pattern = r'<(\d+)>(\w+\s+\d+\s+\d+:\d+:\d+)\s+(\S+)\s+(.*)'
            match = re.match(pattern, self.raw_message)
            
            if match:
                self.priority = int(match.group(1))
                self.timestamp = datetime.now(tz=EngineConfig.TZ_OFFSET)
                """ self.timestamp = datetime.strptime(
                    f"{datetime.now().year} {match.group(2)}", 
                    "%Y %b %d %H:%M:%S"
                ) """
                self.hostname = match.group(3)
                self.message = match.group(4)
                
                # Try to extract JSON from the message
                self.extract_json()
            else:
                logging.warning(f"Could not parse message: {self.raw_message}")
                
        except Exception as e:
            logging.error(f"Error parsing message: {str(e)}")
            logging.error(traceback.format_exc())
            raise


    def extract_json(self) -> None:
        """Extract JSON content from the message if present"""
        try:
            # Look for JSON content between curly braces
            json_pattern = r'\{.*\}'
            json_match = re.search(json_pattern, self.message)
            
            if json_match:
                json_str = json_match.group(0)
                self.json_data = json.loads(json_str)
                logging.debug(f"Successfully extracted JSON data from message")
            else:
                raise
                
        except json.JSONDecodeError as e:
            logging.warning(f"Failed to parse JSON content: {str(e)}")
            logging.error(traceback.format_exc())
            
        except Exception as e:
            logging.error(f"Unexpected error while extracting JSON: {str(e)}")
            logging.error(traceback.format_exc())


class SyslogHandler(socketserver.BaseRequestHandler):
    def handle(self):
        """Handle incoming syslog messages"""
        data = None
        client_address = None
        
        try:
            # Get data and client address atomically to prevent race conditions
            data, _ = self.request
            client_address = self.client_address[0]
            
            # Add rate limiting check                
            # if not self.server.rate_limiter.is_allowed(client_address):
            #     logging.warning(f"Rate limit exceeded for {client_address}")
            #     return
            
            # Check message size to prevent memory issues
            if len(data) > 65535:  # Standard max UDP packet size
                logging.warning(f"Message from {client_address} exceeds maximum size")
                return
                
            # Try to put message in queue with timeout
            try:
                self.server.message_queue.put_nowait((client_address, data))
                self.server.counter += 1
                
            except MessageQueue.Full:
                self.server.dropped += 1
                
                # Log every Nth dropped message to prevent log spam
                if self.server.dropped % 100 == 0:
                    logging.error(f"Queue full - dropped {self.server.dropped} messages, last from {client_address}")
                return
                
        except ConnectionError as e:
            logging.warning(f"Connection error from {client_address}: {str(e)}")
            
        except Exception as e:
            logging.error(f"Error handling message from {client_address}: {str(e)}")
            if data:
                logging.error(f"Problem data: {data}...")  # Log truncated data
            logging.debug(traceback.format_exc())


class ThreadedSyslogServer(socketserver.ThreadingUDPServer):
    """Threaded UDP server for handling syslog messages"""
    def __init__(self, 
                 server_address: tuple, 
                 handler_class: socketserver.BaseRequestHandler,
                 message_queue: MessageQueue,
                 rate_limiter: RateLimiter):
        
        self.allow_reuse_address = True
        self.message_queue = message_queue
        self.rate_limiter = rate_limiter
        self.counter = 0
        self.dropped = 0
        
        super().__init__(server_address, handler_class)
    
    def shutdown(self):
        """Override shutdown to stop forwarder"""
        # if self.forwarder:
        #     self.forwarder.cleanup()
        super().shutdown()


def start_server_threads(
    host: str,
    port: int,
    queue: Optional[TimeseriesQueue] = None,
    cipl: Optional[IPQueue] = None,
    ip2location: Optional[IP2LocationDetector] = None,
    known_bots: Optional[FastIPSubnetChecker] = None
) -> None:
    """Start the syslog server"""
    server = None
    try:
        server = ThreadedSyslogServer((host, port), SyslogHandler, queue, cipl, ip2location, known_bots)
        server_thread = threading.Thread(target=server.serve_forever)
        server_thread.daemon = True
        
        logging.info(f"Starting syslog server on {host}:{port}")
        
        server_thread.start()
        
        # Keep the main thread running
        while True:
            try:
                threading.Event().wait()
            except KeyboardInterrupt:
                logging.info("Received keyboard interrupt signal")
                break
            except Exception as e:
                logging.error(f"Unexpected error in main loop: {str(e)}")
                break
            
    except PermissionError:
        logging.error("Permission denied. Try running with sudo for port 514")
        sys.exit(1)
    except OSError as e:
        logging.error(f"Error starting server: {str(e)}")
        sys.exit(1)
    finally:
        if server:
            try:
                logging.info("Shutting down server...")
                server.shutdown()
                server.server_close()
                logging.info("Server shutdown completed")
            except Exception as e:
                logging.error(f"Error during shutdown: {str(e)}")
                sys.exit(1)
        sys.exit(0)


class MessageProcessor:
    def __init__(self,
                message_queue: MessageQueue = None,
                ts_queue: Optional[TimeseriesQueue] = None,
                cipl: Optional[IPQueue] = None,
                ip2location: Optional[IP2LocationDetector] = None,
                known_bots: Optional[FastIPSubnetChecker] = None,
                ip2hostname: Optional[IP2Hostname] = None):
        
        self.message_queue = message_queue
        self.ts_queue = ts_queue
        self.cipl = cipl
        self.ip2location = ip2location
        self.known_bots = known_bots
        self.ip2hostname = ip2hostname
        
        self.counter = 0
        self.buffer_limit_logs = EngineConfig.SYSLOG_SERVER_BUFFER_LIMIT_LOGS
        self.buffer_limit_queue = EngineConfig.SYSLOG_SERVER_BUFFER_LIMIT_QUEUE
        self.last_buffer_flush_timeout = 3
        self.last_buffer_flush = 0
        self.buffer = {'queue': [], 'logs': []}
        
        self.lock = Lock()
        self.setup_rabbitmq()
        
        
    def start_workers(self, num_workers=4):
        for _ in range(num_workers):
            worker = threading.Thread(
                target=self.process_messages, 
                name=f'WRK{_}',
                daemon=True
            )
            worker.start()
            
    
    def setup_rabbitmq(self):
        # Configure RabbitMQ if URL provided
        rabbitmq_config = RabbitMQ_Config(
            url=EngineConfig.RABBITMQ_URL,
            exchange=EngineConfig.RABBITMQ_SYSLOG_EXCHANGE,
            queue=EngineConfig.RABBITMQ_SYSLOG_QUEUE,
            routing_key=EngineConfig.RABBITMQ_SYSLOG_ROUTING_KEY
        )
        self.rabbitmq_client = RabbitMQ_Client(rabbitmq_config)
    
    
    def has_static_content(self, uri: str) -> bool:
        """
        Check if a URI string contains common static content extensions.
        
        Args:
            uri (str): The URI string to check
            
        Returns:
            bool: True if the URI contains a static content extension, False otherwise
        """
        # Convert URI to lowercase for case-insensitive comparison
        uri_lower = uri.lower()
        
        # Check if URI ends with any static extension
        # return any(uri_lower.endswith(ext) for ext in static_extensions)
        return any(ext in uri_lower for ext in EngineConfig.STATIC_EXTENSIONS)

    
    def decode_message (self, data, encoding: str = 'utf-8'):
        try:
            message = data.decode(encoding).strip()
            return message
        
        except UnicodeDecodeError as e:
            logging.error(f"#{sys._getframe().f_lineno} Failed to decode message: {str(e)}")
            logging.error(traceback.format_exc())
            logging.error(f'{encoding} | {data}')
        
        return self.decode_message(data, encoding='latin1')
    
    
    def process_messages (self):
        """Worker function to process messages from queue"""
        while True:
            try:
                client_address, data = self.message_queue.get()
                message = self.decode_message(data)
                
                try:
                    # Create SyslogMessage instance
                    syslog_msg = SyslogMessage(message)
                    syslog_msg.client_address = client_address # IP OF SERVER SENDING LOGS
                    syslog_msg.json_data['client_address'] = client_address
                    ip = syslog_msg.json_data['remote_addr'] # IP OF VIRTUAL DOMAIN VISITOR
                    
                    # SINGLE SOURCE OF TIMESTAMP
                    syslog_msg.json_data['timestamp'] = syslog_msg.timestamp.timestamp()

                    # STATIC ASSETS
                    syslog_msg.json_data['is_static'] = self.has_static_content(syslog_msg.json_data.get('request'))
                    
                    # URL, METHOD, PROTOCOL
                    request = syslog_msg.json_data.get('request').split(' ') if syslog_msg.json_data.get('request') else None
                    syslog_msg.json_data['http_method']     = request[0] if request else None
                    syslog_msg.json_data['http_uri']        = request[1] if request else None
                    syslog_msg.json_data['http_protocol']   = request[2] if request else None
                    
                    # PTR HOSTNAME
                    # syslog_msg.json_data['ptr'] = self.ip2hostname.detect(ip)
                    
                    # IP2LOCATION
                    ip2location = self.ip2location.detect(ip)
                    syslog_msg.json_data['ip2location'] = ip2location
                    syslog_msg.json_data['country'] = ip2location['country'] if ip2location else 'NA'
                    syslog_msg.json_data['city'] = ip2location['city'] if ip2location else 'NA'

                    # IS KNOWN BOT
                    bot_name, bot_category, bot_support = self.known_bots.detect(ip)
                    syslog_msg.json_data['bot_name'] = bot_name
                    syslog_msg.json_data['bot_category'] = bot_category
                    syslog_msg.json_data['bot_support'] = bot_support
                    
                    # if bot_name:
                    #     logging.debug(f'{ip} | {bot_name} | {bot_type}')
                    
                    # buffer_size_queue = len(self.buffer['queue'])
                    
                    # if buffer_size % 100 == 0:
                    #     logging.debug(f"#{self.counter} | Buffer size: {buffer_size}")
                    
                    # REDIS QUEUE #
                    
                    # SKIP STATIC CONTENT AND KNOWN BOTS
                    if  (not syslog_msg.json_data['is_static'] and not syslog_msg.json_data['bot_support']) and \
                        (self.ts_queue and not self.cipl.contains(ip)):
                        # PUSH TO TS QUEUE
                        self.ts_queue.push(ip, syslog_msg.json_data)
                        # ADD TO BUFFER FOR LATER BATCH PUSH
                        # self.buffer['queue'].append((ip, syslog_msg.json_data))

                    # REDIS TIMESERIESQUEUE BATCH PUSH
                    """ if buffer_size_queue >= self.buffer_limit_queue:
                        with self.lock:
                            flush_buffer = self.buffer['queue']
                            self.buffer['queue'] = []
                            self.ts_queue.batch_push(flush_buffer)
                            logging.info(f"TS QUEUE FLUSH: {buffer_size_queue}")
                    """
                    
                    # RABBIT MQ #
                    
                    message_data = {
                        "timestamp": syslog_msg.timestamp.timestamp(),
                        "hostname": syslog_msg.hostname if syslog_msg.hostname else syslog_msg.json_data.get('domain'),
                        "client_address": syslog_msg.client_address,
                        "message": syslog_msg.message,
                        "priority": syslog_msg.priority,
                        "tag": EngineConfig.SYSLOG_SERVER_GRAYLOG_TAG,
                        "is_static": False,
                    }
                    
                    # Add JSON data if present
                    if syslog_msg.json_data:
                        message_data.update(syslog_msg.json_data)
                    
                    with self.lock:
                        self.buffer['logs'].append(message_data)
                        
                        # Forward to RabbitMQ
                        buffer_size_logs = len(self.buffer['logs'])
                        buffer_flush_size_trigger = buffer_size_logs >= self.buffer_limit_logs
                        buffer_flush_time_trigger = time.time() - self.last_buffer_flush >= self.last_buffer_flush_timeout
                        
                        if buffer_flush_size_trigger or buffer_flush_time_trigger:
                            for message in self.buffer['logs']:
                                self.rabbitmq_client.send_message(message)
                            
                            self.buffer['logs'] = []
                            self.last_buffer_flush = time.time()
                            logging.info(f"WID: {threading.current_thread().name} | FLUSH: {buffer_size_logs}")
                            
                except Exception as e:
                    logging.error(f"Error processing message from {ip}: {e}")
                    
                finally:
                    with self.lock:
                        self.counter += 1
                    self.message_queue.task_done()
                    
            except Exception as e:
                logging.error(f"Worker error: {e}")


def start_server(
    host: str,
    port: int,
    num_workers: int = 3,
    message_queue: MessageQueue = None,
    ts_queue: Optional[TimeseriesQueue] = None,
    cipl: Optional[IPQueue] = None,
    ip2location: Optional[IP2LocationDetector] = None,
    known_bots: Optional[FastIPSubnetChecker] = None,
    # ip2hostname: Optional[IP2Hostname] = None,
    ip2hostname: Optional[IP2Hostname_V2] = None,
    rate_limiter: Optional[RateLimiter] = None
) -> None:
    """Start the syslog server"""
    server = None
    
    try:
        server = ThreadedSyslogServer((host, port), SyslogHandler, message_queue, rate_limiter)
        sock = server.socket
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_RCVBUF, 262144) # Increase OS buffer size for UDP socket, 256KB buffer
        
        # Initialize and start processor
        processor = MessageProcessor(
            message_queue=message_queue, 
            ts_queue=ts_queue, 
            cipl=cipl, 
            ip2location=ip2location, 
            known_bots=known_bots,
            ip2hostname=ip2hostname,
        )
        processor.start_workers(num_workers)
        
        logging.info(f"Starting syslog server on {host}:{port}")
        server.serve_forever()
    
    except Exception as e:
        logging.error(f"Server error: {e}")
    finally:
        if server:
            try:
                logging.info("Shutting down server...")
                server.shutdown()
                server.server_close()
                logging.info("Server shutdown completed")
            except Exception as e:
                logging.error(f"Error during shutdown: {str(e)}")
                
        sys.exit(0)
        

if __name__ == "__main__":
    
    message_queue = MessageQueue.Queue(maxsize=100000)
    ts_queue = TimeseriesQueue()
    cipl = IPQueue()
    # ip2hostname = IP2Hostname()
    ip2hostname = IP2Hostname_V2()
    
    # IP INFO
    known_bots  = FastIPSubnetChecker(
        ips_directory   = EngineConfig.KNOWN_BOTS_IPS_FOLDER,
        cache_file      = EngineConfig.KNOWN_BOTS_CACHE_FILE
    )
    
    ip2location = IP2LocationDetector(
        db_path_ipv4 = EngineConfig.IP2LOCATION_IPv4_BIN,
        db_path_ipv6 = EngineConfig.IP2LOCATION_IPv6_BIN,
    )
    
    rate_limiter = RateLimiter(requests_per_second=1000)
    
    # Start the server
    start_server(
        host=EngineConfig.SYSLOG_SERVER_LISTEN,
        port=EngineConfig.SYSLOG_SERVER_PORT,
        message_queue=message_queue,
        ts_queue=ts_queue,
        cipl=cipl,
        ip2location=ip2location,
        known_bots=known_bots,
        ip2hostname=ip2hostname,
        rate_limiter=rate_limiter
    )

    """
    log_format gelf_json escape=json '{'
        '"domain": "$host",'
        '"timestamp": "$msec",'
        '"timestamp_local": "$time_local",'
        '"timestamp_iso8601": "$time_iso8601",'
        '"remote_addr": "$remote_addr",'
        '"request": "$request",'
        '"response_status": "$status",'
        '"body_bytes_sent": "$body_bytes_sent",'
        '"http_referrer": "$http_referer",'
        '"http_user_agent": "$http_user_agent",'
        '"request_time": "$request_time",'
        '"connection": "$connection",'
        '"connection_requests": "$connection_requests",'
        '"request_length": "$request_length",'
        '"upstream_cache_status": "$upstream_cache_status",'
        '"upstream_addr": "$upstream_addr",'
        '"http_x_forwarded_for": "$http_x_forwarded_for",'
        '"upstream_response_time": "$upstream_response_time"'
        '}';

        access_log syslog:server=127.0.0.1:514,tag=ddos7,severity=info gelf_json;
        access_log syslog:server=detect7.servers.tools:55514,tag=ddos7,severity=info gelf_json;
    """