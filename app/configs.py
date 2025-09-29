import os
import pika
import logging
import warnings
from datetime import datetime, timezone, timedelta
from dotenv import load_dotenv
from dataclasses import dataclass
load_dotenv()

warnings.filterwarnings('ignore')

logging.getLogger("pika").setLevel(logging.WARNING)

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s | %(levelname)s | PID: %(process)d | %(message)s',
    # filename='syslog_server.log'
)

@dataclass
class RabbitMQ_Config:
    """Configuration for RabbitMQ connection"""
    url: str
    exchange: str
    queue: str
    routing_key: str
    

class RabbitMQ_URLParameters (pika.URLParameters):
    def __init__(self, url: str):
        super().__init__(url)
        self.heartbeat = EngineConfig.RABBITMQ_HEARTBEAT
        self.retry_delay = EngineConfig.RABBITMQ_RETRY_DELAY
        self.socket_timeout = EngineConfig.RABBITMQ_SOCKET_TIMEOUT
        self.connection_attempts = EngineConfig.RABBITMQ_CONNECTION_ATTEMPTS
        self.blocked_connection_timeout = EngineConfig.RABBITMQ_BLOCKED_CONNECTION_TIMEOUT
        self.frame_max = EngineConfig.RABBITMQ_FRAME_MAX
        

class EngineConfig:
    # GENERAL SETTINGS
    DEBUG = os.getenv('DEBUG', 'False').lower() in ('true', '1')
    IS_LOCAL = os.getenv('MODE', 'local') == "local"
    TZ_OFFSET = timezone(timedelta(hours=int(os.getenv('TZ_OFFSET_HOURS'))))
    
    # STATIC CONTENT EXTENTIONS
    STATIC_EXTENSIONS = {
        # Images
        '.jpg', '.jpeg', '.png', '.gif', '.svg', '.ico', '.webp',
        # Stylesheets and scripts
        '.css', '.js', '.jsx', '.ts', '.tsx',
        # Fonts
        '.ttf', '.woff', '.woff2', '.eot',
        # Documents
        '.pdf', '.doc', '.docx', '.xls', '.xlsx',
        # Media
        '.mp3', '.mp4', '.wav', '.avi', '.mov',
        # Other
        '.txt', '.csv', '.xml', '.json'
    }
    
    # SERVER SETTINGS
    SYSLOG_SERVER_LISTEN = os.getenv('SYSLOG_SERVER_LISTEN', '0.0.0.0')
    SYSLOG_SERVER_PORT = int(os.getenv('SYSLOG_SERVER_PORT', 514))
    SYSLOG_SERVER_BUFFER_LIMIT_LOGS = int(os.getenv('SYSLOG_SERVER_BUFFER_LIMIT_LOGS', 500))
    SYSLOG_SERVER_BUFFER_LIMIT_QUEUE = int(os.getenv('SYSLOG_SERVER_BUFFER_LIMIT_QUEUE', 100))
    SYSLOG_SERVER_GRAYLOG_TAG = os.getenv('SYSLOG_SERVER_GRAYLOG_TAG')
    
    # GRAYLOG SETTINGS
    GRAYLOG_SERVER = os.getenv('GRAYLOG_SERVER')
    GRAYLOG_PORT = int(os.getenv('GRAYLOG_PORT', 12201))
    GRAYLOG_TIMEOUT = int(os.getenv('GRAYLOG_TIMEOUT', 3))
    
    # DETECTOR SETTINGS
    DETECTOR_MODEL_PATH = os.getenv('DETECTOR_MODEL_PATH')
    DETECTOR_WINDOW_SIZE = int(os.getenv('DETECTOR_WINDOW_SIZE', 60))
    DETECTOR_TRESHOLD_VALUE = float(os.getenv('DETECTOR_TRESHOLD_VALUE', 0.7))
    DETECTOR_MIN_RECORDS = float(os.getenv('DETECTOR_MIN_RECORDS', 3))
    DETECTOR_MIN_RPS = float(os.getenv('DETECTOR_MIN_RPS', 2))
    
    # DETECTOR WORKER SETTINGS
    DETECTOR_WORKER_GRAYLOG_TAG = os.getenv('DETECTOR_WORKER_GRAYLOG_TAG')
    DETECTOR_WORKER_GRAYLOG_PRIORITY = int(os.getenv('DETECTOR_WORKER_GRAYLOG_PRIORITY'))
    DETECTOR_WORKER_NUM_WORKERS = int(os.getenv('DETECTOR_WORKER_NUM_WORKERS', 3))
    DETECTOR_WORKER_BATCH_SIZE = int(os.getenv('DETECTOR_WORKER_BATCH_SIZE', 100))
    DETECTOR_WORKER_WINDOW_SECONDS = int(os.getenv('DETECTOR_WORKER_WINDOW_SECONDS', 30))
    DETECTOR_WORKER_SLEEP_SECONDS = int(os.getenv('DETECTOR_WORKER_SLEEP_SECONDS', 5))
    
    # REDIS TS SETTINGS
    REDIS_TS_PREFIX = os.getenv('REDIS_TS_PREFIX', 'logs')
    REDIS_TS_TIMELINE_KEY = os.getenv('REDIS_TS_TIMELINE_KEY', 'timeline')
    REDIS_TS_MAX_RECORDS = int(os.getenv('REDIS_TS_MAX_RECORDS', 100))
    REDIS_TS_MAX_AGE_IN_SECONDS = int(os.getenv('REDIS_TS_MAX_AGE_IN_SECONDS', 100))
    REDIS_TS_SERVER = os.getenv('REDIS_TS_SERVER', 'localhost')
    REDIS_TS_USER = os.getenv('REDIS_TS_USER', '')
    REDIS_TS_PASS = os.getenv('REDIS_TS_PASS', '')
    REDIS_TS_PORT = int(os.getenv('REDIS_TS_PORT', 6379))
    REDIS_TS_DB = int(os.getenv('REDIS_TS_DB', 0))
    
    # REDIS IPQUEUE SETTINGS
    REDIS_IPQ_QUEUE_NAME = os.getenv('REDIS_IPQ_QUEUE_NAME', 'ip_queue')
    REDIS_IPQ_MAX_AGE_IN_SECONDS = int(os.getenv('REDIS_IPQ_MAX_AGE_IN_SECONDS', 1800))
    REDIS_IPQ_SERVER = os.getenv('REDIS_IPQ_SERVER', 'localhost')
    REDIS_IPQ_USER = os.getenv('REDIS_IPQ_USER', '')
    REDIS_IPQ_PASS = os.getenv('REDIS_IPQ_PASS', '')
    REDIS_IPQ_PORT = int(os.getenv('REDIS_IPQ_PORT', 6379))
    REDIS_IPQ_DB = int(os.getenv('REDIS_IPQ_DB', 0))
    
    # RABBITMQ SETTINGS
    RABBITMQ_URL = os.getenv('RABBITMQ_URL')
    RABBITMQ_HEARTBEAT = int(os.getenv('RABBITMQ_HEARTBEAT', 30))
    RABBITMQ_RETRY_DELAY = int(os.getenv('RABBITMQ_RETRY_DELAY', 3))
    RABBITMQ_SOCKET_TIMEOUT = int(os.getenv('RABBITMQ_SOCKET_TIMEOUT', 10))
    RABBITMQ_CONNECTION_ATTEMPTS = int(os.getenv('RABBITMQ_CONNECTION_ATTEMPTS', 9999))
    RABBITMQ_BLOCKED_CONNECTION_TIMEOUT = int(os.getenv('RABBITMQ_BLOCKED_CONNECTION_TIMEOUT', 30))
    RABBITMQ_FRAME_MAX = int(os.getenv('RABBITMQ_FRAME_MAX', 131072))
    
    # SYSLOG RABBITMQ SETTINGS
    RABBITMQ_SYSLOG_EXCHANGE = os.getenv('RABBITMQ_SYSLOG_EXCHANGE')
    RABBITMQ_SYSLOG_QUEUE = os.getenv('RABBITMQ_SYSLOG_QUEUE')
    RABBITMQ_SYSLOG_ROUTING_KEY = os.getenv('RABBITMQ_SYSLOG_ROUTING_KEY')
    
    # CHALLENGE RABBITMQ SETTINGS
    RABBITMQ_CHALLENGE_EXCHANGE = os.getenv('RABBITMQ_CHALLENGE_EXCHANGE')
    RABBITMQ_CHALLENGE_QUEUE = os.getenv('RABBITMQ_CHALLENGE_QUEUE')
    RABBITMQ_CHALLENGE_ROUTING_KEY = os.getenv('RABBITMQ_CHALLENGE_ROUTING_KEY')

    # IP2LOCATION
    IP2LOCATION_BIN_PATH = os.getenv('IP2LOCATION_PATH_LOCAL') if IS_LOCAL else os.getenv('IP2LOCATION_PATH_PRODUCTION')
    IP2LOCATION_IPv4_BIN = os.path.join(IP2LOCATION_BIN_PATH, os.getenv('IP2LOCATION_IPv4_BIN'))
    IP2LOCATION_IPv6_BIN = os.path.join(IP2LOCATION_BIN_PATH, os.getenv('IP2LOCATION_IPv6_BIN'))
    
    # KNOWN BOTS
    KNOWN_BOTS_PATH = os.getenv('KNOWN_BOTS_PATH_LOCAL') if IS_LOCAL else os.getenv('KNOWN_BOTS_PATH_PRODUCTION')
    KNOWN_BOTS_IPS_FOLDER = os.path.join(KNOWN_BOTS_PATH, os.getenv('KNOWN_BOTS_IPS_FOLDER'))
    KNOWN_BOTS_CACHE_FILE = os.path.join(KNOWN_BOTS_PATH, os.getenv('KNOWN_BOTS_CACHE_FILE'))