import json
import socket
import logging
import time
import pika
import traceback
from pika.exchange_type import ExchangeType
from datetime import datetime
from typing import Dict, Any, List
from dataclasses import dataclass
from configs import EngineConfig, RabbitMQ_URLParameters
from syslog_server import NumpyEncoder
from contextlib import contextmanager
from threading import Lock

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
)

def normalize_timestamp(value):
    """Return UNIX epoch seconds as float for various timestamp inputs."""
    try:
        if value is None:
            return datetime.now(tz=EngineConfig.TZ_OFFSET).timestamp()

        if isinstance(value, (int, float)):
            return float(value)

        if isinstance(value, str):
            v = value.strip()
            # Numeric string
            try:
                return float(v)
            except ValueError:
                pass
            # ISO-8601 string
            try:
                dt = datetime.fromisoformat(v)
                if dt.tzinfo is None:
                    dt = dt.replace(tzinfo=EngineConfig.TZ_OFFSET)
                return dt.timestamp()
            except Exception:
                pass

        if isinstance(value, dict):
            # Handle nested formats, e.g., {"$date": "..."}
            inner = value.get("$date")
            if inner is not None:
                return normalize_timestamp(inner)
    except Exception:
        pass

    return datetime.now(tz=EngineConfig.TZ_OFFSET).timestamp()

@dataclass
class GraylogConfig:
    """Configuration for Graylog forwarding"""
    host: str = EngineConfig.GRAYLOG_SERVER
    port: int = EngineConfig.GRAYLOG_PORT
    timeout: int = EngineConfig.GRAYLOG_TIMEOUT
    facility: str = "syslog-server"


@dataclass
class QueueBinding:
    """Configuration for a queue binding"""
    exchange: str
    queue: str
    routing_keys: List[str]
    exchange_type: str = ExchangeType.direct
    durable: bool = True
                

class GraylogClient:
    def __init__(self):
        self.config = GraylogConfig()
        self.host = self.config.host
        self.port = self.config.port
        self.timeout = self.config.timeout
        self._socket = None
        self._lock = Lock()
        self.large_message_log_threshold = 25 * 1024
        
    def _create_socket(self):
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.settimeout(self.timeout)
        sock.connect((self.host, self.port))
        return sock
    
    @contextmanager
    def get_socket(self):
        with self._lock:  # Ensure thread-safe access to socket
            try:
                if not self._socket:
                    self._socket = self._create_socket()
                yield self._socket
            except socket.error as e:
                # Handle reconnection
                if self._socket:
                    try:
                        self._socket.close()
                    except:
                        pass  # Ignore errors during close
                    self._socket = None
                raise
    
    def send_message(self, message):
        max_retries = 3
        retry_delay = 0.5  # seconds
        message_bytes = message.encode('utf-8')

        # Log large messages for debugging
        if len(message_bytes) >= self.large_message_log_threshold:
            try:
                preview = message[:1024]
            except Exception:
                preview = '<unavailable>'
            logging.debug(
                f"Large Graylog send: size={len(message_bytes)} bytes, host={self.host}, port={self.port}, preview={preview}"
            )
        
        for attempt in range(max_retries):
            try:
                with self.get_socket() as sock:
                    sock.sendall(message_bytes)
                    logging.debug(f"Graylog: sent message of {len(message_bytes)} bytes")
                return  # Success, exit the function
            except socket.error as e:
                if attempt == max_retries - 1:  # Last attempt
                    raise  # Re-raise the last error
                time.sleep(retry_delay * (attempt + 1))  # Exponential backoff
    
    def close(self):
        with self._lock:
            if self._socket:
                try:
                    self._socket.close()
                finally:
                    self._socket = None


def forward_to_graylog(client: GraylogClient, message: Dict[str, Any]) -> bool:
    """Forward a message to Graylog using GELF format"""
    try:
        # Prepare GELF message
        gelf_message = {
            "version": "1.1",
            "host": message.get("hostname", "unknown"), # socket.gethostname()
            "short_message": message.get("message", ""),
            "timestamp": normalize_timestamp(message.get("timestamp")),
            "facility": client.config.facility,
            "_source_queue": message.get("_source_queue", "unknown"),
            "_source_exchange": message.get("_source_exchange", "unknown"),
            "tag": message.get("tag", ""),
        }

        # Add additional fields with '_' prefix
        for key, value in message.items():
            if key not in ["message", "hostname", "timestamp", "tag"] and key[0] != '_':
                gelf_message[f"_{key}"] = value

        # Compress and send message
        message_json = json.dumps(gelf_message, cls=NumpyEncoder, ensure_ascii=False)

        # if gelf_message["tag"] == EngineConfig.DETECTOR_WORKER_GRAYLOG_TAG:
        #     logging.info(message_json)

        message_json += '\0' # Add null terminator required by Graylog TCP input
        # message_bytes = message.encode('utf-8')
        
        # sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        # sock.connect((graylog_config.host, graylog_config.port))
        # sock.sendall(message_bytes)
        # sock.close()
        client.send_message(message_json)
        
        return True
        
    except Exception as e:
        logging.error(f"Error forwarding to Graylog: {str(e)}")
        return False


class GraylogWorker:
    """RabbitMQ worker for processing messages and forwarding to Graylog"""

    def __init__(self, graylog_client: GraylogClient, queue_bindings: List[QueueBinding]):
        self.rabbitmq_url = EngineConfig.RABBITMQ_URL
        self.graylog_client = graylog_client
        self.queue_bindings = queue_bindings
        self.parameters = None
        self.connection = None
        self.channel = None
        self.success = 0
        self.errors = 0
        self.log_treshold = 10000

    def setup_rabbitmq(self) -> bool:
        """Setup RabbitMQ connection and channel"""
        try:
            # Create connection
            self.parameters = RabbitMQ_URLParameters(self.rabbitmq_url)
            self.connection = pika.BlockingConnection(self.parameters)
            self.channel = self.connection.channel()
            self.channel.basic_qos(prefetch_count=1)

            # Set up all exchanges, queues, and bindings
            for binding in self.queue_bindings:
                # Declare exchange
                self.channel.exchange_declare(
                    exchange=binding.exchange,
                    exchange_type=binding.exchange_type,
                    durable=binding.durable
                )

                # Declare queue
                self.channel.queue_declare(
                    queue=binding.queue,
                    durable=binding.durable
                )

                # Create bindings for all routing keys
                for routing_key in binding.routing_keys:
                    self.channel.queue_bind(
                        exchange=binding.exchange,
                        queue=binding.queue,
                        routing_key=routing_key
                    )

                # Set up consumer for this queue
                self.channel.basic_consume(
                    queue=binding.queue,
                    on_message_callback=lambda ch, method, props, body: 
                        self.callback(ch, method, props, body, binding)
                )

            logging.info("Successfully connected to RabbitMQ and set up all bindings")
            return True

        except Exception as e:
            logging.error(f"Failed to setup RabbitMQ connection: {str(e)}")
            return False

    def callback(self, ch, method, properties, body, binding: QueueBinding):
        """Process received messages"""
        message = None
        
        try:
            # Parse message
            message = json.loads(body.decode())
            # Normalize message to dict in case publishers sent a JSON string or non-object
            if isinstance(message, str):
                # Try to parse again if it's a JSON-encoded JSON string; else wrap
                try:
                    maybe_obj = json.loads(message)
                    message = maybe_obj if isinstance(maybe_obj, dict) else {"message": message}
                except Exception:
                    message = {"message": message}
            elif not isinstance(message, dict):
                message = {"message": message}
            
            # Add source information
            message['_source_queue'] = binding.queue
            message['_source_exchange'] = binding.exchange
            
            # Forward to Graylog
            success = forward_to_graylog(self.graylog_client, message)
            
            if success:
                ch.basic_ack(delivery_tag=method.delivery_tag)
                self.success += 1
            else:
                # Negative acknowledgment, message will be requeued
                self.errors += 1
                ch.basic_nack(delivery_tag=method.delivery_tag, requeue=True)

            if (self.success > 0 and self.success % self.log_treshold == 0) \
                or (self.errors > 0 and self.errors % self.log_treshold == 0):
                logging.info(f"Success: {self.success} | Errors: {self.errors}")
                
        except json.JSONDecodeError as e:
            logging.error(f"Failed to decode message from {binding.queue}: {str(e)}")
            # Message is malformed, don't requeue
            ch.basic_ack(delivery_tag=method.delivery_tag)
        except Exception as e:
            logging.error(f"Error processing message from {binding.queue}: {str(e)}")
            logging.info(message)
            logging.error(traceback.format_exc())
            # Requeue message for retry
            ch.basic_nack(delivery_tag=method.delivery_tag, requeue=True)

    def start_consuming(self):
        """Start consuming messages from all queues"""
        attempts = 0
        while True:
            try:
                logging.info(f"Started consuming from queues: {[binding.queue for binding in self.queue_bindings]}")
                self.channel.start_consuming()
                
            except KeyboardInterrupt:
                self.channel.stop_consuming()
                self.connection.close()
                logging.info("Stopped consuming messages")
                
            except Exception as e:
                if attempts == self.parameters.connection_attempts:
                    logging.error("Max retry attempts reached.")
                    raise
                
                logging.error(f"Error while consuming messages: {str(e)}")
                time.sleep(self.parameters.retry_delay)
                self.setup_rabbitmq()


def main():
    queue_bindings = [
        QueueBinding(
            exchange=EngineConfig.RABBITMQ_SYSLOG_EXCHANGE,
            queue=EngineConfig.RABBITMQ_SYSLOG_QUEUE,
            routing_keys=[EngineConfig.RABBITMQ_SYSLOG_ROUTING_KEY]
        ),
        QueueBinding(
            exchange=EngineConfig.RABBITMQ_CHALLENGE_EXCHANGE,
            queue=EngineConfig.RABBITMQ_CHALLENGE_QUEUE,
            routing_keys=[EngineConfig.RABBITMQ_CHALLENGE_ROUTING_KEY],
        ),
    ]

    graylog_client = GraylogClient()
    worker = GraylogWorker(graylog_client, queue_bindings)
    
    if worker.setup_rabbitmq():
        worker.start_consuming()

if __name__ == "__main__":
    main()