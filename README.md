# Detect7 - DDoS Detection System

A near real-time DDoS detection system that uses machine learning to analyze network traffic patterns and identify potential attacks.

## Overview

Detect7 is a comprehensive DDoS detection platform that combines:
- **Near real-time log processing** via syslog server
- **Machine learning detection** using LightGBM models
- **Log aggregation** with Graylog
- **Message queuing** with RabbitMQ
- **Caching & Reporting** with Redis
- **DNS resolution** with BIND9 and caching

### Dashboard Screenshots

The imported dashboard provides comprehensive monitoring of your DDoS detection system:

![Main Dashboard](dashboard/screenshots/1.%20Dashboard.png)
*Main DDoS Detection Dashboard - Overview of traffic patterns and detection metrics*

![Known Bots](dashboard/screenshots/2.%20Known%20Bots.png)
*Known Bots Analysis - Tracking legitimate bot traffic and filtering*

![SEO Analysis](dashboard/screenshots/3.%20SEO.png)
*SEO Traffic Analysis - Monitoring search engine and crawler activity*


### DDoS Response/Prevention

The Redis queue (`REDIS_IPQ_QUEUE_NAME`) will contain a list of detected abusing IP addresses that can be used for automated response actions (not included in this repo):

- **Firewall Integration**: Block malicious IPs automatically
- **Cool-down Pages**: Redirect suspicious traffic to maintenance pages
- **Rate Limiting**: Implement dynamic rate limiting for detected IPs
- **Custom Response**: Integrate with your existing security tools/infrastructure

Access the queue using Redis commands or integrate with your security tools for automated response.
See "Detect7 - Architecture and example usage with Nginx.pdf" for example setup.

## Quick Start

### Prerequisites

- Docker and Docker Compose
- At least 4GB RAM
- Ports 514 (UDP), 9000, 12201, 15672, 6379 available

### 1. Clone and Setup

```bash
git clone https://github.com/Learning-Machine-AZ/detect7
cd detect7

# Unzip detector ML model
unzip models/lgbm-20m-bytes-100k-ips-research/model.zip
```

### 2. Download IP2Location Database

Download free "IP2Location Lite" database files to enable geolocation features from:
https://lite.ip2location.com/database/db3-ip-country-region-city

or unzip included database files as of 15/09/2025:

```bash
unzip models/ip2location/IP2LOCATION-LITE-DB3.IPV6.zip
unzip models/ip2location/IP2LOCATION-LITE-DB3.zip
```

**Note**: You need to register at [IP2Location](https://www.ip2location.com/register) to get the download links for the free LITE databases.

### 3. Environment Configuration

Use provided `example.env` file to config and customize system.

### 4. Start the System

```bash
docker-compose up -d
```

### 5. Access Services

- **Graylog Web UI**: http://localhost:9000 (admin/admin7)
- **RabbitMQ Management**: http://localhost:15672 (ddos7/ddos7)
- **Syslog Server**: UDP port 514

## Architecture

### Core Components

1. **Syslog Server** (`syslog_server.py`)
   - Receives syslog messages on UDP port 514
   - Parses and enriches log data with geolocation
   - Queues data for ML processing

2. **ML Worker** (`ml_worker.py`)
   - Processes log data using LightGBM model
   - Detects DDoS patterns in near real-time
   - Sends alerts for suspicious activity

3. **Graylog Worker** (`graylog_worker.py`)
   - Forwards processed logs to Graylog
   - Handles log aggregation and storage

### Infrastructure Services

- **MongoDB**: Graylog data storage
- **Redis**: Caching and time-series data
- **RabbitMQ**: Message queuing between components
- **BIND9**: DNS resolution for IP enrichment

## Configuration

### Detection Parameters

| Parameter | Default | Description |
|-----------|---------|-------------|
| `DETECTOR_TRESHOLD_VALUE` | 0.7 | ML model confidence threshold |
| `DETECTOR_WINDOW_SIZE` | 60 | Analysis window in seconds |
| `DETECTOR_MIN_RPS` | 2 | Minimum requests per second to analyze |

### Log Processing

| Parameter | Default | Description |
|-----------|---------|-------------|
| `SYSLOG_SERVER_PORT` | 514 | UDP port for syslog input |
| `SYSLOG_SERVER_BUFFER_LIMIT_LOGS` | 500 | Max logs in buffer |

## Usage

### Sending Test Logs

Use the provided test script:

```bash
./send-logs-test.sh
```

### Send real logs from Nginx:

Add logging to remote syslog to virtual host:

```
access_log syslog:server=10.0.0.200:514,tag=ddos7,severity=info gelf_json;
```

### Monitoring

1. **Check container status**:
   ```bash
   docker-compose ps
   ```

2. **View logs**:
   ```bash
   docker-compose logs -f syslog-server
   docker-compose logs -f ml-worker
   ```

3. **Access Dashboard**:
   - Navigate to Graylog http://localhost:9000
   - Login with admin/admin7
   - Import ready-to-use dashboard via System -> Content Packs -> Upload: `dashboard/content-pack-detect7.json`
   - View dashboards and alerts

### Custom Log Format

The system expects syslog messages in this format:
```
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
```

## Model Information

20 million records including 100K unique "bad" IPs addresses were used to train the model.
The system uses a pre-trained LightGBM model located at:
- **Model**: `app/models/lgbm-20m-bytes-100k-ips-research/ddos_detector.txt`
- **Metadata**: `app/models/lgbm-20m-bytes-100k-ips-research/ddos_detector_metadata.json`


### Features Analyzed

- Request patterns and frequencies
- IP behavior analysis
- Traffic burst detection
- Protocol analysis
- Error rate monitoring
- Geographic distribution

### Performance Tuning

- Adjust `DETECTOR_WINDOW_SIZE` for different analysis windows
- Modify `DETECTOR_TRESHOLD_VALUE` for sensitivity tuning
- Scale workers by adjusting `DETECTOR_WORKER_NUM_WORKERS`

## Security Notes

- Change default passwords in production
- Use environment variables for sensitive data
- Configure firewall rules for syslog port

## Support

The is a pet project and may not be actively maintained.
But you are free to open PRs and ask questions.