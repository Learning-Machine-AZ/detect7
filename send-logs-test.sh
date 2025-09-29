#!/usr/bin/env bash
statuses=(200 200 200 200 404 500 301)
# methods=("GET" "POST" "PUT" "DELETE")
methods=("GET" "POST")
paths=("/api/users" "/api/orders" "/test-endpoint" "/images" "/api/auth")

HOST=${HOST:-10.0.0.100}
PORT=${PORT:-514}
CONCURRENCY=${CONCURRENCY:-20}
REQUESTS=${REQUESTS:-1000}
DOMAIN=${DOMAIN:-example.com}
REMOTE_IP=${REMOTE_IP:-100.100.100.100}

usage() {
  echo "Usage: $0 [-H|--host HOST] [-p|--port PORT] [-c|--concurrency N] [-n|--requests N] [-d|--domain DOMAIN] [-i|--ip REMOTE_IP]" >&2
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    -H|--host)
      HOST="$2"; shift 2 ;;
    -p|--port)
      PORT="$2"; shift 2 ;;
    -c|--concurrency)
      CONCURRENCY="$2"; shift 2 ;;
    -n|--requests)
      REQUESTS="$2"; shift 2 ;;
    -d|--domain)
      DOMAIN="$2"; shift 2 ;;
    -i|--ip)
      REMOTE_IP="$2"; shift 2 ;;
    -h|--help)
      usage; exit 0 ;;
    *)
      echo "Unknown option: $1" >&2; usage; exit 1 ;;
  esac
done

for ((i=1; i<=REQUESTS; i++)); do
  (
    status=${statuses[$((RANDOM % ${#statuses[@]}))]}
    method=${methods[$((RANDOM % ${#methods[@]}))]}
    path=${paths[$((RANDOM % ${#paths[@]}))]}
    
    echo "Request #$i: $method $path -> $status" >&2
    json=$(cat <<EOF
{"domain": "$DOMAIN", "timestamp": "$(date +%s.%3N)", "timestamp_local": "$(date '+%d/%b/%Y:%H:%M:%S %z')", "timestamp_iso8601": "$(date -Iseconds)", "remote_addr": "$REMOTE_IP", "request": "$method $path/$i HTTP/1.1", "response_status": "$status", "body_bytes_sent": "$((RANDOM % 5000 + 500))", "http_referrer": "https://$DOMAIN/dashboard", "http_user_agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36", "request_time": "0.$((RANDOM % 999 + 100))", "connection": "$((12000 + i))", "connection_requests": "$((RANDOM % 10 + 1))", "request_length": "$((RANDOM % 1000 + 200))", "upstream_cache_status": "HIT", "upstream_addr": "10.0.1.50:8080", "http_x_forwarded_for": "", "upstream_response_time": "0.$((RANDOM % 200 + 50))"}
EOF
)
    pri="<13>"  # user-level notice
    ts3164=$(date '+%b %e %H:%M:%S')
    hostname="$DOMAIN"
    tag="ddos7"
    syslog_line="$pri$ts3164 $hostname $tag: $json"
    printf "%s" "$syslog_line" > /dev/udp/$HOST/$PORT
    sleep 0.01  # Small delay to avoid overwhelming
  ) &
  while [ "$(jobs -r | wc -l)" -ge "$CONCURRENCY" ]; do
    wait -n
  done
done
wait