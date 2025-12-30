#!/bin/bash
set -e

# 1. Start Kafka Connect (lệnh gốc của image)
echo "Starting Kafka Connect..."
/docker-entrypoint.sh &

# 2. Chờ Connect lên
echo "Waiting Kafka Connect..."
until curl -s http://localhost:8083/connectors >/dev/null; do
  sleep 5
done

# 3. Register connector
echo "Registering connector..."
curl -X POST http://localhost:8083/connectors \
  -H "Content-Type: application/json" \
  -d @/connectors/postgres-dbz-connector.json

# 4. Giữ container sống
wait