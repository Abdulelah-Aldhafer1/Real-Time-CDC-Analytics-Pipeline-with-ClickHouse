set -e

echo "Setting up Kafka topics..."

# wait for Kafka
echo "Waiting for Kafka..."
until docker exec cdc_kafka kafka-topics --bootstrap-server localhost:9092 --list > /dev/null 2>&1; do
    sleep 2
done
echo "Kafka is ready"

# Create topics (must match kafka-config/topics-and-consumers.json)
echo "Creating topics..."

# Debezium CDC topics (sources)
docker exec cdc_kafka kafka-topics --bootstrap-server localhost:9092 \
    --create --topic cdc.public.engagement_events \
    --partitions 4 --replication-factor 1 || true

docker exec cdc_kafka kafka-topics --bootstrap-server localhost:9092 \
    --create --topic cdc.public.content \
    --partitions 2 --replication-factor 1 || true

# Outbox/enriched topic for external consumers
docker exec cdc_kafka kafka-topics --bootstrap-server localhost:9092 \
    --create --topic external.enriched_events \
    --partitions 3 --replication-factor 1 || true

echo "Topics created"

# list topics
echo "Topics:"
docker exec cdc_kafka kafka-topics --bootstrap-server localhost:9092 --list

# Setup Schema Registry (for CDC topics only)
echo "Setting up Schema Registry..."

until curl -s http://localhost:8081/subjects > /dev/null 2>&1; do
    sleep 2
done
echo "Schema Registry is ready"

if [ -f "schemas/engagement_event.avsc" ]; then
    ENG_SCHEMA=$(jq -Rs . schemas/engagement_event.avsc)
    printf '{"schema": %s}\n' "$ENG_SCHEMA" > /tmp/engagement_schema.json

    curl -s -X POST \
        -H "Content-Type: application/vnd.schemaregistry.v1+json" \
        --data @/tmp/engagement_schema.json \
        http://localhost:8081/subjects/cdc.public.engagement_events-value/versions > /dev/null
    
    echo "Registered engagement event schema"
else
    echo "engagement_event.avsc not found, skipping"
fi

if [ -f "schemas/content.avsc" ]; then
    CONTENT_SCHEMA=$(jq -Rs . schemas/content.avsc)
    printf '{"schema": %s}\n' "$CONTENT_SCHEMA" > /tmp/content_schema.json

    curl -s -X POST \
        -H "Content-Type: application/vnd.schemaregistry.v1+json" \
        --data @/tmp/content_schema.json \
        http://localhost:8081/subjects/cdc.public.content-value/versions > /dev/null
    
    echo "Registered content schema"
else
    echo "content.avsc not found, skipping"
fi

echo "Schemas:"
curl -s http://localhost:8081/subjects | jq .

echo "Kafka setup complete!" 