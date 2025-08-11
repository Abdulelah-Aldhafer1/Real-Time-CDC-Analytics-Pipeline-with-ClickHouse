

set -e

# Load environment variables from .env file
if [ -f .env ]; then
    export $(cat .env | grep -v '^#' | xargs)
fi

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
PURPLE='\033[0;35m'
NC='\033[0m' # No Color

echo -e "${BLUE}🚀 CDC Pipeline Deployment${NC}"
echo -e "${BLUE}==========================${NC}"

# Function to print status
print_status() {
    local status=$1
    local message=$2
    case $status in
        "INFO") echo -e "${BLUE}ℹ️  $message${NC}" ;;
        "SUCCESS") echo -e "${GREEN}✅ $message${NC}" ;;
        "WARNING") echo -e "${YELLOW}⚠️  $message${NC}" ;;
        "ERROR") echo -e "${RED}❌ $message${NC}" ;;
        "STEP") echo -e "${PURPLE}🔄 $message${NC}" ;;
    esac
}

# Function to wait for service
wait_for_service() {
    local service_name=$1
    local check_command=$2
    local max_attempts=30
    local attempt=1
    
    print_status "INFO" "Waiting for $service_name to be ready..."
    
    while [ $attempt -le $max_attempts ]; do
        if eval $check_command > /dev/null 2>&1; then
            print_status "SUCCESS" "$service_name is ready"
            return 0
        fi
        
        echo -n "."
        sleep 2
        ((attempt++))
    done
    
    print_status "ERROR" "$service_name failed to become ready after ${max_attempts} attempts"
    return 1
}

# Step 1: Start infrastructure
print_status "STEP" "Step 1: Starting infrastructure services"
docker-compose up -d postgres kafka schema-registry redis clickhouse kafka-ui

# Step 2: Wait for services to be ready
print_status "STEP" "Step 2: Waiting for services to be ready"
wait_for_service "PostgreSQL" "docker exec cdc_postgres pg_isready -U postgres"
wait_for_service "Kafka" "docker exec cdc_kafka kafka-topics --bootstrap-server localhost:9092 --list"
wait_for_service "Schema Registry" "curl -s http://localhost:8081/subjects"
wait_for_service "Redis" "docker exec cdc_redis redis-cli -a '${REDIS_PASSWORD}' --no-auth-warning ping"
wait_for_service "ClickHouse" "curl -s 'http://localhost:8123/?query=SELECT%201'"
wait_for_service "Kafka UI" "curl -s http://localhost:8082/api/clusters"

# Step 3: Set up Kafka topics and schemas
print_status "STEP" "Step 3: Setting up Kafka topics and Schema Registry"
chmod +x kafka-config/setup-kafka-topics.sh
./kafka-config/setup-kafka-topics.sh

# Step 4: Start Kafka Connect and deploy Debezium connector
print_status "STEP" "Step 4: Starting Kafka Connect and Debezium connector"
docker-compose up -d connect

wait_for_service "Kafka Connect" "curl -s http://localhost:8083/connectors"

# Deploy Debezium connector
print_status "INFO" "Deploying enhanced Debezium connector..."

# Create a temporary connector config with the correct password from environment
TEMP_CONNECTOR_CONFIG=$(mktemp)
sed "s/ENV_POSTGRES_PASSWORD/${POSTGRES_PASSWORD}/" debezium/enhanced-postgres-connector.json > "$TEMP_CONNECTOR_CONFIG"

curl -X POST -H "Content-Type: application/json" \
    --data @"$TEMP_CONNECTOR_CONFIG" \
    http://localhost:8083/connectors || true

# Clean up temporary file
rm -f "$TEMP_CONNECTOR_CONFIG"

# Wait for connector to be ready
sleep 10
connector_status=$(curl -s http://localhost:8083/connectors/enhanced-postgres-engagement-connector/status | jq -r '.connector.state' 2>/dev/null || echo "UNKNOWN")
if [ "$connector_status" = "RUNNING" ]; then
    print_status "SUCCESS" "Debezium connector is running"
else
    print_status "ERROR" "Debezium connector failed to start: $connector_status"
    exit 1
fi

# Step 5: Set up ClickHouse tables
print_status "STEP" "Step 5: Setting up ClickHouse analytics tables"
docker exec cdc_clickhouse clickhouse-client --queries-file /sql/clickhouse-init.sql

# Step 6: Start data generator
print_status "STEP" "Step 6: Starting data generator"
docker-compose up -d data-generator

# Step 7: Start Flink job
print_status "STEP" "Step 7: Starting Flink streaming job"
docker-compose up -d jobmanager taskmanager

# Wait for Flink to be ready
sleep 30
wait_for_service "Flink JobManager" "curl -s http://localhost:8080/jobs"

# Deploy Flink job
print_status "INFO" "Deploying Flink streaming job..."
chmod +x deploy-flink-job.sh
./deploy-flink-job.sh

# Step 8: Final validation
print_status "STEP" "Step 8: Final validation"
sleep 10

# Check all services
containers=("cdc_postgres" "cdc_kafka" "cdc_schema_registry" "cdc_connect" "cdc_redis" "cdc_clickhouse" "cdc_data_generator" "cdc_jobmanager" "cdc_taskmanager" "cdc_kafka_ui")

for container in "${containers[@]}"; do
    if docker ps --format '{{.Names}}' | grep -q "^${container}$"; then
        print_status "SUCCESS" "$container is running"
    else
        print_status "ERROR" "$container is not running"
    fi
done

print_status "SUCCESS" "CDC Pipeline deployment completed!"
echo ""
echo -e "${GREEN}📊 Access Points:${NC}"
echo -e "  • Flink Dashboard: ${BLUE}http://localhost:8080${NC}"
echo -e "  • Kafka UI: ${BLUE}http://localhost:8082${NC}"
echo -e "  • ClickHouse UI (HTTP): ${BLUE}http://localhost:8123${NC}"
echo ""
echo -e "${GREEN}🔧 Useful Commands:${NC}"
echo -e "  • View logs: ${BLUE}docker-compose logs -f${NC}"
echo -e "  • Stop pipeline: ${BLUE}docker-compose down${NC}"
echo -e "  • Restart: ${BLUE}./deploy.sh${NC}" 