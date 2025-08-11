#!/bin/bash

# Deploy Flink streaming job
echo "Deploying Enhanced Flink streaming job..."

# Copy jar to container if needed
docker cp flink-streaming-job/target/flink-streaming-job-1.0-SNAPSHOT.jar cdc_jobmanager:/opt/flink/

# Submit the enhanced job to Flink cluster
docker exec cdc_jobmanager flink run \
  --class com.cdc.streaming.EnhancedEngagementStreamingJob \
  /opt/flink/flink-streaming-job-1.0-SNAPSHOT.jar

echo "Enhanced Flink job submitted successfully!"
echo "You can monitor the job at: http://localhost:8080"