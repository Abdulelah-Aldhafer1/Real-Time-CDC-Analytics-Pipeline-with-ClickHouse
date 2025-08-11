package com.cdc.streaming;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ReadOnlyBroadcastState;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.BroadcastConnectedStream;
import org.apache.flink.streaming.api.datastream.BroadcastStream;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;

import java.io.Serializable;
import java.time.Duration;
import java.time.LocalDateTime;
import java.time.OffsetDateTime;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;

public class EnhancedEngagementStreamingJob {
    private static final Logger LOG = LoggerFactory.getLogger(EnhancedEngagementStreamingJob.class);
    private static final ObjectMapper objectMapper = new ObjectMapper();

    // Broadcast state descriptor for content table
    private static final MapStateDescriptor<String, ContentInfo> CONTENT_STATE_DESC =
            new MapStateDescriptor<>(
                    "content-broadcast-state",
                    Types.STRING,
                    TypeInformation.of(ContentInfo.class)
            );

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // Enable exactly-once processing
        env.enableCheckpointing(60000);
        env.getCheckpointConfig().setCheckpointTimeout(300000);
        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(15000);
        env.getCheckpointConfig().setMaxConcurrentCheckpoints(1);
        
        env.setParallelism(4);

        // Configuration
        String kafkaBrokers = System.getenv().getOrDefault("KAFKA_BOOTSTRAP_SERVERS", "kafka:29092");
        String redisHost = System.getenv().getOrDefault("REDIS_HOST", "redis");
        int redisPort = Integer.parseInt(System.getenv().getOrDefault("REDIS_PORT", "6379"));
        String redisPassword = System.getenv().getOrDefault("REDIS_PASSWORD", "");
        String chJdbcUrl = System.getenv().getOrDefault("CLICKHOUSE_JDBC_URL", "jdbc:clickhouse://clickhouse:8123");
        String chDatabase = System.getenv().getOrDefault("CLICKHOUSE_DATABASE", "analytics");
        String chTable = System.getenv().getOrDefault("CLICKHOUSE_TABLE", "enriched_events");
        String chUser = System.getenv().getOrDefault("CLICKHOUSE_USER", "default");
        String chPassword = System.getenv().getOrDefault("CLICKHOUSE_PASSWORD", "");

        LOG.info("Starting streaming job - Kafka: {}, Redis: {}:{}, ClickHouse: {}/{}", 
                kafkaBrokers, redisHost, redisPort, chDatabase, chTable);

        // kafka source 
        KafkaSource<String> engagementSource = KafkaSource.<String>builder()
                .setBootstrapServers(kafkaBrokers)
                .setTopics("cdc.public.engagement_events")
                .setGroupId("flink-enhanced-engagement-consumer")
                .setStartingOffsets(OffsetsInitializer.latest())
                .setValueOnlyDeserializer(new SimpleStringSchema())
                .setProperty("auto.offset.reset", "latest")
                .setProperty("enable.auto.commit", "false")
                .build();

        DataStream<String> engagementRaw = env.fromSource(
                engagementSource,
                WatermarkStrategy.forBoundedOutOfOrderness(Duration.ofSeconds(30)),
                "Engagement Source"
        );

        // Kafka source for content table 
        KafkaSource<String> contentSource = KafkaSource.<String>builder()
                .setBootstrapServers(kafkaBrokers)
                .setTopics("cdc.public.content")
                .setGroupId("flink-content-broadcast-consumer")
                .setStartingOffsets(OffsetsInitializer.latest())
                .setValueOnlyDeserializer(new SimpleStringSchema())
                .setProperty("auto.offset.reset", "latest")
                .setProperty("enable.auto.commit", "false")
                .build();

        DataStream<String> contentRaw = env.fromSource(
                contentSource,
                WatermarkStrategy.noWatermarks(),
                "Content Source"
        );

        // parse and broadcast 
        DataStream<ContentInfo> contentUpdates = contentRaw
                .map(new ContentMessageParser())
                .filter(ci -> ci != null);

        BroadcastStream<ContentInfo> contentBroadcast = contentUpdates.broadcast(CONTENT_STATE_DESC);

        // parse
        DataStream<EngagementEvent> engagementEvents = engagementRaw
                .map(new DebeziumMessageParser())
                .filter(event -> event != null);

        BroadcastConnectedStream<EngagementEvent, ContentInfo> connected = engagementEvents.connect(contentBroadcast);
        DataStream<EngagementEvent> enrichedStream = connected.process(new BroadcastContentEnricher());

        enrichedStream.addSink(new RedisSink(redisHost, redisPort, redisPassword))
                .name("Redis Sink")
                .setParallelism(2);

        enrichedStream.addSink(new ClickHouseSink(chJdbcUrl, chDatabase, chTable, chUser, chPassword))
                .name("ClickHouse Sink")
                .setParallelism(1);

        KafkaSink<String> externalSink = KafkaSink.<String>builder()
                .setBootstrapServers(kafkaBrokers)
                .setRecordSerializer(
                        KafkaRecordSerializationSchema.builder()
                                .setTopic("external.enriched_events")
                                .setValueSerializationSchema(new SimpleStringSchema())
                                .build()
                )
                .setDeliverGuarantee(DeliveryGuarantee.AT_LEAST_ONCE)
                .setTransactionalIdPrefix("ext-sink")
                .build();

        enrichedStream
                .map(event -> {
                    ObjectNode json = toJson(event);
                    return objectMapper.writeValueAsString(json);
                })
                .sinkTo(externalSink)
                .name("External Sink");

        enrichedStream.print("Processed Event");

        env.execute("Enhanced Engagement Streaming Job");
    }

    public static class DebeziumMessageParser implements MapFunction<String, EngagementEvent> {
        @Override
        public EngagementEvent map(String value) throws Exception {
            try {
                JsonNode rootNode = objectMapper.readTree(value);

                JsonNode recordNode = null;
                if (rootNode.has("payload") && rootNode.get("payload").has("after")) {
                    recordNode = rootNode.get("payload").get("after");
                } else {
                    recordNode = rootNode;
                }

                if (recordNode == null || recordNode.isNull()) {
                    return null;
                }

                EngagementEvent event = new EngagementEvent();

                if (recordNode.has("id") && !recordNode.get("id").isNull()) {
                    event.setEventId(recordNode.get("id").asText());
                }
                if (recordNode.has("user_id") && !recordNode.get("user_id").isNull()) {
                    event.setUserId(recordNode.get("user_id").asText());
                }
                if (recordNode.has("content_id") && !recordNode.get("content_id").isNull()) {
                    event.setContentId(recordNode.get("content_id").asText());
                }
                if (recordNode.has("event_type") && !recordNode.get("event_type").isNull()) {
                    event.setEventType(recordNode.get("event_type").asText());
                }
                if (recordNode.has("device") && !recordNode.get("device").isNull()) {
                    event.setPlatform(recordNode.get("device").asText());
                }
                if (recordNode.has("duration_ms") && !recordNode.get("duration_ms").isNull()) {
                    event.setDuration(recordNode.get("duration_ms").asLong());
                }
                if (recordNode.has("event_ts") && !recordNode.get("event_ts").isNull()) {
                    String ts = recordNode.get("event_ts").asText();
                    event.setTimestamp(parseTimestamp(ts));
                }

                return event;
            } catch (Exception e) {
                LOG.warn("Failed to parse message: {}", e.getMessage());
                return null;
            }
        }

        private LocalDateTime parseTimestamp(String timestampStr) {
            if (timestampStr == null || timestampStr.isEmpty()) {
                return null;
            }

            // Try different timestamp formats
            try {
                return OffsetDateTime.parse(timestampStr).toLocalDateTime();
            } catch (DateTimeParseException ignored) { }

            try {
                DateTimeFormatter f = DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss.SSSSSS");
                return LocalDateTime.parse(timestampStr, f);
            } catch (DateTimeParseException ignored) { }

            try {
                DateTimeFormatter f = DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss.SSS");
                return LocalDateTime.parse(timestampStr, f);
            } catch (DateTimeParseException ignored) { }

            try {
                DateTimeFormatter f = DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss");
                return LocalDateTime.parse(timestampStr, f);
            } catch (DateTimeParseException e) {
                LOG.warn("Unknown timestamp format: {}", timestampStr);
                return null;
            }
        }
    }

    public static class ContentMessageParser implements MapFunction<String, ContentInfo> {
        @Override
        public ContentInfo map(String value) throws Exception {
            try {
                JsonNode rootNode = objectMapper.readTree(value);
                JsonNode after = null;
                JsonNode opNode = null;

                if (rootNode.has("payload")) {
                    JsonNode payload = rootNode.get("payload");
                    opNode = payload.get("op");
                    if (payload.has("after") && !payload.get("after").isNull()) {
                        after = payload.get("after");
                    } else if (payload.has("before") && (opNode != null && (opNode.asText().equals("d") || opNode.asText().equals("D")))) {
                        // deletion, remove from state
                        JsonNode before = payload.get("before");
                        if (before != null && before.has("id") && !before.get("id").isNull()) {
                            return new ContentInfo(before.get("id").asText(), null, null, true);
                        }
                        return null;
                    }
                } else {
                    after = rootNode;
                }

                if (after == null || after.isNull()) {
                    return null;
                }

                String id = after.has("id") && !after.get("id").isNull() ? after.get("id").asText() : null;
                String contentType = after.has("content_type") && !after.get("content_type").isNull() ? after.get("content_type").asText() : null;
                Integer lengthSeconds = after.has("length_seconds") && !after.get("length_seconds").isNull() ? after.get("length_seconds").asInt() : null;

                if (id == null) return null;
                return new ContentInfo(id, contentType, lengthSeconds, false);
            } catch (Exception e) {
                LOG.warn("Failed to parse content message: {}", e.getMessage());
                return null;
            }
        }
    }

    // Broadcast enrichment joining engagement with content
    public static class BroadcastContentEnricher extends BroadcastProcessFunction<EngagementEvent, ContentInfo, EngagementEvent> {
        @Override
        public void processElement(EngagementEvent event, ReadOnlyContext ctx, Collector<EngagementEvent> out) throws Exception {
            if (event == null) return;
            if (event.getContentId() != null) {
                ReadOnlyBroadcastState<String, ContentInfo> state = ctx.getBroadcastState(CONTENT_STATE_DESC);
                ContentInfo info = state.get(event.getContentId());
                if (info != null && !info.isDeletion()) {
                    event.setContentType(info.getContentType());
                    event.setLengthSeconds(info.getLengthSeconds());
                }
            }

            if (event.getDuration() != null) {
                event.setEngagementSeconds(event.getDuration() / 1000.0);
            }

            if (event.getLengthSeconds() != null && event.getDuration() != null && event.getLengthSeconds() > 0) {
                double pct = (event.getDuration() / 1000.0) / event.getLengthSeconds();
                event.setEngagementPct(Math.round(pct * 10000.0) / 100.0); // two decimals, percentage
            } else {
                event.setEngagementPct(null);
            }

            double score = calculateScore(event);
            event.setEngagementScore(score);

            out.collect(event);
        }

        @Override
        public void processBroadcastElement(ContentInfo value, Context ctx, Collector<EngagementEvent> out) throws Exception {
            if (value == null || value.getId() == null) return;
            var state = ctx.getBroadcastState(CONTENT_STATE_DESC);
            if (value.isDeletion()) {
                state.remove(value.getId());
            } else {
                state.put(value.getId(), value);
            }
        }

        private double calculateScore(EngagementEvent event) {
            double baseScore = 1.0;

            if (event.getEventType() != null) {
                switch (event.getEventType().toLowerCase()) {
                    case "play": baseScore = 1.0; break;
                    case "pause": baseScore = 0.5; break;
                    case "finish": baseScore = 3.0; break;
                    case "click": baseScore = 0.2; break;
                }
            }

            if (event.getDuration() != null && event.getDuration() > 0) {
                baseScore *= Math.min(2.5, Math.log(event.getDuration() / 1000.0 + 1));
            }

            return Math.round(baseScore * 100.0) / 100.0;
        }
    }

    public static class ContentInfo implements Serializable {
        private String id;
        private String contentType;
        private Integer lengthSeconds;
        private boolean deletion;

        public ContentInfo() {}

        public ContentInfo(String id, String contentType, Integer lengthSeconds, boolean deletion) {
            this.id = id;
            this.contentType = contentType;
            this.lengthSeconds = lengthSeconds;
            this.deletion = deletion;
        }

        public String getId() { return id; }
        public String getContentType() { return contentType; }
        public Integer getLengthSeconds() { return lengthSeconds; }
        public boolean isDeletion() { return deletion; }

        public void setId(String id) { this.id = id; }
        public void setContentType(String contentType) { this.contentType = contentType; }
        public void setLengthSeconds(Integer lengthSeconds) { this.lengthSeconds = lengthSeconds; }
        public void setDeletion(boolean deletion) { this.deletion = deletion; }
    }

    private static ObjectNode toJson(EngagementEvent event) {
        ObjectNode jsonEvent = objectMapper.createObjectNode();
        if (event.getEventId() != null) jsonEvent.put("event_id", event.getEventId());
        if (event.getUserId() != null) jsonEvent.put("user_id", event.getUserId());
        if (event.getContentId() != null) jsonEvent.put("content_id", event.getContentId());
        if (event.getEventType() != null) jsonEvent.put("event_type", event.getEventType());
        if (event.getPlatform() != null) jsonEvent.put("platform", event.getPlatform());
        if (event.getTimestamp() != null) jsonEvent.put("event_ts", event.getTimestamp().toString());
        if (event.getDuration() != null) jsonEvent.put("duration_ms", event.getDuration());
        if (event.getEngagementScore() != null) jsonEvent.put("engagement_score", event.getEngagementScore());
        if (event.getContentType() != null) jsonEvent.put("content_type", event.getContentType());
        if (event.getLengthSeconds() != null) jsonEvent.put("length_seconds", event.getLengthSeconds());
        if (event.getEngagementSeconds() != null) jsonEvent.put("engagement_seconds", event.getEngagementSeconds());
        if (event.getEngagementPct() != null) jsonEvent.put("engagement_pct", event.getEngagementPct());
        return jsonEvent;
    }

    public static class RedisSink implements SinkFunction<EngagementEvent> {
        private final String redisHost;
        private final int redisPort;
        private final String redisPassword;
        private transient JedisPool jedisPool;

        public RedisSink(String redisHost, int redisPort, String redisPassword) {
            this.redisHost = redisHost;
            this.redisPort = redisPort;
            this.redisPassword = redisPassword;
        }

        @Override
        public void invoke(EngagementEvent event, Context context) throws Exception {
            if (jedisPool == null) {
                JedisPoolConfig config = new JedisPoolConfig();
                config.setMaxTotal(20);
                config.setMaxIdle(10);
                config.setMinIdle(2);
                config.setTestOnBorrow(true);
                
                if (redisPassword != null && !redisPassword.isEmpty()) {
                    jedisPool = new JedisPool(config, redisHost, redisPort, 2000, redisPassword);
                } else {
                    jedisPool = new JedisPool(config, redisHost, redisPort);
                }
            }

            try (Jedis jedis = jedisPool.getResource()) {
                String key = "engagement:" + event.getUserId() + ":" + event.getContentId();
                
                ObjectNode jsonEvent = toJson(event);
                jedis.setex(key, 3600, jsonEvent.toString());
                
                String userScoreKey = "user_engagement:" + event.getUserId();
                if (event.getEngagementScore() != null) {
                    jedis.zincrby(userScoreKey, event.getEngagementScore(), event.getContentId());
                    jedis.expire(userScoreKey, 86400);
                }
                
                // Update content scores
                String contentScoreKey = "content_engagement:" + event.getContentId();
                if (event.getEngagementScore() != null) {
                    jedis.zincrby(contentScoreKey, event.getEngagementScore(), event.getUserId());
                    jedis.expire(contentScoreKey, 86400);
                }
                
                // Time-based aggregation (minute buckets)
                if (event.getTimestamp() != null && event.getEngagementScore() != null) {
                    String minuteBucket = event.getTimestamp().format(DateTimeFormatter.ofPattern("yyyyMMddHHmm"));
                    String topContentKey = "top_content:bucket:" + minuteBucket;
                    jedis.zincrby(topContentKey, event.getEngagementScore(), event.getContentId());
                    jedis.expire(topContentKey, 15 * 60);
                }
                
                // Event counters
                String counterKey = "event_counters:" + event.getEventType();
                jedis.incr(counterKey);
                jedis.expire(counterKey, 3600);
                
            } catch (Exception e) {
                LOG.error("Redis write failed: {}", e.getMessage());
            }
        }
    }

    public static class ClickHouseSink extends org.apache.flink.streaming.api.functions.sink.RichSinkFunction<EngagementEvent> {
        private final String jdbcUrl;
        private final String database;
        private final String table;
        private final String username;
        private final String password;
        private transient java.sql.Connection connection;
        private transient java.sql.PreparedStatement statement;
        private transient int batchCount;
        private final int batchSize = 500;

        public ClickHouseSink(String jdbcUrl, String database, String table, String username, String password) {
            this.jdbcUrl = jdbcUrl;
            this.database = database;
            this.table = table;
            this.username = username;
            this.password = password;
        }

        @Override
        public void open(org.apache.flink.configuration.Configuration parameters) throws Exception {
            super.open(parameters);
            
            Class.forName("com.clickhouse.jdbc.ClickHouseDriver");
            String fullUrl = jdbcUrl + "/" + database;
            
            java.util.Properties props = new java.util.Properties();
            if (username != null && !username.isEmpty()) props.setProperty("user", username);
            if (password != null) props.setProperty("password", password);
            props.setProperty("socket_timeout", "600000");
            props.setProperty("max_execution_time", "600");

            int attempts = 0;
            while (attempts < 10) {
                try {
                    connection = java.sql.DriverManager.getConnection(fullUrl, props);
                    break;
                } catch (java.sql.SQLException se) {
                    attempts++;
                    if (attempts >= 10) {
                        LOG.error("Failed to connect to ClickHouse after {} attempts", attempts, se);
                        throw se;
                    }
                    LOG.warn("ClickHouse connect attempt {} failed, retrying...", attempts);
                    Thread.sleep(1000 * attempts);
                }
            }

            connection.setAutoCommit(false);

            String insertSQL = String.format(
                "INSERT INTO %s (event_id, user_id, content_id, event_type, device, event_ts, duration_ms, engagement_score, content_type, length_seconds, engagement_seconds, engagement_pct) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                table
            );

            statement = connection.prepareStatement(insertSQL);
            batchCount = 0;
            
            LOG.info("ClickHouse sink initialized");
        }

        private void flushBatch() throws Exception {
            if (batchCount > 0) {
                statement.executeBatch();
                connection.commit();
                batchCount = 0;
            }
        }

        @Override
        public void invoke(EngagementEvent event, Context context) throws Exception {
            try {
                if (event.getEventId() != null) {
                    try {
                        statement.setLong(1, Long.parseLong(event.getEventId()));
                    } catch (NumberFormatException e) {
                        statement.setLong(1, Math.abs(event.getEventId().hashCode()));
                    }
                } else {
                    statement.setNull(1, java.sql.Types.BIGINT);
                }
                
                statement.setString(2, event.getUserId());
                statement.setString(3, event.getContentId());
                statement.setString(4, event.getEventType());
                statement.setString(5, event.getPlatform());
                
                if (event.getTimestamp() != null) {
                    statement.setTimestamp(6, java.sql.Timestamp.valueOf(event.getTimestamp()));
                } else {
                    statement.setTimestamp(6, new java.sql.Timestamp(System.currentTimeMillis()));
                }
                
                if (event.getDuration() != null) {
                    statement.setLong(7, event.getDuration());
                } else {
                    statement.setNull(7, java.sql.Types.INTEGER);
                }
                
                if (event.getEngagementScore() != null) {
                    statement.setDouble(8, event.getEngagementScore());
                } else {
                    statement.setNull(8, java.sql.Types.DOUBLE);
                }
                
                if (event.getContentType() != null) {
                    statement.setString(9, event.getContentType());
                } else {
                    statement.setNull(9, java.sql.Types.VARCHAR);
                }
                
                if (event.getLengthSeconds() != null) {
                    statement.setInt(10, event.getLengthSeconds());
                } else {
                    statement.setNull(10, java.sql.Types.INTEGER);
                }
                
                if (event.getEngagementSeconds() != null) {
                    statement.setDouble(11, event.getEngagementSeconds());
                } else {
                    statement.setNull(11, java.sql.Types.DOUBLE);
                }
                
                if (event.getEngagementPct() != null) {
                    statement.setDouble(12, event.getEngagementPct());
                } else {
                    statement.setNull(12, java.sql.Types.DOUBLE);
                }
                
                statement.addBatch();
                batchCount++;
                
                if (batchCount >= batchSize) {
                    flushBatch();
                }
                
            } catch (Exception e) {
                LOG.error("ClickHouse insert failed: {}", e.getMessage());
                connection.rollback();
                throw e;
            }
        }

        @Override
        public void close() throws Exception {
            flushBatch();
            if (statement != null) statement.close();
            if (connection != null) connection.close();
            super.close();
        }
    }
} 