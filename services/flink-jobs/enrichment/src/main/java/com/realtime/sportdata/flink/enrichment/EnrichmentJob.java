/*
 * Flink Enrichment Job — Main Entry Point
 *
 * Topology: match-events-raw → Enrichment → match-events-enriched
 *
 * Responsibilities:
 * - Join with player DB (async I/O to Redis)
 * - Maintain running match state (Flink keyed state)
 * - Compute derived fields (xG, score)
 * - Idempotent deduplication (event_id in state)
 *
 * From the architecture: "True event-at-a-time processing with first-class
 * support for event time, watermarks, and complex event processing."
 */

package com.realtime.sportdata.flink.enrichment;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.AsyncDataStream;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;

import java.time.Duration;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

public class EnrichmentJob {

    public static void main(String[] args) throws Exception {
        // --- Environment Setup ---
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // Enable exactly-once checkpointing every 30 seconds
        env.enableCheckpointing(30_000);
        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(10_000);
        env.getCheckpointConfig().setCheckpointTimeout(60_000);

        // --- Kafka Consumer (Source) ---
        Properties kafkaConsumerProps = new Properties();
        kafkaConsumerProps.setProperty("bootstrap.servers", "kafka-1:9092,kafka-2:9092,kafka-3:9092");
        kafkaConsumerProps.setProperty("group.id", "flink-enrichment-group");
        kafkaConsumerProps.setProperty("auto.offset.reset", "earliest");
        kafkaConsumerProps.setProperty("enable.auto.commit", "false");

        // Read raw events from Kafka
        DataStream<RawMatchEvent> rawEvents = env
            .addSource(new FlinkKafkaConsumer<>(
                "match-events-raw",
                new RawMatchEventSchema(),
                kafkaConsumerProps
            ))
            .name("kafka-source-raw-events")

            // Assign event-time timestamps with 3-second out-of-orderness tolerance
            // This is critical: we use occurred_at (the physical event time on the pitch)
            // not received_at or published_at
            .assignTimestampsAndWatermarks(
                WatermarkStrategy
                    .<RawMatchEvent>forBoundedOutOfOrderness(Duration.ofSeconds(3))
                    .withTimestampAssigner((event, ts) -> event.getOccurredAt())
            );

        // --- Step 1: Async Enrichment from Redis ---
        // Look up player names, team logos, photos etc.
        // Non-blocking: up to 100 concurrent requests with 5-second timeout
        DataStream<EnrichedMatchEvent> enriched = AsyncDataStream.unorderedWait(
            rawEvents,
            new RedisPlayerLookup(),
            5000, TimeUnit.MILLISECONDS,
            100  // max concurrent async requests
        ).name("async-redis-player-lookup");

        // --- Step 2: Stateful Match State Tracking ---
        // Maintains running score, cards, substitutions in Flink keyed state
        // Uses RocksDB backend for large state, checkpointed to S3
        DataStream<EnrichedMatchEvent> withMatchState = enriched
            .keyBy(event -> event.getMatchId())
            .process(new MatchStateTracker())
            .name("match-state-tracker");

        // --- Step 3: Idempotent Deduplication ---
        // Uses event_id stored in Flink state to detect and drop duplicates
        DataStream<EnrichedMatchEvent> deduplicated = withMatchState
            .keyBy(event -> event.getMatchId())
            .process(new IdempotentDeduplicator())
            .name("idempotent-deduplicator");

        // --- Kafka Producer (Sink) ---
        Properties kafkaProducerProps = new Properties();
        kafkaProducerProps.setProperty("bootstrap.servers", "kafka-1:9092,kafka-2:9092,kafka-3:9092");
        kafkaProducerProps.setProperty("transaction.timeout.ms", "60000");

        deduplicated.addSink(
            new FlinkKafkaProducer<>(
                "match-events-enriched",
                new EnrichedMatchEventSchema(),
                kafkaProducerProps,
                FlinkKafkaProducer.Semantic.EXACTLY_ONCE
            )
        ).name("kafka-sink-enriched-events");

        // --- Execute ---
        env.execute("Match Event Enrichment Pipeline");
    }
}
