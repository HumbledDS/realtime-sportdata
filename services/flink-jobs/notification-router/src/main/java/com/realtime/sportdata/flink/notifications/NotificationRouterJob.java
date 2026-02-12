/*
 * Notification Router — Flink Job
 *
 * Topology: match-events-enriched → Notification Routing → match-events-notifications
 *
 * Responsibilities:
 * 1. Segment users (PSG fans, Ligue 1 followers, fantasy players)
 * 2. Priority routing (GOAL=high, CARD=normal, SUBSTITUTION=silent)
 * 3. Multi-language templating (FR, EN, AR)
 * 4. Rate limiting (max 10 notifications/match/user)
 * 5. Produce to notifications topic partitioned by user_segment_hash
 */

package com.realtime.sportdata.flink.notifications;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;

import java.time.Duration;
import java.util.Properties;

public class NotificationRouterJob {

    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.enableCheckpointing(30_000);

        Properties kafkaProps = new Properties();
        kafkaProps.setProperty("bootstrap.servers", "kafka-1:9092,kafka-2:9092,kafka-3:9092");
        kafkaProps.setProperty("group.id", "flink-notification-router");

        // Read enriched events
        DataStream<EnrichedMatchEvent> enrichedEvents = env
            .addSource(new FlinkKafkaConsumer<>(
                "match-events-enriched",
                new EnrichedMatchEventSchema(),
                kafkaProps
            ))
            .name("kafka-source-enriched-events")
            .assignTimestampsAndWatermarks(
                WatermarkStrategy
                    .<EnrichedMatchEvent>forBoundedOutOfOrderness(Duration.ofSeconds(2))
                    .withTimestampAssigner((event, ts) -> event.getOccurredAt())
            );

        // Filter to notification-worthy events only
        DataStream<EnrichedMatchEvent> notifiableEvents = enrichedEvents
            .filter(event -> isNotifiable(event.getEventType()))
            .name("filter-notifiable-events");

        // Route and segment
        DataStream<NotificationPayload> notifications = notifiableEvents
            .keyBy(event -> event.getMatchId())
            .process(new NotificationSegmenter())
            .name("notification-segmenter");

        // Rate limit per user
        DataStream<NotificationPayload> rateLimited = notifications
            .keyBy(notif -> notif.getUserSegmentHash())
            .process(new RateLimiter(10)) // Max 10 per match per user
            .name("rate-limiter");

        // Produce to notifications topic
        Properties producerProps = new Properties();
        producerProps.setProperty("bootstrap.servers", "kafka-1:9092,kafka-2:9092,kafka-3:9092");

        rateLimited.addSink(
            new FlinkKafkaProducer<>(
                "match-events-notifications",
                new NotificationPayloadSchema(),
                producerProps,
                FlinkKafkaProducer.Semantic.AT_LEAST_ONCE  // Duplicate notifications OK, missing not OK
            )
        ).name("kafka-sink-notifications");

        env.execute("Notification Router Pipeline");
    }

    /**
     * Filter: only GOAL, RED_CARD, PENALTY, FULL_TIME events trigger notifications.
     * Other events (corners, free kicks) are too frequent and low-impact.
     */
    private static boolean isNotifiable(String eventType) {
        return switch (eventType) {
            case "GOAL_SCORED" -> true;
            case "GOAL_DISALLOWED" -> true;
            case "RED_CARD" -> true;
            case "PENALTY_AWARDED" -> true;
            case "FULL_TIME" -> true;
            case "HALF_TIME" -> true;
            case "VAR_DECISION" -> true;
            default -> false;
        };
    }
}
