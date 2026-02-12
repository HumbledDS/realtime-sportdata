/*
 * Odds Trigger Job — Flink
 *
 * Topology: match-events-enriched → Filter & Prioritize → match-events-odds
 *
 * The most latency-sensitive and commercially critical consumer path.
 * When a goal is scored, betting markets MUST suspend within 50ms
 * before the event becomes public knowledge.
 *
 * Responsibilities:
 * - Filter critical events (goals, cards, penalties)
 * - Priority flagging for immediate market suspension
 * - Produce to odds topic partitioned by market_id
 */

package com.realtime.sportdata.flink.odds;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;

import java.time.Duration;
import java.util.Properties;

public class OddsTriggerJob {

    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.enableCheckpointing(30_000);

        // Optimize for minimum latency
        env.setBufferTimeout(1); // Flush buffers after 1ms

        Properties kafkaProps = new Properties();
        kafkaProps.setProperty("bootstrap.servers", "kafka-1:9092,kafka-2:9092,kafka-3:9092");
        kafkaProps.setProperty("group.id", "flink-odds-trigger");

        DataStream<EnrichedMatchEvent> enrichedEvents = env
            .addSource(new FlinkKafkaConsumer<>(
                "match-events-enriched",
                new EnrichedMatchEventSchema(),
                kafkaProps
            ))
            .name("kafka-source-enriched-events")
            .assignTimestampsAndWatermarks(
                WatermarkStrategy
                    .<EnrichedMatchEvent>forBoundedOutOfOrderness(Duration.ofSeconds(1))
                    .withTimestampAssigner((event, ts) -> event.getOccurredAt())
            );

        // Filter only odd-affecting events
        DataStream<EnrichedMatchEvent> oddsEvents = enrichedEvents
            .filter(event -> affectsOdds(event.getEventType()))
            .name("filter-odds-affecting-events");

        // Map to OddsTrigger with priority and affected markets
        DataStream<OddsTrigger> triggers = oddsEvents
            .map(event -> {
                OddsTrigger trigger = new OddsTrigger();
                trigger.setEventId(event.getEventId());
                trigger.setEventType(event.getEventType());
                trigger.setMatchId(event.getMatchId());
                trigger.setTimestamp(event.getOccurredAt());

                // Set priority based on event type
                trigger.setPriority(getOddsPriority(event.getEventType()));

                // Determine affected markets
                trigger.setAffectedMarkets(getAffectedMarkets(event.getEventType()));

                // CRITICAL: Market suspension flag
                // Goals and penalties require IMMEDIATE market suspension
                trigger.setSuspendMarkets(
                    "GOAL_SCORED".equals(event.getEventType()) ||
                    "RED_CARD".equals(event.getEventType()) ||
                    "PENALTY_AWARDED".equals(event.getEventType())
                );

                trigger.setEventData(event.getData());
                return trigger;
            })
            .name("map-to-odds-trigger");

        // Produce to odds topic with EXACTLY_ONCE — duplicate bet settlements are catastrophic
        Properties producerProps = new Properties();
        producerProps.setProperty("bootstrap.servers", "kafka-1:9092,kafka-2:9092,kafka-3:9092");
        producerProps.setProperty("transaction.timeout.ms", "60000");

        triggers.addSink(
            new FlinkKafkaProducer<>(
                "match-events-odds",
                new OddsTriggerSchema(),
                producerProps,
                FlinkKafkaProducer.Semantic.EXACTLY_ONCE
            )
        ).name("kafka-sink-odds-triggers");

        env.execute("Odds Trigger Pipeline");
    }

    private static boolean affectsOdds(String eventType) {
        return switch (eventType) {
            case "GOAL_SCORED", "GOAL_DISALLOWED" -> true;
            case "RED_CARD" -> true;
            case "PENALTY_AWARDED", "PENALTY_MISSED" -> true;
            case "VAR_DECISION" -> true;
            case "HALF_TIME", "FULL_TIME" -> true;
            case "SUBSTITUTION" -> true;
            default -> false;
        };
    }

    private static int getOddsPriority(String eventType) {
        return switch (eventType) {
            case "GOAL_SCORED" -> 1;      // CRITICAL — suspend immediately
            case "RED_CARD" -> 1;         // CRITICAL — major game state change
            case "PENALTY_AWARDED" -> 1;  // CRITICAL — high xG event
            case "GOAL_DISALLOWED" -> 2;  // HIGH — revert suspension
            case "VAR_DECISION" -> 2;     // HIGH — may change state
            case "PENALTY_MISSED" -> 3;   // MEDIUM
            case "FULL_TIME" -> 3;        // MEDIUM — settle all markets
            case "HALF_TIME" -> 4;        // LOW
            case "SUBSTITUTION" -> 5;     // MINOR — minor odds adjustment
            default -> 5;
        };
    }

    private static String[] getAffectedMarkets(String eventType) {
        return switch (eventType) {
            case "GOAL_SCORED" -> new String[]{
                "MATCH_RESULT", "OVER_UNDER", "CORRECT_SCORE",
                "NEXT_GOAL_SCORER", "BOTH_TEAMS_SCORE", "HANDICAP"
            };
            case "RED_CARD" -> new String[]{
                "MATCH_RESULT", "TOTAL_CARDS", "HANDICAP"
            };
            case "PENALTY_AWARDED" -> new String[]{
                "MATCH_RESULT", "NEXT_GOAL_SCORER", "CORRECT_SCORE"
            };
            case "FULL_TIME" -> new String[]{"ALL_MARKETS"};
            default -> new String[]{"MATCH_RESULT"};
        };
    }
}
