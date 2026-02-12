/*
 * Analytics Aggregator Job — Flink
 *
 * Topology: match-events-enriched → Windowed Aggregation → match-events-analytics → ClickHouse
 *
 * Responsibilities:
 * - 1-minute, 5-minute, and match-level aggregation windows
 * - Possession %, shot maps, running xG, heat maps
 * - Produces pre-computed aggregations to analytics topic
 * - ClickHouse ingests directly via Kafka Engine
 */

package com.realtime.sportdata.flink.analytics;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;

import java.time.Duration;
import java.util.Properties;

public class AnalyticsAggregatorJob {

    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.enableCheckpointing(30_000);

        Properties kafkaProps = new Properties();
        kafkaProps.setProperty("bootstrap.servers", "kafka-1:9092,kafka-2:9092,kafka-3:9092");
        kafkaProps.setProperty("group.id", "flink-analytics-aggregator");

        DataStream<EnrichedMatchEvent> events = env
            .addSource(new FlinkKafkaConsumer<>(
                "match-events-enriched",
                new EnrichedMatchEventSchema(),
                kafkaProps
            ))
            .name("kafka-source-enriched-events")
            .assignTimestampsAndWatermarks(
                WatermarkStrategy
                    .<EnrichedMatchEvent>forBoundedOutOfOrderness(Duration.ofSeconds(5))
                    .withTimestampAssigner((event, ts) -> event.getOccurredAt())
            );

        // --- 1-Minute Window Aggregations ---
        // Per-match statistics updated every minute
        DataStream<MatchAnalytics> oneMinuteAgg = events
            .keyBy(event -> event.getMatchId())
            .window(TumblingEventTimeWindows.of(Time.minutes(1)))
            .process(new MatchAnalyticsWindowFunction())
            .name("1-minute-aggregation");

        // --- 5-Minute Window Aggregations ---
        // Trend analysis: possession shifts, momentum changes
        DataStream<MatchAnalytics> fiveMinuteAgg = events
            .keyBy(event -> event.getMatchId())
            .window(TumblingEventTimeWindows.of(Time.minutes(5)))
            .process(new TrendAnalyticsWindowFunction())
            .name("5-minute-trend-aggregation");

        // --- Cumulative xG Tracking ---
        // Running xG total per team, updated on every shot/goal event
        DataStream<XGUpdate> xgStream = events
            .filter(event ->
                "GOAL_SCORED".equals(event.getEventType()) ||
                "PENALTY_MISSED".equals(event.getEventType())
            )
            .keyBy(event -> event.getMatchId())
            .process(new CumulativeXGTracker())
            .name("cumulative-xg-tracker");

        // --- Produce to Analytics Topic ---
        Properties producerProps = new Properties();
        producerProps.setProperty("bootstrap.servers", "kafka-1:9092,kafka-2:9092,kafka-3:9092");

        oneMinuteAgg.addSink(
            new FlinkKafkaProducer<>(
                "match-events-analytics",
                new MatchAnalyticsSchema(),
                producerProps,
                FlinkKafkaProducer.Semantic.AT_LEAST_ONCE
            )
        ).name("kafka-sink-1min-analytics");

        fiveMinuteAgg.addSink(
            new FlinkKafkaProducer<>(
                "match-events-analytics",
                new MatchAnalyticsSchema(),
                producerProps,
                FlinkKafkaProducer.Semantic.AT_LEAST_ONCE
            )
        ).name("kafka-sink-5min-analytics");

        xgStream.addSink(
            new FlinkKafkaProducer<>(
                "match-events-analytics",
                new XGUpdateSchema(),
                producerProps,
                FlinkKafkaProducer.Semantic.AT_LEAST_ONCE
            )
        ).name("kafka-sink-xg-updates");

        env.execute("Analytics Aggregation Pipeline");
    }
}
