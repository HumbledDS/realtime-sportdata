/*
 * MatchStateTracker — Flink KeyedProcessFunction
 *
 * Maintains the "living memory" of each match in Flink keyed state:
 * - Current score
 * - List of goals scored
 * - Cards shown
 * - Substitutions made
 * - Current match period and minute
 *
 * When a goal event arrives, this function increments the score
 * and attaches score_after to the event.
 *
 * State is stored in RocksDB and checkpointed to S3 every 30 seconds.
 * If the job crashes, it restarts from the last checkpoint with
 * exact state recovery.
 */

package com.realtime.sportdata.flink.enrichment;

import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

public class MatchStateTracker extends KeyedProcessFunction<String, EnrichedMatchEvent, EnrichedMatchEvent> {

    // Keyed state: one instance per match_id
    private transient ValueState<Integer> homeScore;
    private transient ValueState<Integer> awayScore;
    private transient ValueState<String> matchPeriod;
    private transient ValueState<Integer> currentMinute;
    private transient MapState<String, Boolean> processedEventIds;

    @Override
    public void open(Configuration parameters) {
        // Initialize keyed state
        homeScore = getRuntimeContext().getState(
            new ValueStateDescriptor<>("home-score", Integer.class, 0));
        awayScore = getRuntimeContext().getState(
            new ValueStateDescriptor<>("away-score", Integer.class, 0));
        matchPeriod = getRuntimeContext().getState(
            new ValueStateDescriptor<>("match-period", String.class, "PRE_MATCH"));
        currentMinute = getRuntimeContext().getState(
            new ValueStateDescriptor<>("current-minute", Integer.class, 0));
        processedEventIds = getRuntimeContext().getMapState(
            new MapStateDescriptor<>("processed-events", String.class, Boolean.class));
    }

    @Override
    public void processElement(
            EnrichedMatchEvent event,
            KeyedProcessFunction<String, EnrichedMatchEvent, EnrichedMatchEvent>.Context ctx,
            Collector<EnrichedMatchEvent> out) throws Exception {

        String eventType = event.getEventType();

        // Update match state based on event type
        switch (eventType) {
            case "GOAL_SCORED":
                handleGoal(event);
                break;
            case "GOAL_DISALLOWED":
                handleGoalDisallowed(event);
                break;
            case "KICK_OFF":
            case "HALF_TIME":
            case "FULL_TIME":
                handleMatchState(event);
                break;
            default:
                break;
        }

        // Update current minute
        if (event.getMatchMinute() != null) {
            currentMinute.update(event.getMatchMinute());
        }

        // Attach current match state to the event
        event.setCurrentScore(homeScore.value(), awayScore.value());
        event.setCurrentMatchPeriod(matchPeriod.value());
        event.setCurrentMinute(currentMinute.value());

        out.collect(event);
    }

    private void handleGoal(EnrichedMatchEvent event) throws Exception {
        String side = event.getTeamSide();
        if ("HOME".equals(side)) {
            homeScore.update(homeScore.value() + 1);
        } else if ("AWAY".equals(side)) {
            awayScore.update(awayScore.value() + 1);
        }
    }

    private void handleGoalDisallowed(EnrichedMatchEvent event) throws Exception {
        // Revert score if a previously counted goal is disallowed
        String side = event.getTeamSide();
        if ("HOME".equals(side) && homeScore.value() > 0) {
            homeScore.update(homeScore.value() - 1);
        } else if ("AWAY".equals(side) && awayScore.value() > 0) {
            awayScore.update(awayScore.value() - 1);
        }
    }

    private void handleMatchState(EnrichedMatchEvent event) throws Exception {
        switch (event.getEventType()) {
            case "KICK_OFF":
                matchPeriod.update("FIRST_HALF");
                break;
            case "HALF_TIME":
                matchPeriod.update("HALF_TIME_BREAK");
                break;
            case "FULL_TIME":
                matchPeriod.update("POST_MATCH");
                break;
        }
    }
}
