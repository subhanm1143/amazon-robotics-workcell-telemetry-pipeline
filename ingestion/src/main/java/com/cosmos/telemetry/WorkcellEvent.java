package com.cosmos.telemetry;

import java.time.Instant;
import java.util.Map;
import java.util.UUID;

/**
 * The event shape robotic workcells actually emit: a status heartbeat or a
 * fault, tagged with the workcell id the whole pipeline partitions on.
 *
 * occurredAtMillis is epoch millis, not an ISO-8601 string. Flink's SQL
 * TO_TIMESTAMP_LTZ takes a bigint directly; parsing a timezone-suffixed
 * string back into an event-time column is friction every downstream step
 * would otherwise pay for something the producer can fix once.
 *
 * The partition key downstream is workcellId, not a random id, because the
 * one ordering guarantee Kinesis gives you is per-shard: records sharing a
 * partition key hash to the same shard, in order. Two events from the same
 * workcell must never be reordered relative to each other; two events from
 * different workcells are free to interleave.
 */
public record WorkcellEvent(
    String eventId,
    String workcellId,
    String workcellType,
    String eventType,      // "status" | "fault"
    String status,          // "idle" | "picking" | "sorting" | "faulted"
    long occurredAtMillis,
    Map<String, Object> attributes
) {
    public static WorkcellEvent statusEvent(String workcellId, String workcellType, String status) {
        return new WorkcellEvent(
            UUID.randomUUID().toString(),
            workcellId,
            workcellType,
            "status",
            status,
            Instant.now().toEpochMilli(),
            Map.of("throughputPerMin", (int) (Math.random() * 40))
        );
    }

    public static WorkcellEvent faultEvent(String workcellId, String workcellType, String code) {
        return new WorkcellEvent(
            UUID.randomUUID().toString(),
            workcellId,
            workcellType,
            "fault",
            "faulted",
            Instant.now().toEpochMilli(),
            Map.of("faultCode", code)
        );
    }
}
