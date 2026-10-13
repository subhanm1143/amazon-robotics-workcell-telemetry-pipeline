package com.cosmos.telemetry;

import com.fasterxml.jackson.databind.ObjectMapper;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.core.SdkBytes;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.kinesis.KinesisClient;
import software.amazon.awssdk.services.kinesis.model.PutRecordsRequest;
import software.amazon.awssdk.services.kinesis.model.PutRecordsRequestEntry;
import software.amazon.awssdk.services.kinesis.model.PutRecordsResponse;
import software.amazon.awssdk.services.kinesis.model.PutRecordsResultEntry;

import java.net.URI;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

/**
 * Simulates a fleet of workcells emitting status and fault events onto the
 * "workcell-events" Kinesis stream, batched through PutRecords the way a
 * real ingestion service would rather than one PutRecord call per event.
 */
public final class EventPublisher {
    private static final String STREAM = "workcell-events";
    private static final int FLEET_SIZE = 12;
    private static final int BATCH_SIZE = 25;
    private static final double MALFORMED_RATE = 0.03; // ~3% intentionally broken

    public static void main(String[] args) throws InterruptedException {
        KinesisClient kinesis = KinesisClient.builder()
            .endpointOverride(URI.create(
                System.getenv().getOrDefault("KINESIS_ENDPOINT", "http://localhost:4566")))
            .region(Region.US_EAST_1)
            .credentialsProvider(StaticCredentialsProvider.create(
                AwsBasicCredentials.create("test", "test")))
            .build();

        ObjectMapper mapper = new ObjectMapper();
        Random rng = new Random(42);
        List<String> workcellTypes = List.of("picker", "sorter", "packer", "palletizer");
        List<String> workcells = new ArrayList<>();
        for (int i = 0; i < FLEET_SIZE; i++) {
            workcells.add(String.format("wc-%03d", i));
        }

        int published = 0;
        while (true) {
            List<PutRecordsRequestEntry> entries = new ArrayList<>();
            for (int i = 0; i < BATCH_SIZE; i++) {
                String workcellId = workcells.get(rng.nextInt(workcells.size()));
                String type = workcellTypes.get(Math.abs(workcellId.hashCode()) % workcellTypes.size());
                boolean isFault = rng.nextDouble() < 0.08;
                WorkcellEvent event = isFault
                    ? WorkcellEvent.faultEvent(workcellId, type, "E" + (100 + rng.nextInt(9)))
                    : WorkcellEvent.statusEvent(workcellId, type, "picking");

                byte[] payload;
                try {
                    if (rng.nextDouble() < MALFORMED_RATE) {
                        // Deliberately break the payload: truncate the JSON so the
                        // Flink validator has something real to reject downstream.
                        String json = mapper.writeValueAsString(event);
                        payload = json.substring(0, json.length() / 2).getBytes();
                    } else {
                        payload = mapper.writeValueAsBytes(event);
                    }
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }

                entries.add(PutRecordsRequestEntry.builder()
                    .partitionKey(workcellId)
                    .data(SdkBytes.fromByteArray(payload))
                    .build());
            }

            PutRecordsResponse response = kinesis.putRecords(PutRecordsRequest.builder()
                .streamName(STREAM)
                .records(entries)
                .build());

            int failed = response.failedRecordCount();
            if (failed > 0) {
                // PutRecords is partial-failure by design: some records in a batch
                // can succeed while others hit a throttled shard. Only the failed
                // ones get retried, never the whole batch.
                retryFailed(kinesis, entries, response);
            }

            published += BATCH_SIZE - failed;
            System.out.printf("published %d events (%d failed-then-retried this batch)%n", published, failed);
            Thread.sleep(1000);
        }
    }

    private static void retryFailed(KinesisClient kinesis, List<PutRecordsRequestEntry> sent, PutRecordsResponse response) {
        List<PutRecordsRequestEntry> retry = new ArrayList<>();
        List<PutRecordsResultEntry> results = response.records();
        for (int i = 0; i < results.size(); i++) {
            if (results.get(i).errorCode() != null) {
                retry.add(sent.get(i));
            }
        }
        if (!retry.isEmpty()) {
            kinesis.putRecords(PutRecordsRequest.builder()
                .streamName(STREAM)
                .records(retry)
                .build());
        }
    }
}
