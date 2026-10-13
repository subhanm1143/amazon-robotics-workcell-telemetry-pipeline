# Amazon Robotics | Workcell Telemetry Pipeline: Kinesis to Flink to a Queryable Lake

An advanced data-engineering capstone modeled on the Cosmos team inside Amazon Robotics, whose Software Development Engineer I posting (Westboro, MA) actually asks for real-time pipelines in Kinesis and Flink, a data lake on S3 and Iceberg queried through Athena, and services shipped via CDK — a bar closer to an SDE II than the title suggests. You build the exact loop the posting names. A Java ingestion service emits status and fault events from a simulated workcell fleet onto a Kinesis stream, partitioned by workcell id so Kinesis's one ordering guarantee — per shard — actually holds. A Flink job validates every event against a schema and routes anything malformed to a dead-letter stream instead of dropping it. A second Flink job folds the valid stream into 1-minute tumbling windows of utilization and fault rate, on event time, tolerant of late arrivals. Both land in Iceberg tables on S3, partitioned by workcell type first and day second, because "which workcell type faults most" is the question the whole pipeline exists to answer. A DuckDB layer queries that lake the way Athena would. You close the loop with a dead-letter-rate alarm that pages on a rate crossing a threshold rather than a raw count, replay a real incident end to end — robotics engineering ships a new event type nobody announced — and deploy the pipeline's infrastructure as an AWS CDK app, then serve the lake's aggregates through a small TypeScript API, the other half of the posting's own Java/TypeScript split. No AWS account is required anywhere in this project: Kinesis and S3 run against LocalStack on your machine, and DuckDB stands in for Athena, which has no free local emulator — the same substitution the Iceberg prerequisite project already made.

Built step-by-step with [KhwajaLabs Build](https://khwajalabs.com).

## Stack
- Java
- Amazon Kinesis
- Apache Flink
- Apache Iceberg
- DuckDB
- AWS CDK
- TypeScript
- LocalStack
- Docker Compose
