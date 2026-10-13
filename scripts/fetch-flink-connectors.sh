#!/usr/bin/env bash
# One-time download of the Kinesis SQL connector jar both Flink jobs need on
# their classpath. No AWS credentials involved — this is a Maven Central
# artifact, not an AWS API call.
set -euo pipefail
mkdir -p flink-jobs/lib
curl -sSL -o flink-jobs/lib/flink-sql-connector-aws-kinesis-streams-4.3.0-1.19.jar \
  "https://repo1.maven.org/maven2/org/apache/flink/flink-sql-connector-aws-kinesis-streams/4.3.0-1.19/flink-sql-connector-aws-kinesis-streams-4.3.0-1.19.jar"
echo "connector jar ready at flink-jobs/lib/"
