#!/usr/bin/env bash
# Creates the two Kinesis streams and the lake bucket LocalStack needs, using
# the real AWS CLI pointed at LocalStack's endpoint instead of AWS. Nothing
# below this line would change if you swapped the endpoint for a real account.
set -euo pipefail
ENDPOINT=http://localhost:4566
export AWS_ACCESS_KEY_ID=test
export AWS_SECRET_ACCESS_KEY=test
export AWS_DEFAULT_REGION=us-east-1

aws --endpoint-url="$ENDPOINT" kinesis create-stream \
  --stream-name workcell-events --shard-count 2

aws --endpoint-url="$ENDPOINT" kinesis create-stream \
  --stream-name workcell-events-dlq --shard-count 1

aws --endpoint-url="$ENDPOINT" s3 mb s3://cosmos-lake

echo "streams + bucket ready"
