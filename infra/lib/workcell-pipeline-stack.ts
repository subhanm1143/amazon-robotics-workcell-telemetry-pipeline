import { Stack, StackProps, RemovalPolicy, CfnOutput } from "aws-cdk-lib";
import { Construct } from "constructs";
import * as kinesis from "aws-cdk-lib/aws-kinesis";
import * as s3 from "aws-cdk-lib/aws-s3";

/**
 * The same two Kinesis streams and the lake bucket that scripts/bootstrap.sh
 * created by hand against LocalStack, now as CDK — the posting names CDK as
 * how Cosmos actually deploys this. `cdk synth` renders the CloudFormation
 * template from this file with no AWS credentials at all; only `cdk deploy`
 * would need a real account, and this project never asks you to run it.
 */
export class WorkcellPipelineStack extends Stack {
  constructor(scope: Construct, id: string, props?: StackProps) {
    super(scope, id, props);

    const events = new kinesis.Stream(this, "WorkcellEvents", {
      streamName: "workcell-events",
      shardCount: 2,
    });

    const deadLetter = new kinesis.Stream(this, "WorkcellEventsDlq", {
      streamName: "workcell-events-dlq",
      shardCount: 1,
    });

    const lake = new s3.Bucket(this, "CosmosLake", {
      bucketName: "cosmos-lake",
      removalPolicy: RemovalPolicy.RETAIN,
      // Iceberg's own snapshots handle table history; bucket versioning here
      // protects only against a manifest object being overwritten out from
      // under a reader mid-read — a narrower, different guarantee.
      versioned: true,
    });

    new CfnOutput(this, "EventsStreamName", { value: events.streamName });
    new CfnOutput(this, "DeadLetterStreamName", { value: deadLetter.streamName });
    new CfnOutput(this, "LakeBucketName", { value: lake.bucketName });
  }
}
