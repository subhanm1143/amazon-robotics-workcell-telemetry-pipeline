#!/usr/bin/env node
import { App } from "aws-cdk-lib";
import { WorkcellPipelineStack } from "../lib/workcell-pipeline-stack";

const app = new App();
new WorkcellPipelineStack(app, "CosmosWorkcellPipeline");
