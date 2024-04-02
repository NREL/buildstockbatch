#!/usr/bin/env python3
import os

import aws_cdk as cdk

from core_infrastructure.core_infrastructure_stack import CoreInfrastructureStack


app = cdk.App()
BuildStockBatch(
    app,
    "BuildStockBatch",
)

app.synth()
