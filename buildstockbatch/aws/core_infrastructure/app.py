#!/usr/bin/env python3
import os

import aws_cdk as cdk

from core_infrastructure.vpc import BuildStockVpc
from core_infrastructure.batch import BatchEC2Stack


class BuildStockStack(cdk.Stack):
    def __init__(
        self,
    ):
        build_stock_vpc = BuildStockVpc(self, "buildstock-vpc")
        batch_ec2_stack = BatchEC2Stack(self, "buildstick-batch-ec2", build_stock_vpc)


app = cdk.App()

app.synth()
