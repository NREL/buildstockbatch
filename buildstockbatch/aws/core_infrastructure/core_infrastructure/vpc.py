from aws_cdk import CfnOutput, Stack
import aws_cdk.aws_ec2 as ec2
from constructs import Construct


class BuildStockVpc(Construct):
    def __init__(self, scope: Construct, id: str, **kwargs) -> None:

        super().__init__(scope, id, **kwargs)

        # The code that defines your stack goes here

        self.vpc = ec2.Vpc(
            self,
            "VPC",
            # nat_gateway_provider=ec2.NatProvider.gateway(),
            nat_gateways=1,
            max_azs=2,
            cidr="10.10.0.0/16",  # TO-DO: parameterize
            # configuration will create 2 groups in 2 AZs = 4 subnets.
            subnet_configuration=[
                ec2.SubnetConfiguration(subnet_type=ec2.SubnetType.PUBLIC, name="Public", cidr_mask=26),
                ec2.SubnetConfiguration(subnet_type=ec2.SubnetType.PRIVATE_WITH_NAT, name="Private", cidr_mask=23),
            ],
        )

        CfnOutput(self, "Output", value=self.vpc.vpc_id)
