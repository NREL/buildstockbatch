from aws_cdk import aws_ec2 as ec2, aws_batch as batch, aws_ecs as ecs, App, Stack, CfnOutput, Size
from constructs import Construct
from vpc import BuildStockVpc


class BatchEC2Stack(Construct):
    def __init__(self, scope: Construct, id: str, bstkvpc: BuildStockVpc, maxv_cpus: int = 10000, **kwargs) -> None:
        super().__init__(scope, id, **kwargs)

        batch.ManagedEc2EcsComputeEnvironment(
            self,
            f"{bstkvpc.id}-batch-compute-environment",
            spot=True,
            spot_bid_percentage=100,
            vpc_subnets=ec2.SubnetSelection(subnet_type=ec2.SubnetType.PRIVATE_WITH_NAT),
            vpc=bstkvpc.vpc,
            minv_cpus=0,
            maxv_cpus=maxv_cpus,
            launch_template=ec2.LaunchTemplate(
                self,
                "bsb_ec2",
                block_devices=[
                    ec2.BlockDevice(
                        device_name="/dev/xvda",
                        volume=ec2.BlockDeviceVolume(
                            ebs_device=ec2.EbsDeviceProps(volume_size=100, volume_type=ec2.EbsDeviceVolumeType.GP2)
                        ),
                    )
                ],
            ),
        )

        # TODO: Go back and look at security groups. We had a pretty
        # permissive security group before and left it off here.
