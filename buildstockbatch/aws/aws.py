# -*- coding: utf-8 -*-

"""
buildstockbatch.aws
~~~~~~~~~~~~~~~
This class contains the object & methods that allow for usage of the library with AWS Batch

:author: Noel Merket
:copyright: (c) 2018 by The Alliance for Sustainable Energy
:license: BSD-3
"""
import argparse
import base64
import boto3
from botocore.exceptions import ClientError
from copy import deepcopy
import csv
from dask.distributed import Client
from dask_cloudprovider.aws import FargateCluster
import gzip
import hashlib
from joblib import Parallel, delayed
import json
import logging
import os
import pathlib
import random
from s3fs import S3FileSystem
import shutil
import tarfile
import re
import tempfile
import time
import tqdm
import io
import yaml

from buildstockbatch.aws.awsbase import AwsJobBase, boto_client_config
from buildstockbatch.base import ValidationError
from buildstockbatch.cloud import docker_base
from buildstockbatch.cloud.docker_base import DockerBatchBase
from buildstockbatch.utils import (
    log_error_details,
    get_project_configuration,
    get_bool_env_var,
)

logger = logging.getLogger(__name__)


def backoff(thefunc, *args, **kwargs):
    backoff_mult = 1.1
    delay = 3
    tries = 5
    error_patterns = [r"\w+.NotFound"]
    while tries > 0:
        try:
            result = thefunc(*args, **kwargs)
        except ClientError as error:
            error_code = error.response["Error"]["Code"]
            caught_error = False
            for pat in error_patterns:
                if re.search(pat, error_code):
                    logger.debug(f"{error_code}: Waiting and retrying in {delay} seconds")
                    caught_error = True
                    time.sleep(delay)
                    delay *= backoff_mult
                    tries -= 1
                    break
            if not caught_error:
                raise error
        else:
            return result


def upload_file_to_s3(*args, **kwargs):
    s3 = boto3.client("s3", config=boto_client_config)
    s3.upload_file(*args, **kwargs)


def upload_directory_to_s3(local_directory, bucket, prefix):
    local_dir_abs = pathlib.Path(local_directory).absolute()

    def filename_generator():
        for dirpath, dirnames, filenames in os.walk(local_dir_abs):
            for filename in filenames:
                if filename.startswith("."):
                    continue
                local_filepath = pathlib.Path(dirpath, filename)
                s3_key = pathlib.PurePosixPath(prefix, local_filepath.relative_to(local_dir_abs))
                yield local_filepath, s3_key

    logger.debug("Uploading {} => {}/{}".format(local_dir_abs, bucket, prefix))

    Parallel(n_jobs=-1, verbose=9)(
        delayed(upload_file_to_s3)(str(local_file), bucket, s3_key.as_posix())
        for local_file, s3_key in filename_generator()
    )


def compress_file(in_filename, out_filename):
    with gzip.open(str(out_filename), "wb") as f_out:
        with open(str(in_filename), "rb") as f_in:
            shutil.copyfileobj(f_in, f_out)


def calc_hash_for_file(filename):
    with open(filename, "rb") as f:
        return hashlib.sha256(f.read()).hexdigest()


def copy_s3_file(src_bucket, src_key, dest_bucket, dest_key):
    s3 = boto3.client("s3", config=boto_client_config)
    s3.copy({"Bucket": src_bucket, "Key": src_key}, dest_bucket, dest_key)


class AwsBatchEnv(AwsJobBase):
    """
    Class to manage the AWS Batch environment.
    """

    def __init__(self, job_name, aws_config, boto3_session):
        """
        Initialize the Batch environment.
        :param job_name:  Name of the job being run

        """
        super().__init__(job_name, aws_config, boto3_session)

        self.batch = self.session.client("batch", config=boto_client_config)
        self.ec2 = self.session.client("ec2", config=boto_client_config)
        self.ec2r = self.session.resource("ec2", config=boto_client_config)
        self.step_functions = self.session.client("stepfunctions", config=boto_client_config)
        self.aws_lambda = self.session.client("lambda", config=boto_client_config)
        self.s3 = self.session.client("s3", config=boto_client_config)
        self.s3_res = self.session.resource("s3", config=boto_client_config)

        self.task_role_arn = None
        self.job_definition_arn = None
        self.instance_role_arn = None
        self.spot_service_role_arn = None
        self.service_role_arn = None
        self.instance_profile_arn = None
        self.job_queue_arn = None
        self.s3_gateway_endpoint_id = None
        self.prefix_list_id = None

        logger.propagate = False

    def __repr__(self):
        return super().__repr__()

    def create_vpc(self):
        cidrs_in_use = set()
        vpc_response = self.ec2.describe_vpcs()
        for vpc in vpc_response["Vpcs"]:
            cidrs_in_use.add(vpc["CidrBlock"])
            for cidr_assoc in vpc["CidrBlockAssociationSet"]:
                cidrs_in_use.add(cidr_assoc["CidrBlock"])

        need_to_find_cidr = True
        while need_to_find_cidr:
            self.vpc_cidr = "172.{}.0.0/16".format(random.randrange(100, 200))
            need_to_find_cidr = self.vpc_cidr in cidrs_in_use

        self.pub_subnet_cidr = self.vpc_cidr.replace("/16", "/17")
        self.priv_subnet_cidr_1 = self.vpc_cidr.replace(".0.0/16", ".128.0/18")
        self.priv_subnet_cidr_2 = self.vpc_cidr.replace(".0.0/16", ".192.0/18")

        # Create the VPC

        response = backoff(
            self.ec2.create_vpc,
            CidrBlock=self.vpc_cidr,
            AmazonProvidedIpv6CidrBlock=False,
            InstanceTenancy="default",
        )
        self.vpc_id = response["Vpc"]["VpcId"]
        logger.info(f"VPC {self.vpc_id} created")

        backoff(
            self.ec2.create_tags,
            Resources=[self.vpc_id],
            Tags=self.get_tags_uppercase(Name=self.job_identifier),
        )
        # Find the default security group

        sec_response = self.ec2.describe_security_groups(
            Filters=[
                {"Name": "vpc-id", "Values": [self.vpc_id]},
            ]
        )

        self.batch_security_group = sec_response["SecurityGroups"][0]["GroupId"]

        logger.info(f"Security group {self.batch_security_group} created for vpc/job.")

        response = backoff(
            self.ec2.authorize_security_group_ingress,
            GroupId=self.batch_security_group,
            IpPermissions=[
                {
                    "FromPort": 0,
                    "IpProtocol": "tcp",
                    "IpRanges": [
                        {"CidrIp": "0.0.0.0/0"},
                    ],
                    "ToPort": 65535,
                },
            ],
        )

        # Create the private subnets

        priv_response_1 = backoff(
            self.ec2.create_subnet,
            CidrBlock=self.priv_subnet_cidr_1,
            AvailabilityZone=f"{self.region}a",
            VpcId=self.vpc_id,
        )

        self.priv_vpc_subnet_id_1 = priv_response_1["Subnet"]["SubnetId"]

        logger.info("Private subnet created.")

        priv_response_2 = backoff(
            self.ec2.create_subnet,
            CidrBlock=self.priv_subnet_cidr_2,
            AvailabilityZone=f"{self.region}b",
            VpcId=self.vpc_id,
        )

        self.priv_vpc_subnet_id_2 = priv_response_2["Subnet"]["SubnetId"]

        logger.info("Private subnet created.")

        backoff(
            self.ec2.create_tags,
            Resources=[self.priv_vpc_subnet_id_1],
            Tags=self.get_tags_uppercase(Name=self.job_identifier),
        )

        backoff(
            self.ec2.create_tags,
            Resources=[self.priv_vpc_subnet_id_2],
            Tags=self.get_tags_uppercase(Name=self.job_identifier),
        )

        ig_response = self.ec2.create_internet_gateway()

        self.internet_gateway_id = ig_response["InternetGateway"]["InternetGatewayId"]

        backoff(
            self.ec2.create_tags,
            Resources=[self.internet_gateway_id],
            Tags=self.get_tags_uppercase(Name=self.job_identifier),
        )

        logger.info(f"Internet gateway {self.internet_gateway_id} created.")

        # Create the public subnet

        pub_response = backoff(self.ec2.create_subnet, CidrBlock=self.pub_subnet_cidr, VpcId=self.vpc_id)

        logger.info("EIP allocated.")

        self.pub_vpc_subnet_id = pub_response["Subnet"]["SubnetId"]

        backoff(
            self.ec2.create_tags,
            Resources=[self.pub_vpc_subnet_id],
            Tags=self.get_tags_uppercase(Name=self.job_identifier),
        )

        # Create and elastic IP for the NAT Gateway

        ip_response = backoff(self.ec2.allocate_address, Domain="vpc")

        self.nat_ip_allocation = ip_response["AllocationId"]

        logger.info("EIP allocated.")

        backoff(
            self.ec2.create_tags,
            Resources=[self.nat_ip_allocation],
            Tags=self.get_tags_uppercase(Name=self.job_identifier),
        )

        # Create an internet gateway

        backoff(self.ec2.attach_internet_gateway, InternetGatewayId=self.internet_gateway_id, VpcId=self.vpc_id)

        logger.info("Internet Gateway attached.")

        # Find the only/default route table

        drt_response = self.ec2.describe_route_tables(
            Filters=[
                {"Name": "vpc-id", "Values": [self.vpc_id]},
            ]
        )

        self.pub_route_table_id = drt_response["RouteTables"][0]["RouteTableId"]

        # Modify the default route table to be used as the public route

        backoff(
            self.ec2.create_route,
            DestinationCidrBlock="0.0.0.0/0",
            GatewayId=self.internet_gateway_id,
            RouteTableId=self.pub_route_table_id,
        )

        # Create a NAT Gateway

        nat_response = backoff(
            self.ec2.create_nat_gateway, AllocationId=self.nat_ip_allocation, SubnetId=self.pub_vpc_subnet_id
        )

        self.nat_gateway_id = nat_response["NatGateway"]["NatGatewayId"]

        backoff(
            self.ec2.create_tags,
            Resources=[self.nat_gateway_id],
            Tags=self.get_tags_uppercase(Name=self.job_identifier),
        )

        logger.info("NAT Gateway created.")

        # Create a new private route table

        prt_response = backoff(self.ec2.create_route_table, VpcId=self.vpc_id)

        self.priv_route_table_id = prt_response["RouteTable"]["RouteTableId"]

        logger.info("Route table created.")

        backoff(
            self.ec2.create_tags,
            Resources=[self.nat_gateway_id, self.priv_route_table_id],
            Tags=self.get_tags_uppercase(Name=self.job_identifier),
        )

        # Associate the private route to the private subnet

        backoff(
            self.ec2.associate_route_table, RouteTableId=self.priv_route_table_id, SubnetId=self.priv_vpc_subnet_id_1
        )
        logger.info("Route table associated with subnet.")

        backoff(
            self.ec2.associate_route_table, RouteTableId=self.priv_route_table_id, SubnetId=self.priv_vpc_subnet_id_2
        )
        logger.info("Route table associated with subnet.")

        # Associate the NAT gateway with the private route

        backoff(
            self.ec2.create_route,
            DestinationCidrBlock="0.0.0.0/0",
            NatGatewayId=self.nat_gateway_id,
            RouteTableId=self.priv_route_table_id,
        )

        gateway_response = backoff(
            self.ec2.create_vpc_endpoint,
            VpcId=self.vpc_id,
            ServiceName=f"com.amazonaws.{self.region}.s3",
            RouteTableIds=[self.priv_route_table_id, self.pub_route_table_id],
            VpcEndpointType="Gateway",
            PolicyDocument='{"Statement": [{"Action": "*", "Effect": "Allow", "Resource": "*", "Principal": "*"}]}',
        )

        logger.info("S3 gateway created for VPC.")

        self.s3_gateway_endpoint_id = gateway_response["VpcEndpoint"]["VpcEndpointId"]

        backoff(
            self.ec2.create_tags,
            Resources=[self.s3_gateway_endpoint_id],
            Tags=self.get_tags_uppercase(Name=self.job_identifier),
        )

    def generate_name_value_inputs(self, var_dictionary):
        """
        Helper to properly format more easily used dictionaries.
        :param var_dictionary: a dictionary of key/values to be transformed
        :return: list of dictionaries in name: and value: outputs
        """
        name_vals = []
        for k, v in var_dictionary.items():
            name_vals.append(dict(name=k, value=v))
        return name_vals

    def create_batch_service_roles(self):
        """
        Creates the IAM roles used in the various areas of the batch service.
        This currently will not try to overwrite or update existing roles.
        """
        self.service_role_arn = self.iam_helper.role_stitcher(
            self.batch_service_role_name,
            "batch",
            f"Service role for Batch environment {self.job_identifier}",
            managed_policie_arns=["arn:aws:iam::aws:policy/service-role/AWSBatchServiceRole"],
        )

        # Instance Role for Batch compute environment

        self.instance_role_arn = self.iam_helper.role_stitcher(
            self.batch_instance_role_name,
            "ec2",
            f"Instance role for Batch compute environment {self.job_identifier}",
            managed_policie_arns=["arn:aws:iam::aws:policy/service-role/AmazonEC2ContainerServiceforEC2Role"],
        )

        # Instance Profile

        try:
            response = self.iam.create_instance_profile(InstanceProfileName=self.batch_instance_profile_name)

            self.instance_profile_arn = response["InstanceProfile"]["Arn"]

            logger.info("Instance Profile created")

            response = self.iam.add_role_to_instance_profile(
                InstanceProfileName=self.batch_instance_profile_name,
                RoleName=self.batch_instance_role_name,
            )

        except Exception as e:
            if "EntityAlreadyExists" in str(e):
                logger.info("ECS Instance Profile not created - already exists")
                response = self.iam.get_instance_profile(InstanceProfileName=self.batch_instance_profile_name)
                self.instance_profile_arn = response["InstanceProfile"]["Arn"]

        # ECS Task Policy

        task_permissions_policy = f"""{{
                "Version": "2012-10-17",
                "Statement": [
                    {{
                        "Sid": "VisualEditor0",
                        "Effect": "Allow",
                        "Action": [
                            "s3:PutAnalyticsConfiguration",
                            "s3:GetObjectVersionTagging",
                            "s3:CreateBucket",
                            "s3:ReplicateObject",
                            "s3:GetObjectAcl",
                            "s3:DeleteBucketWebsite",
                            "s3:PutLifecycleConfiguration",
                            "s3:GetObjectVersionAcl",
                            "s3:PutObjectTagging",
                            "s3:DeleteObject",
                            "s3:GetIpConfiguration",
                            "s3:DeleteObjectTagging",
                            "s3:GetBucketWebsite",
                            "s3:PutReplicationConfiguration",
                            "s3:DeleteObjectVersionTagging",
                            "s3:GetBucketNotification",
                            "s3:PutBucketCORS",
                            "s3:GetReplicationConfiguration",
                            "s3:ListMultipartUploadParts",
                            "s3:PutObject",
                            "s3:GetObject",
                            "s3:PutBucketNotification",
                            "s3:PutBucketLogging",
                            "s3:GetAnalyticsConfiguration",
                            "s3:GetObjectVersionForReplication",
                            "s3:GetLifecycleConfiguration",
                            "s3:ListBucketByTags",
                            "s3:GetInventoryConfiguration",
                            "s3:GetBucketTagging",
                            "s3:PutAccelerateConfiguration",
                            "s3:DeleteObjectVersion",
                            "s3:GetBucketLogging",
                            "s3:ListBucketVersions",
                            "s3:ReplicateTags",
                            "s3:RestoreObject",
                            "s3:ListBucket",
                            "s3:GetAccelerateConfiguration",
                            "s3:GetBucketPolicy",
                            "s3:PutEncryptionConfiguration",
                            "s3:GetEncryptionConfiguration",
                            "s3:GetObjectVersionTorrent",
                            "s3:AbortMultipartUpload",
                            "s3:PutBucketTagging",
                            "s3:GetBucketRequestPayment",
                            "s3:GetObjectTagging",
                            "s3:GetMetricsConfiguration",
                            "s3:DeleteBucket",
                            "s3:PutBucketVersioning",
                            "s3:ListBucketMultipartUploads",
                            "s3:PutMetricsConfiguration",
                            "s3:PutObjectVersionTagging",
                            "s3:GetBucketVersioning",
                            "s3:GetBucketAcl",
                            "s3:PutInventoryConfiguration",
                            "s3:PutIpConfiguration",
                            "s3:GetObjectTorrent",
                            "s3:PutBucketWebsite",
                            "s3:PutBucketRequestPayment",
                            "s3:GetBucketCORS",
                            "s3:GetBucketLocation",
                            "s3:ReplicateDelete",
                            "s3:GetObjectVersion"
                        ],
                        "Resource": [
                            "{self.s3_bucket_arn}",
                            "{self.s3_bucket_arn}/*"
                        ]
                    }},
                    {{
                        "Sid": "VisualEditor1",
                        "Effect": "Allow",
                        "Action": [
                            "s3:ListAllMyBuckets",
                            "s3:HeadBucket"
                        ],
                        "Resource": "*"
                    }}
                ]
            }}"""

        self.task_role_arn = self.iam_helper.role_stitcher(
            self.batch_ecs_task_role_name,
            "ecs-tasks",
            f"Task role for Batch job {self.job_identifier}",
            policies_list=[task_permissions_policy],
        )

        if self.batch_use_spot:
            # Spot Fleet Role
            self.spot_service_role_arn = self.iam_helper.role_stitcher(
                self.batch_spot_service_role_name,
                "spotfleet",
                f"Spot Fleet role for Batch compute environment {self.job_identifier}",
                managed_policie_arns=["arn:aws:iam::aws:policy/service-role/AmazonEC2SpotFleetTaggingRole"],
            )

    def create_compute_environment(self, maxCPUs=10000):
        """
        Creates a compute environment suffixed with the job name
        :param subnets: list of subnet IDs
        :param maxCPUs: numeric value for max VCPUs for the envionment

        """

        logger.debug(f"Creating launch template {self.launch_template_name}")
        try:
            self.ec2.create_launch_template(
                LaunchTemplateName=self.launch_template_name,
                LaunchTemplateData={
                    "BlockDeviceMappings": [
                        {
                            "DeviceName": "/dev/xvda",
                            "Ebs": {"VolumeSize": 100, "VolumeType": "gp2"},
                        }
                    ]
                },
            )
        except ClientError as error:
            if error.response["Error"]["Code"] == "InvalidLaunchTemplateName.AlreadyExistsException":
                logger.debug("Launch template exists, skipping creation")
            else:
                raise error

        while True:
            lt_resp = self.ec2.describe_launch_templates(LaunchTemplateNames=[self.launch_template_name])
            launch_templates = lt_resp["LaunchTemplates"]
            next_token = lt_resp.get("NextToken")
            while next_token:
                lt_resp = self.ec2.describe_launch_templates(
                    LaunchTemplateNames=[self.launch_template_name],
                    NextToken=next_token,
                )
                launch_templates.extend(lt_resp["LaunchTemplates"])
                next_token = lt_resp.get("NextToken")
            n_launch_templates = len(launch_templates)
            assert n_launch_templates <= 1, f"There are {n_launch_templates} launch templates, this shouldn't happen."
            if n_launch_templates == 0:
                logger.debug(f"Waiting for the launch template {self.launch_template_name} to be created")
                time.sleep(5)
            if n_launch_templates == 1:
                break

        try:
            compute_resources = {
                "minvCpus": 0,
                "maxvCpus": maxCPUs,
                "desiredvCpus": 0,
                "instanceTypes": [
                    "optimal",
                ],
                "launchTemplate": {
                    "launchTemplateName": self.launch_template_name,
                },
                "subnets": [self.priv_vpc_subnet_id_1, self.priv_vpc_subnet_id_2],
                "securityGroupIds": [self.batch_security_group],
                "instanceRole": self.instance_profile_arn,
            }

            if self.batch_use_spot:
                compute_resources.update(
                    {
                        "type": "SPOT",
                        "bidPercentage": 100,
                        "spotIamFleetRole": self.spot_service_role_arn,
                    }
                )
            else:
                compute_resources["type"] = "EC2"

            compute_resources["tags"] = self.get_tags(Name=f"{self.job_identifier} batch instance")

            self.batch.create_compute_environment(
                computeEnvironmentName=self.batch_compute_environment_name,
                type="MANAGED",
                state="ENABLED",
                computeResources=compute_resources,
                serviceRole=self.service_role_arn,
                tags=self.get_tags(),
            )

            logger.info(f"Compute environment {self.batch_compute_environment_name} created.")

        except Exception as e:
            if "Object already exists" in str(e):
                logger.info(f"Compute environment {self.batch_compute_environment_name} not created - already exists")
            else:
                raise

    def create_job_queue(self):
        """
        Creates a job queue based on the Batch environment definition
        """

        while True:
            try:
                response = self.batch.create_job_queue(
                    jobQueueName=self.batch_job_queue_name,
                    state="ENABLED",
                    priority=1,
                    computeEnvironmentOrder=[
                        {
                            "order": 1,
                            "computeEnvironment": self.batch_compute_environment_name,
                        },
                    ],
                    tags=self.get_tags(),
                )

                # print("JOB QUEUE")
                # print(response)
                self.job_queue_arn = response["jobQueueArn"]
                logger.info(f"Job queue {self.batch_job_queue_name} created")
                break

            except Exception as e:
                if "Object already exists" in str(e):
                    logger.info(f"Job queue {self.batch_job_queue_name} not created - already exists")
                    response = self.batch.describe_job_queues(
                        jobQueues=[
                            self.batch_job_queue_name,
                        ]
                    )
                    self.job_queue_arn = response["jobQueues"][0]["jobQueueArn"]
                    break

                elif "is not valid" in str(e):
                    # Need to wait a second for the compute environment to complete registration
                    logger.warning("waiting a few seconds for compute environment creation: " + str(e))
                    time.sleep(5)

                else:
                    raise

    def create_job_definition(self, docker_image, vcpus, memory, command, env_vars):
        """
        Creates a job definition to run in the Batch environment.  This will create a new version with every execution.
        :param docker_image: The image ID from the related ECR enviornment
        :param vcpus: Numeric value of the vcpus dedicated to each container
        :param memory: Numeric value of the memory MBs dedicated to each container
        :param command: Command to run in the container
        :param env_vars: Dictionary of key/value environment variables to include in the job
        """
        response = self.batch.register_job_definition(
            jobDefinitionName=self.job_identifier,
            type="container",
            # parameters={
            #    'string': 'string'
            # },
            containerProperties={
                "image": docker_image,
                "vcpus": vcpus,
                "memory": memory,
                "command": command,
                "jobRoleArn": self.task_role_arn,
                "environment": self.generate_name_value_inputs(env_vars),
            },
            retryStrategy={"attempts": 2},
            tags=self.get_tags(),
        )

        self.job_definition_arn = response["jobDefinitionArn"]

    def submit_job(self, array_size=4):
        """
        Submits the created job definition and version to be run.
        """

        resp = backoff(
            self.batch.submit_job,
            jobName=self.job_identifier,
            jobQueue=self.batch_job_queue_name,
            arrayProperties={"size": array_size},
            jobDefinition=self.job_definition_arn,
            tags=self.get_tags(),
        )

        logger.info(f"Job {self.job_identifier} submitted.")
        return resp

    def clean(self):
        # Get our vpc:

        response = self.ec2.describe_vpcs(
            Filters=[
                {
                    "Name": "tag:Name",
                    "Values": [
                        self.vpc_name,
                    ],
                },
            ]
        )
        try:
            self.vpc_id = response["Vpcs"][0]["VpcId"]
        except (KeyError, IndexError):
            self.vpc_id = None

        default_sg_response = self.ec2.describe_security_groups(
            Filters=[
                {
                    "Name": "group-name",
                    "Values": [
                        "default",
                    ],
                },
            ]
        )

        logger.info("Removing egress from default security group.")
        for group in default_sg_response["SecurityGroups"]:
            if group["VpcId"] == self.vpc_id:
                default_group_id = group["GroupId"]
                dsg = self.ec2r.SecurityGroup(default_group_id)
                if len(dsg.ip_permissions_egress):
                    response = dsg.revoke_egress(IpPermissions=dsg.ip_permissions_egress)

        try:
            self.batch.update_job_queue(jobQueue=self.batch_job_queue_name, state="DISABLED")

            while True:
                try:
                    response = self.batch.delete_job_queue(jobQueue=self.batch_job_queue_name)
                    logger.info(f"Job queue {self.batch_job_queue_name} deleted.")
                    break
                except Exception as e:
                    if "Cannot delete, resource is being modified" in str(e):
                        logger.info("Job queue being modified - sleeping until ready...")
                        time.sleep(5)
                    else:
                        raise
        except Exception as e:
            if "does not exist" in str(e):
                logger.info(f"Job queue {self.batch_job_queue_name} missing, skipping...")

        # Delete compute enviornment

        try:
            self.batch.update_compute_environment(
                computeEnvironment=self.batch_compute_environment_name, state="DISABLED"
            )

            while True:
                try:
                    response = self.batch.delete_compute_environment(
                        computeEnvironment=self.batch_compute_environment_name
                    )
                    logger.info(f"Compute environment {self.batch_compute_environment_name} deleted.")
                    break
                except Exception as e:
                    if "Cannot delete, resource is being modified" in str(e) or "found existing JobQueue" in str(e):
                        logger.info("Compute environment being modified - sleeping until ready...")
                        time.sleep(5)
                    else:
                        raise
        except Exception as e:
            if "does not exist" in str(e):
                logger.info(f"Compute environment {self.batch_compute_environment_name} missing, skipping...")
            else:
                raise

        # Delete Launch Template
        try:
            self.ec2.delete_launch_template(LaunchTemplateName=self.launch_template_name)
        except Exception as e:
            if "does not exist" in str(e):
                logger.info(f"Launch template {self.launch_template_name} does not exist, skipping...")
            else:
                raise

        self.iam_helper.delete_role(self.batch_service_role_name)
        self.iam_helper.delete_role(self.batch_spot_service_role_name)
        self.iam_helper.delete_role(self.batch_ecs_task_role_name)
        # Instance profile order of removal
        self.iam_helper.remove_role_from_instance_profile(self.batch_instance_profile_name)
        self.iam_helper.delete_role(self.batch_instance_role_name)
        self.iam_helper.delete_instance_profile(self.batch_instance_profile_name)

        # Find Nat Gateways and VPCs

        response = self.ec2.describe_vpcs(
            Filters=[
                {
                    "Name": "tag:Name",
                    "Values": [
                        self.job_identifier,
                    ],
                },
            ],
        )

        for vpc in response["Vpcs"]:
            this_vpc = vpc["VpcId"]

            s3gw_response = self.ec2.describe_vpc_endpoints(Filters=[{"Name": "vpc-id", "Values": [this_vpc]}])

            for s3gw in s3gw_response["VpcEndpoints"]:
                this_s3gw = s3gw["VpcEndpointId"]

                if s3gw["State"] != "deleted":
                    self.ec2.delete_vpc_endpoints(VpcEndpointIds=[this_s3gw])

            ng_response = self.ec2.describe_nat_gateways(Filters=[{"Name": "vpc-id", "Values": [this_vpc]}])

            for natgw in ng_response["NatGateways"]:
                this_natgw = natgw["NatGatewayId"]

                if natgw["State"] != "deleted":
                    self.ec2.delete_nat_gateway(NatGatewayId=this_natgw)

            rtas_response = self.ec2.describe_route_tables(Filters=[{"Name": "vpc-id", "Values": [this_vpc]}])

            for route_table in rtas_response["RouteTables"]:
                route_table_id = route_table["RouteTableId"]
                for association in route_table["Associations"]:
                    if not association["Main"]:
                        response = self.ec2.disassociate_route_table(
                            AssociationId=association["RouteTableAssociationId"]
                        )
                        rt_counter = 10
                        while rt_counter:
                            try:
                                response = self.ec2.delete_route_table(RouteTableId=route_table_id)
                                logger.info("Route table removed.")
                                break
                            except Exception as e:
                                rt_counter = rt_counter - 1
                                if "DependencyViolation" in str(e):
                                    logger.info(
                                        "Waiting for association to be released before deleting route table.  Sleeping..."
                                    )  # noqa E501
                                    time.sleep(5)
                                else:
                                    raise

            igw_response = self.ec2.describe_internet_gateways(
                Filters=[{"Name": "tag:Name", "Values": [self.job_identifier]}]
            )

            for internet_gateway in igw_response["InternetGateways"]:
                for attachment in internet_gateway["Attachments"]:
                    if attachment["VpcId"] == this_vpc:
                        while True:
                            try:
                                try:
                                    self.ec2.detach_internet_gateway(
                                        InternetGatewayId=internet_gateway["InternetGatewayId"],
                                        VpcId=attachment["VpcId"],
                                    )
                                except Exception as e:
                                    logger.info(f"Error on Internet Gateway disassociation - ignoring... {str(e)}")

                                self.ec2.delete_internet_gateway(
                                    InternetGatewayId=internet_gateway["InternetGatewayId"]
                                )
                                logger.info("Internet Gateway deleted.")
                                break

                            except Exception as e:
                                if "DependencyViolation" in str(e):
                                    logger.info(
                                        "Waiting for IPs to be released before deleting Internet Gateway.  Sleeping..."
                                    )
                                    time.sleep(5)
                                else:
                                    raise

            subn_response = self.ec2.describe_subnets(Filters=[{"Name": "vpc-id", "Values": [this_vpc]}])

            for subnet in subn_response["Subnets"]:
                while True:
                    try:
                        self.ec2.delete_subnet(SubnetId=subnet["SubnetId"])
                        break
                    except Exception as e:
                        if "DependencyViolation" in str(e):
                            logger.info("Subnet cannot be deleted as dependencies are still being deleted. Sleeping...")
                            time.sleep(10)
                        else:
                            raise

            self.ec2.delete_vpc(VpcId=this_vpc)

        # Find the Elastic IP from the NAT
        response = self.ec2.describe_addresses(
            Filters=[
                {
                    "Name": "tag:Name",
                    "Values": [
                        self.job_identifier,
                    ],
                },
            ],
        )
        for address in response["Addresses"]:
            this_address = address["AllocationId"]

            response = self.ec2.release_address(AllocationId=this_address)

        try:
            self.ec2.delete_security_group(GroupName=f"dask-{self.job_identifier}")
        except ClientError as error:
            if error.response["Error"]["Code"] == "InvalidGroup.NotFound":
                pass
            else:
                raise error


class AwsBatch(DockerBatchBase):
    def __init__(self, project_filename):
        super().__init__(project_filename)

        self.job_identifier = re.sub("[^0-9a-zA-Z]+", "_", self.cfg["aws"]["job_identifier"])[:10]

        self.project_filename = project_filename
        self.region = self.cfg["aws"]["region"]
        self.ecr = boto3.client("ecr", region_name=self.region, config=boto_client_config)
        self.s3 = boto3.client("s3", region_name=self.region, config=boto_client_config)
        self.s3_bucket = self.cfg["aws"]["s3"]["bucket"]
        self.s3_bucket_prefix = self.cfg["aws"]["s3"]["prefix"].rstrip("/")
        self.batch_env_use_spot = self.cfg["aws"]["use_spot"]
        self.batch_array_size = self.cfg["aws"]["batch_array_size"]
        self.boto3_session = boto3.Session(region_name=self.region)

    @staticmethod
    def validate_dask_settings(project_file):
        cfg = get_project_configuration(project_file)
        if "emr" in cfg["aws"]:
            logger.warning("The `aws.emr` configuration is no longer used and is ignored. Recommend removing.")
        dask_cfg = cfg["aws"]["dask"]
        errors = []
        mem_rules = {
            1024: (2, 8, 1),
            2048: (4, 16, 1),
            4096: (8, 30, 1),
            8192: (16, 60, 4),
            16384: (32, 120, 8),
        }
        for node_type in ("scheduler", "worker"):
            mem = dask_cfg.get(f"{node_type}_memory", 8 * 1024)
            if mem % 1024 != 0:
                errors.append(f"`aws.dask.{node_type}_memory` = {mem}, needs to be a multiple of 1024.")
            mem_gb = mem // 1024
            min_gb, max_gb, incr_gb = mem_rules[dask_cfg.get(f"{node_type}_cpu", 2 * 1024)]
            if not (min_gb <= mem_gb <= max_gb and (mem_gb - min_gb) % incr_gb == 0):
                errors.append(
                    f"`aws.dask.{node_type}_memory` = {mem}, "
                    f"should be between {min_gb * 1024} and {max_gb * 1024} in a multiple of {incr_gb * 1024}."
                )
        if errors:
            errors.append("See https://docs.aws.amazon.com/AmazonECS/latest/developerguide/task-cpu-memory-error.html")
            raise ValidationError("\n".join(errors))

        return True

    @staticmethod
    def validate_project(project_file):
        super(AwsBatch, AwsBatch).validate_project(project_file)
        AwsBatch.validate_dask_settings(project_file)

    @property
    def docker_image(self):
        return "nrel/buildstockbatch"

    @property
    def results_dir(self):
        return f"{self.s3_bucket}/{self.s3_bucket_prefix}/results"

    @property
    def output_dir(self):
        return f"{self.s3_bucket}/{self.s3_bucket_prefix}"

    @property
    def container_repo(self):
        repo_name = self.docker_image
        repos = self.ecr.describe_repositories()
        repo = None
        for repo in repos["repositories"]:
            if repo["repositoryName"] == repo_name:
                break
        if repo is None:
            resp = self.ecr.create_repository(repositoryName=repo_name)
            repo = resp["repository"]
        return repo

    @property
    def image_url(self):
        return f"{self.container_repo['repositoryUri']}:{self.job_identifier}"

    def build_image(self):
        """
        Build the docker image to use in the batch simulation
        """
        root_path = pathlib.Path(os.path.abspath(__file__)).parent.parent.parent
        if not (root_path / "Dockerfile").exists():
            raise RuntimeError(f"The needs to be run from the root of the repo, found {root_path}")

        # Make the buildstock/resources/.aws_docker_image dir to store logs
        local_log_dir = pathlib.Path(self.buildstock_dir, "resources", ".aws_docker_image")
        if not os.path.exists(local_log_dir):
            os.makedirs(local_log_dir)

        # Determine whether or not to build the image with custom gems bundled in
        if self.cfg.get("baseline", dict()).get("custom_gems", False):
            # Ensure the custom Gemfile exists in the buildstock dir
            local_gemfile_path = pathlib.Path(self.buildstock_dir, "resources", "Gemfile")
            if not local_gemfile_path.exists():
                raise AttributeError(f"baseline:custom_gems = True, but did not find Gemfile at {local_gemfile_path}")

            # Copy the custom Gemfile into the buildstockbatch repo
            new_gemfile_path = root_path / "Gemfile"
            shutil.copyfile(local_gemfile_path, new_gemfile_path)
            logger.info(f"Copying custom Gemfile from {local_gemfile_path}")

            # Choose the custom-gems stage in the Dockerfile,
            # which runs bundle install to build custom gems into the image
            stage = "buildstockbatch-custom-gems"
        else:
            # Choose the base stage in the Dockerfile,
            # which stops before bundling custom gems into the image
            stage = "buildstockbatch"

        logger.info(f"Building docker image stage: {stage} from OpenStudio {self.os_version}")
        img, build_logs = self.docker_client.images.build(
            path=str(root_path),
            tag=self.docker_image,
            rm=True,
            target=stage,
            platform="linux/amd64",
            buildargs={
                "OS_VER": self.os_version,
                "CLOUD_PLATFORM": "aws",
            },
        )
        build_image_log = os.path.join(local_log_dir, "build_image.log")
        with open(build_image_log, "w") as f_out:
            f_out.write("Built image")
            for line in build_logs:
                for itm_type, item_msg in line.items():
                    if itm_type in ["stream", "status"]:
                        try:
                            f_out.write(f"{item_msg}")
                        except UnicodeEncodeError:
                            pass
        logger.debug(f"Review docker image build log: {build_image_log}")

        # Report and confirm the openstudio version from the image
        os_ver_cmd = "openstudio openstudio_version"
        container_output = self.docker_client.containers.run(
            self.docker_image, os_ver_cmd, remove=True, name="list_openstudio_version"
        )
        assert self.os_version in container_output.decode()

        # Report gems included in the docker image.
        # The OpenStudio Docker image installs the default gems
        # to /var/oscli/gems, and the custom docker image
        # overwrites these with the custom gems.
        list_gems_cmd = (
            "openstudio --bundle /var/oscli/Gemfile --bundle_path /var/oscli/gems "
            "--bundle_without native_ext gem_list"
        )
        container_output = self.docker_client.containers.run(
            self.docker_image, list_gems_cmd, remove=True, name="list_gems"
        )
        gem_list_log = os.path.join(local_log_dir, "openstudio_gem_list_output.log")
        with open(gem_list_log, "wb") as f_out:
            f_out.write(container_output)
        for line in container_output.decode().split("\n"):
            logger.debug(line)
        logger.debug(f"Review custom gems list at: {gem_list_log}")

    def push_image(self):
        """
        Push the locally built docker image to the AWS docker repo
        """
        auth_token = self.ecr.get_authorization_token()
        dkr_user, dkr_pass = (
            base64.b64decode(auth_token["authorizationData"][0]["authorizationToken"]).decode("ascii").split(":")
        )
        repo_url = self.container_repo["repositoryUri"]
        registry_url = "https://" + repo_url.split("/")[0]
        resp = self.docker_client.login(username=dkr_user, password=dkr_pass, registry=registry_url)
        logger.debug(resp)
        image = self.docker_client.images.get(self.docker_image)
        image.tag(repo_url, tag=self.job_identifier)
        last_status = None
        for x in self.docker_client.images.push(repo_url, tag=self.job_identifier, stream=True):
            try:
                y = json.loads(x)
            except json.JSONDecodeError:
                continue
            else:
                if y.get("status") is not None and y.get("status") != last_status:
                    logger.debug(y["status"])
                    last_status = y["status"]

    def clean(self):
        """
        Clean up the AWS environment
        :return:
        """
        logger.info("Beginning cleanup of AWS resources...")

        batch_env = AwsBatchEnv(self.job_identifier, self.cfg["aws"], self.boto3_session)
        batch_env.clean()

    def upload_batch_files_to_cloud(self, tmppath):
        """Implements :func:`DockerBase.upload_batch_files_to_cloud`"""
        logger.debug("Uploading Batch files to S3")
        upload_directory_to_s3(
            tmppath,
            self.cfg["aws"]["s3"]["bucket"],
            self.cfg["aws"]["s3"]["prefix"],
        )

    def copy_files_at_cloud(self, files_to_copy):
        """Implements :func:`DockerBase.copy_files_at_cloud`"""
        logger.debug("Copying weather files on S3")
        bucket = self.cfg["aws"]["s3"]["bucket"]
        Parallel(n_jobs=-1, verbose=9)(
            delayed(copy_s3_file)(
                bucket,
                f"{self.cfg['aws']['s3']['prefix']}/weather/{src}",
                bucket,
                f"{self.cfg['aws']['s3']['prefix']}/weather/{dest}",
            )
            for src, dest in files_to_copy
        )

    def start_batch_job(self, batch_info):
        """Implements :func:`DockerBase.start_batch_job`"""
        # Create the output directories
        fs = S3FileSystem()
        for upgrade_id in range(len(self.cfg.get("upgrades", [])) + 1):
            fs.makedirs(
                f"{self.cfg['aws']['s3']['bucket']}/{self.cfg['aws']['s3']['prefix']}/results/simulation_output/"
                f"timeseries/up{upgrade_id:02d}"
            )

        # Define the batch environment
        batch_env = AwsBatchEnv(self.job_identifier, self.cfg["aws"], self.boto3_session)
        logger.info(
            "Launching Batch environment - (resource configs will not be updated on subsequent executions, but new job revisions will be created):"  # noqa 501
        )
        logger.debug(str(batch_env))
        batch_env.create_batch_service_roles()
        batch_env.create_vpc()
        batch_env.create_compute_environment()
        batch_env.create_job_queue()

        # Pass through config for the Docker containers
        env_vars = dict(
            S3_BUCKET=self.s3_bucket,
            S3_PREFIX=self.s3_bucket_prefix,
            JOB_NAME=self.job_identifier,
            REGION=self.region,
        )

        job_env_cfg = self.cfg["aws"].get("job_environment", {})
        batch_env.create_job_definition(
            self.image_url,
            command=["python3", "-m", "buildstockbatch.aws.aws"],
            vcpus=job_env_cfg.get("vcpus", 1),
            memory=job_env_cfg.get("memory", 1024),
            env_vars=env_vars,
        )

        # start job
        job_info = batch_env.submit_job(array_size=self.batch_array_size)

        # Monitor job status
        n_succeeded_last_time = 0
        with tqdm.tqdm(desc="Running Simulations", total=self.batch_array_size) as progress_bar:
            job_status = None
            while job_status not in ("SUCCEEDED", "FAILED"):
                time.sleep(10)
                job_desc_resp = batch_env.batch.describe_jobs(jobs=[job_info["jobId"]])
                job_status = job_desc_resp["jobs"][0]["status"]

                jobs_resp = batch_env.batch.list_jobs(arrayJobId=job_info["jobId"], jobStatus="SUCCEEDED")
                n_succeeded = len(jobs_resp["jobSummaryList"])
                next_token = jobs_resp.get("nextToken")
                while next_token is not None:
                    jobs_resp = batch_env.batch.list_jobs(
                        arrayJobId=job_info["jobId"],
                        jobStatus="SUCCEEDED",
                        nextToken=next_token,
                    )
                    n_succeeded += len(jobs_resp["jobSummaryList"])
                    next_token = jobs_resp.get("nextToken")
                progress_bar.update(n_succeeded - n_succeeded_last_time)
                n_succeeded_last_time = n_succeeded

        logger.info(f"Batch job status: {job_status}")
        if job_status == "FAILED":
            raise RuntimeError("Batch Job Failed. Go look at the CloudWatch logs.")

    @classmethod
    def run_job(cls, job_id, bucket, prefix, job_name, region):
        """
        Run a few simulations inside a container.

        This method is called from inside docker container in AWS. It will
        go get the necessary files from S3, run the simulation, and post the
        results back to S3.
        """

        logger.debug(f"region: {region}")
        s3 = boto3.client("s3", config=boto_client_config)

        sim_dir = pathlib.Path("/var/simdata/openstudio")

        logger.debug("Downloading assets")
        assets_file_path = sim_dir.parent / "assets.tar.gz"
        s3.download_file(bucket, f"{prefix}/assets.tar.gz", str(assets_file_path))
        with tarfile.open(assets_file_path, "r") as tar_f:
            tar_f.extractall(sim_dir)
        os.remove(assets_file_path)

        logger.debug("Reading config")
        with io.BytesIO() as f:
            s3.download_fileobj(bucket, f"{prefix}/config.json", f)
            cfg = json.loads(f.getvalue())

        logger.debug("Getting job information")
        jobs_file_path = sim_dir.parent / "jobs.tar.gz"
        s3.download_file(bucket, f"{prefix}/jobs.tar.gz", str(jobs_file_path))
        with tarfile.open(jobs_file_path, "r") as tar_f:
            jobs_d = json.load(tar_f.extractfile(f"jobs/job{job_id:05d}.json"), encoding="utf-8")
        logger.debug("Number of simulations = {}".format(len(jobs_d["batch"])))

        logger.debug("Getting weather files")
        weather_dir = sim_dir / "weather"
        os.makedirs(weather_dir, exist_ok=True)

        epws_to_download = docker_base.determine_epws_needed_for_job(sim_dir, jobs_d)
        # Make a lookup of which parameter points to the weather file from options_lookup.tsv
        with open(sim_dir / "lib" / "resources" / "options_lookup.tsv", "r", encoding="utf-8") as f:
            tsv_reader = csv.reader(f, delimiter="\t")
            next(tsv_reader)  # skip headers
            param_name = None
            epws_by_option = {}
            for row in tsv_reader:
                row_has_epw = [x.endswith(".epw") for x in row[2:]]
                if sum(row_has_epw):
                    if row[0] != param_name and param_name is not None:
                        raise RuntimeError(
                            f"The epw files are specified in options_lookup.tsv under more than one parameter type: {param_name}, {row[0]}"
                        )  # noqa: E501
                    epw_filename = row[row_has_epw.index(True) + 2].split("=")[1].split("/")[-1]
                    param_name = row[0]
                    option_name = row[1]
                    epws_by_option[option_name] = epw_filename

        # Look through the buildstock.csv to find the appropriate location and epw
        epws_to_download = set()
        building_ids = [x[0] for x in jobs_d["batch"]]
        with open(
            sim_dir / "lib" / "housing_characteristics" / "buildstock.csv",
            "r",
            encoding="utf-8",
        ) as f:
            csv_reader = csv.DictReader(f)
            for row in csv_reader:
                if int(row["Building"]) in building_ids:
                    epws_to_download.add(epws_by_option[row[param_name]])

        # Download the epws needed for these simulations
        for epw_filename in epws_to_download:
            with io.BytesIO() as f_gz:
                logger.debug("Downloading {}.gz".format(epw_filename))
                s3.download_fileobj(bucket, f"{prefix}/weather/{epw_filename}.gz", f_gz)
                with open(weather_dir / epw_filename, "wb") as f_out:
                    logger.debug("Extracting {}".format(epw_filename))
                    f_out.write(gzip.decompress(f_gz.getvalue()))

        cls.run_simulations(cfg, job_id, jobs_d, sim_dir, S3FileSystem(), f"{bucket}/{prefix}")

    def get_fs(self):
        return S3FileSystem()

    def get_dask_client(self):
        dask_cfg = self.cfg["aws"]["dask"]

        batch_env = AwsBatchEnv(self.job_identifier, self.cfg["aws"], self.boto3_session)
        m = 1024
        self.dask_cluster = FargateCluster(
            region_name=self.region,
            fargate_spot=True,
            image=self.image_url,
            cluster_name_template=f"dask-{self.job_identifier}",
            scheduler_cpu=dask_cfg.get("scheduler_cpu", 2 * m),
            scheduler_mem=dask_cfg.get("scheduler_memory", 8 * m),
            worker_cpu=dask_cfg.get("worker_cpu", 2 * m),
            worker_mem=dask_cfg.get("worker_memory", 8 * m),
            n_workers=dask_cfg["n_workers"],
            task_role_policies=["arn:aws:iam::aws:policy/AmazonS3FullAccess"],
            tags=batch_env.get_tags(),
        )
        self.dask_client = Client(self.dask_cluster)
        return self.dask_client

    def cleanup_dask(self):
        self.dask_client.close()
        self.dask_cluster.close()

    def upload_results(self, *args, **kwargs):
        """Do nothing because the results are already on S3"""
        return self.s3_bucket, self.s3_bucket_prefix + "/results/parquet"

    def process_results(self, *args, **kwargs):
        with tempfile.TemporaryDirectory() as tmpdir:
            tmppath = pathlib.Path(tmpdir)
            container_workpath = pathlib.PurePosixPath("/var/simdata/openstudio")

            cfg = deepcopy(self.cfg)
            container_buildstock_dir = str(container_workpath / "buildstock")
            cfg["buildstock_directory"] = container_buildstock_dir
            cfg["project_directory"] = str(pathlib.Path(self.project_dir).relative_to(self.buildstock_dir))

            with open(tmppath / "project_config.yml", "w") as f:
                f.write(yaml.dump(cfg, Dumper=yaml.SafeDumper))
            container_cfg_path = str(container_workpath / "project_config.yml")

            with open(tmppath / "args.json", "w") as f:
                json.dump([args, kwargs], f)

            credentials = boto3.Session().get_credentials().get_frozen_credentials()
            env = {
                "AWS_ACCESS_KEY_ID": credentials.access_key,
                "AWS_SECRET_ACCESS_KEY": credentials.secret_key,
            }
            if credentials.token:
                env["AWS_SESSION_TOKEN"] = credentials.token
            env["POSTPROCESSING_INSIDE_DOCKER_CONTAINER"] = "true"

            logger.info("Starting container for postprocessing")
            container = self.docker_client.containers.run(
                self.image_url,
                ["python3", "-m", "buildstockbatch.aws.aws", container_cfg_path],
                volumes={
                    tmpdir: {"bind": str(container_workpath), "mode": "rw"},
                    self.buildstock_dir: {"bind": container_buildstock_dir, "mode": "ro"},
                },
                environment=env,
                name="bsb_post",
                auto_remove=True,
                detach=True,
            )
            for msg in container.logs(stream=True):
                logger.debug(msg)

    def _process_results_inside_container(self):
        with open("/var/simdata/openstudio/args.json", "r") as f:
            args, kwargs = json.load(f)

        logger.info("Running postprocessing in container")
        super().process_results(*args, **kwargs)


@log_error_details()
def main():
    logging.config.dictConfig(
        {
            "version": 1,
            "disable_existing_loggers": True,
            "formatters": {
                "defaultfmt": {
                    "format": "%(levelname)s:%(asctime)s:%(name)s:%(message)s",
                    "datefmt": "%Y-%m-%d %H:%M:%S",
                }
            },
            "handlers": {
                "console": {
                    "class": "logging.StreamHandler",
                    "formatter": "defaultfmt",
                    "level": "DEBUG",
                    "stream": "ext://sys.stdout",
                }
            },
            "loggers": {
                "__main__": {
                    "level": "DEBUG",
                    "propagate": True,
                    "handlers": ["console"],
                },
                "buildstockbatch": {
                    "level": "DEBUG",
                    "propagate": True,
                    "handlers": ["console"],
                },
            },
        }
    )
    print(AwsBatch.LOGO)
    if "AWS_BATCH_JOB_ARRAY_INDEX" in os.environ:
        job_id = int(os.environ["AWS_BATCH_JOB_ARRAY_INDEX"])
        s3_bucket = os.environ["S3_BUCKET"]
        s3_prefix = os.environ["S3_PREFIX"]
        job_name = os.environ["JOB_NAME"]
        region = os.environ["REGION"]
        AwsBatch.run_job(job_id, s3_bucket, s3_prefix, job_name, region)
    elif get_bool_env_var("POSTPROCESSING_INSIDE_DOCKER_CONTAINER"):
        parser = argparse.ArgumentParser()
        parser.add_argument("project_filename")
        args = parser.parse_args()
        batch = AwsBatch(args.project_filename)
        batch._process_results_inside_container()
    else:
        parser = argparse.ArgumentParser()
        parser.add_argument("project_filename")
        group = parser.add_mutually_exclusive_group()
        group.add_argument(
            "-c",
            "--clean",
            action="store_true",
            help="After the simulation is done, run with --clean to clean up AWS environment",
        )
        group.add_argument(
            "--validateonly",
            help="Only validate the project YAML file and references. Nothing is executed",
            action="store_true",
        )
        group.add_argument(
            "--postprocessonly",
            help="Only do postprocessing, useful for when the simulations are already done",
            action="store_true",
        )
        group.add_argument(
            "--crawl",
            help="Only do the crawling in Athena. When simulations and postprocessing are done.",
            action="store_true",
        )
        args = parser.parse_args()

        # validate the project, and in case of the --validateonly flag return True if validation passes
        AwsBatch.validate_project(os.path.abspath(args.project_filename))
        if args.validateonly:
            return True

        batch = AwsBatch(args.project_filename)
        if args.clean:
            batch.clean()
        elif args.postprocessonly:
            batch.build_image()
            batch.push_image()
            batch.process_results()
        elif args.crawl:
            batch.process_results(skip_combine=True, use_dask_cluster=False)
        else:
            batch.build_image()
            batch.push_image()
            batch.run_batch()
            batch.process_results()
            batch.clean()


if __name__ == "__main__":
    main()
