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
from awsretry import AWSRetry
import base64
import boto3
from botocore.exceptions import ClientError
import csv
from fsspec.implementations.local import LocalFileSystem
import gzip
from joblib import Parallel, delayed
import json
import logging
import os
import pathlib
import random
from s3fs import S3FileSystem
import shutil
import subprocess
import tarfile
import re
import time
import io
import zipfile

from buildstockbatch import postprocessing
from buildstockbatch.aws.awsbase import AwsJobBase
from buildstockbatch.base import ValidationError
from buildstockbatch.cloud.docker_base import DockerBatchBase
from buildstockbatch.utils import log_error_details, get_project_configuration

logger = logging.getLogger(__name__)


def upload_file_to_s3(*args, **kwargs):
    s3 = boto3.client("s3")
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


def copy_s3_file(src_bucket, src_key, dest_bucket, dest_key):
    s3 = boto3.client("s3")
    s3.copy({"Bucket": src_bucket, "Key": src_key}, dest_bucket, dest_key)


class AwsBatchEnv(AwsJobBase):
    """
    Class to manage the AWS Batch environment and Step Function controller.
    """

    def __init__(self, job_name, aws_config, boto3_session):
        """
        Initialize the Batch environment.
        :param job_name:  Name of the job being run

        """
        super().__init__(job_name, aws_config, boto3_session)

        self.batch = self.session.client("batch")
        self.ec2 = self.session.client("ec2")
        self.ec2r = self.session.resource("ec2")
        self.emr = self.session.client("emr")
        self.step_functions = self.session.client("stepfunctions")
        self.aws_lambda = self.session.client("lambda")
        self.s3 = self.session.client("s3")
        self.s3_res = self.session.resource("s3")

        self.task_role_arn = None
        self.job_definition_arn = None
        self.instance_role_arn = None
        self.spot_service_role_arn = None
        self.service_role_arn = None
        self.instance_profile_arn = None
        self.job_queue_arn = None

        logger.propagate = False

    def __repr__(self):
        return super().__repr__()

    def create_emr_lambda_roles(self):
        """
        Create supporting IAM roles for Lambda support.
        """

        # EMR

        lambda_policy = {
            "Version": "2012-10-17",
            "Statement": [
                {
                    "Effect": "Allow",
                    "Action": "logs:CreateLogGroup",
                    "Resource": f"arn:aws:logs:{self.region}:{self.account}:*",
                },
                {
                    "Effect": "Allow",
                    "Action": ["logs:CreateLogStream", "logs:PutLogEvents"],
                    "Resource": [f"arn:aws:logs:{self.region}:{self.account}:log-group:/aws/lambda/launchemr:*"],
                },
                {
                    "Effect": "Allow",
                    "Action": "elasticmapreduce:RunJobFlow",
                    "Resource": "*",
                },
                {
                    "Effect": "Allow",
                    "Action": "iam:PassRole",
                    "Resource": [
                        f"arn:aws:iam::{self.account}:role/EMR_DefaultRole",
                        f"arn:aws:iam::{self.account}:role/EMR_EC2_DefaultRole",
                        f"arn:aws:iam::{self.account}:role/EMR_AutoScaling_DefaultRole",
                        self.emr_job_flow_role_arn,
                        self.emr_service_role_arn,
                    ],
                },
                {
                    "Effect": "Allow",
                    "Action": "s3:GetObject",
                    "Resource": [f"arn:aws:s3:::{self.s3_bucket}/*"],
                },
            ],
        }

        self.lambda_emr_job_step_execution_role_arn = self.iam_helper.role_stitcher(
            self.lambda_emr_job_step_execution_role,
            "lambda",
            f"Lambda execution role for {self.lambda_emr_job_step_function_name}",
            policies_list=[json.dumps(lambda_policy, indent=4)],
        )

    def create_vpc(self):
        cidrs_in_use = set()
        vpc_response = AWSRetry.backoff()(self.ec2.describe_vpcs)()
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

        response = self.ec2.create_vpc(
            CidrBlock=self.vpc_cidr,
            AmazonProvidedIpv6CidrBlock=False,
            InstanceTenancy="default",
        )
        self.vpc_id = response["Vpc"]["VpcId"]
        logger.info(f"VPC {self.vpc_id} created")

        while True:
            try:
                self.ec2.create_tags(
                    Resources=[self.vpc_id],
                    Tags=[{"Key": "Name", "Value": self.job_identifier}],
                )
                break
            except Exception as e:
                if "InvalidVpcID.NotFound" in str(e):
                    logger.info("Cannot tag VPC.  VPC not yet created. Sleeping...")
                    time.sleep(5)
                else:
                    raise

        # Find the default security group

        sec_response = self.ec2.describe_security_groups(
            Filters=[
                {"Name": "vpc-id", "Values": [self.vpc_id]},
            ]
        )

        self.batch_security_group = sec_response["SecurityGroups"][0]["GroupId"]

        logger.info(f"Security group {self.batch_security_group} created for vpc/job.")

        response = self.ec2.authorize_security_group_ingress(
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

        priv_response_1 = self.ec2.create_subnet(
            CidrBlock=self.priv_subnet_cidr_1,
            AvailabilityZone=f"{self.region}a",
            VpcId=self.vpc_id,
        )

        self.priv_vpc_subnet_id_1 = priv_response_1["Subnet"]["SubnetId"]

        logger.info("Private subnet created.")

        priv_response_2 = self.ec2.create_subnet(
            CidrBlock=self.priv_subnet_cidr_2,
            AvailabilityZone=f"{self.region}b",
            VpcId=self.vpc_id,
        )

        self.priv_vpc_subnet_id_2 = priv_response_2["Subnet"]["SubnetId"]

        logger.info("Private subnet created.")

        self.ec2.create_tags(
            Resources=[self.priv_vpc_subnet_id_1],
            Tags=[{"Key": "Name", "Value": self.job_identifier}],
        )

        self.ec2.create_tags(
            Resources=[self.priv_vpc_subnet_id_2],
            Tags=[{"Key": "Name", "Value": self.job_identifier}],
        )

        ig_response = self.ec2.create_internet_gateway()

        self.internet_gateway_id = ig_response["InternetGateway"]["InternetGatewayId"]

        AWSRetry.backoff()(self.ec2.create_tags)(
            Resources=[self.internet_gateway_id],
            Tags=[{"Key": "Name", "Value": self.job_identifier}],
        )

        logger.info(f"Internet gateway {self.internet_gateway_id} created.")

        # Create the public subnet

        pub_response = self.ec2.create_subnet(CidrBlock=self.pub_subnet_cidr, VpcId=self.vpc_id)

        logger.info("EIP allocated.")

        self.pub_vpc_subnet_id = pub_response["Subnet"]["SubnetId"]

        self.ec2.create_tags(
            Resources=[self.pub_vpc_subnet_id],
            Tags=[{"Key": "Name", "Value": self.job_identifier}],
        )

        # Create and elastic IP for the NAT Gateway

        try:
            ip_response = self.ec2.allocate_address(Domain="vpc")

            self.nat_ip_allocation = ip_response["AllocationId"]

            logger.info("EIP allocated.")

            self.ec2.create_tags(
                Resources=[self.nat_ip_allocation],
                Tags=[{"Key": "Name", "Value": self.job_identifier}],
            )

        except Exception as e:
            if "AddressLimitExceeded" in str(e):
                raise

        # Create an internet gateway

        self.ec2.attach_internet_gateway(InternetGatewayId=self.internet_gateway_id, VpcId=self.vpc_id)

        logger.info("Internet Gateway attached.")

        # Find the only/default route table

        drt_response = self.ec2.describe_route_tables(
            Filters=[
                {"Name": "vpc-id", "Values": [self.vpc_id]},
            ]
        )

        self.pub_route_table_id = drt_response["RouteTables"][0]["RouteTableId"]

        # Modify the default route table to be used as the public route

        while True:
            try:
                self.ec2.create_route(
                    DestinationCidrBlock="0.0.0.0/0",
                    GatewayId=self.internet_gateway_id,
                    RouteTableId=self.pub_route_table_id,
                )
                logger.info("Route created for Internet Gateway.")
                break

            except Exception as e:
                if "NotFound" in str(e):
                    time.sleep(5)
                    logger.info("Internet Gateway not yet created. Sleeping...")
                else:
                    raise

        # Create a NAT Gateway

        nat_response = self.ec2.create_nat_gateway(AllocationId=self.nat_ip_allocation, SubnetId=self.pub_vpc_subnet_id)

        self.nat_gateway_id = nat_response["NatGateway"]["NatGatewayId"]

        logger.info("NAT Gateway created.")

        # Create a new private route table

        prt_response = self.ec2.create_route_table(VpcId=self.vpc_id)

        self.priv_route_table_id = prt_response["RouteTable"]["RouteTableId"]

        logger.info("Route table created.")

        AWSRetry.backoff()(self.ec2.create_tags)(
            Resources=[self.priv_route_table_id],
            Tags=[{"Key": "Name", "Value": self.job_identifier}],
        )

        # Associate the private route to the private subnet

        self.ec2.associate_route_table(RouteTableId=self.priv_route_table_id, SubnetId=self.priv_vpc_subnet_id_1)
        logger.info("Route table associated with subnet.")

        self.ec2.associate_route_table(RouteTableId=self.priv_route_table_id, SubnetId=self.priv_vpc_subnet_id_2)
        logger.info("Route table associated with subnet.")

        # Associate the NAT gateway with the private route

        while True:
            try:
                self.ec2.create_route(
                    DestinationCidrBlock="0.0.0.0/0",
                    NatGatewayId=self.nat_gateway_id,
                    RouteTableId=self.priv_route_table_id,
                )
                logger.info("Route created for subnet.")
                break
            except Exception as e:
                if "InvalidNatGatewayID.NotFound" in str(e):
                    time.sleep(5)
                    logger.info("Nat Gateway not yet created. Sleeping...")
                else:
                    raise

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

        try:
            compute_resources = {
                "minvCpus": 0,
                "maxvCpus": maxCPUs,
                "desiredvCpus": 0,
                "instanceTypes": [
                    "optimal",
                ],
                "imageId": self.batch_compute_environment_ami,
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

            self.batch.create_compute_environment(
                computeEnvironmentName=self.batch_compute_environment_name,
                type="MANAGED",
                state="ENABLED",
                computeResources=compute_resources,
                serviceRole=self.service_role_arn,
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
                    logger.warning(
                        "5 second sleep initiated to wait for compute environment creation due to error: " + str(e)
                    )
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
        )

        self.job_definition_arn = response["jobDefinitionArn"]

    def submit_job(self, array_size=4):
        """
        Submits the created job definition and version to be run.
        """

        while True:
            try:
                self.batch.submit_job(
                    jobName=self.job_identifier,
                    jobQueue=self.batch_job_queue_name,
                    arrayProperties={"size": array_size},
                    jobDefinition=self.job_definition_arn,
                )

                logger.info(f"Job {self.job_identifier} submitted.")
                break

            except Exception as e:
                if "not in VALID state" in str(e):
                    # Need to wait a second for the compute environment to complete registration
                    logger.warning("5 second sleep initiated to wait for job queue creation due to error: " + str(e))
                    time.sleep(5)
                else:
                    raise

    def create_state_machine_roles(self):
        lambda_policy = f"""{{
    "Version": "2012-10-17",
    "Statement": [
        {{
            "Effect": "Allow",
            "Action": [
                "lambda:InvokeFunction"
            ],
            "Resource": [
                "arn:aws:lambda:*:*:function:{self.lambda_emr_job_step_function_name}"
            ]
        }}
    ]
}}

        """

        batch_policy = """{
    "Version": "2012-10-17",
    "Statement": [
        {
            "Effect": "Allow",
            "Action": [
                "batch:SubmitJob",
                "batch:DescribeJobs",
                "batch:TerminateJob"
            ],
            "Resource": "*"
        },
        {
            "Effect": "Allow",
            "Action": [
                "events:PutTargets",
                "events:PutRule",
                "events:DescribeRule"
            ],
            "Resource": [
               "arn:aws:events:*:*:rule/StepFunctionsGetEventsForBatchJobsRule"
            ]
        }
    ]
}

        """

        sns_policy = f"""{{
            "Version": "2012-10-17",
            "Statement": [
                {{
                    "Effect": "Allow",
                    "Action": [
                        "sns:Publish"
                    ],
                    "Resource": "arn:aws:sns:*:*:{self.sns_state_machine_topic}"
                }}
            ]
        }}
        """

        policies_list = [lambda_policy, batch_policy, sns_policy]

        self.state_machine_role_arn = self.iam_helper.role_stitcher(
            self.state_machine_role_name,
            "states",
            "Permissions for statemachine to run jobs",
            policies_list=policies_list,
        )

    def create_state_machine(self):
        job_definition = f"""{{
  "Comment": "An example of the Amazon States Language for notification on an AWS Batch job completion",
  "StartAt": "Submit Batch Job",
  "States": {{
    "Submit Batch Job": {{
      "Type": "Task",
      "Resource": "arn:aws:states:::batch:submitJob.sync",
      "Parameters": {{
        "JobDefinition": "{self.job_definition_arn}",
        "JobName": "{self.job_identifier}",
        "JobQueue": "{self.job_queue_arn}",
         "ArrayProperties": {{
                        "Size.$": "$.array_size"
                    }}
    }},
      "Next": "Notify Batch Success",
      "Catch": [
        {{
          "ErrorEquals": [ "States.ALL" ],
          "Next": "Notify Batch Failure"
        }}
      ]
    }},
    "Notify Batch Success": {{
      "Type": "Task",
      "Resource": "arn:aws:states:::sns:publish",
      "Parameters": {{
        "Message": "Batch job submitted through Step Functions succeeded",
        "TopicArn": "arn:aws:sns:{self.region}:{self.account}:{self.sns_state_machine_topic}"
      }},
      "Next": "Run EMR Job"
    }},
    "Notify Batch Failure": {{
      "Type": "Task",
      "Resource": "arn:aws:states:::sns:publish",
      "Parameters": {{
        "Message": "Batch job submitted through Step Functions failed",
        "TopicArn": "arn:aws:sns:{self.region}:{self.account}:{self.sns_state_machine_topic}"
      }},
      "Next": "Job Failure"
    }},
    "Run EMR Job": {{
      "Type": "Task",
      "Resource": "arn:aws:lambda:{self.region}:{self.account}:function:{self.lambda_emr_job_step_function_name}",
      "Next": "Notify EMR Job Success",
      "Catch": [
        {{
          "ErrorEquals": [ "States.ALL" ],
          "Next": "Notify EMR Job Failure"
        }}
      ]
    }},
    "Notify EMR Job Success": {{
      "Type": "Task",
      "Resource": "arn:aws:states:::sns:publish",
      "Parameters": {{
        "Message": "EMR Job succeeded",
        "TopicArn": "arn:aws:sns:{self.region}:{self.account}:{self.sns_state_machine_topic}"
      }},
      "End": true
    }},
    "Notify EMR Job Failure": {{
      "Type": "Task",
      "Resource": "arn:aws:states:::sns:publish",
      "Parameters": {{
        "Message": "EMR job failed",
        "TopicArn": "arn:aws:sns:{self.region}:{self.account}:{self.sns_state_machine_topic}"
      }},
      "Next": "Job Failure"
    }},
    "Job Failure": {{
      "Type": "Fail"
    }}
  }}
}}

        """

        while True:
            try:
                response = self.step_functions.create_state_machine(
                    name=self.state_machine_name,
                    definition=job_definition,
                    roleArn=self.state_machine_role_arn,
                )

                # print(response)
                self.state_machine_arn = response["stateMachineArn"]
                logger.info(f"State machine {self.state_machine_name} created.")
                break
            except Exception as e:
                if "AccessDeniedException" in str(e):
                    logger.info("State machine role not yet registered, sleeping...")
                    time.sleep(5)
                elif "StateMachineAlreadyExists" in str(e):
                    logger.info("State machine already exists, skipping...")
                    self.state_machine_arn = f"arn:aws:states:{self.region}:{self.account}:stateMachine:{self.state_machine_name}"  # noqa E501

                    break
                else:
                    raise

    def start_state_machine_execution(self, array_size):
        self.step_functions.start_execution(
            stateMachineArn=self.state_machine_arn,
            name=f"{self.state_machine_name}_execution_{int(time.time())}",
            input=f'{{"array_size": {array_size}}}',
        )

        logger.info(f"Starting state machine {self.state_machine_name}.")

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

        logger.info("Cleaning up EMR.")

        try:
            self.emr.terminate_job_flows(JobFlowIds=[self.emr_cluster_name])
            logger.info(f"EMR cluster {self.emr_cluster_name} deleted.")

        except Exception as e:
            if "ResourceNotFoundException" in str(e):
                logger.info(f"EMR cluster {self.emr_cluster_name} already MIA - skipping...")

        self.iam_helper.remove_role_from_instance_profile(self.emr_instance_profile_name)
        self.iam_helper.delete_instance_profile(self.emr_instance_profile_name)
        self.iam_helper.delete_role(self.emr_job_flow_role_name)
        self.iam_helper.delete_role(self.emr_service_role_name)

        logger.info(f"EMR clean complete.  Results bucket and data {self.s3_bucket} have not been deleted.")

        logger.info(f"Deleting Security group {self.emr_cluster_security_group_name}.")
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

        sg_response = AWSRetry.backoff()(self.ec2.describe_security_groups)(
            Filters=[
                {
                    "Name": "group-name",
                    "Values": [
                        self.emr_cluster_security_group_name,
                    ],
                },
            ]
        )

        try:
            group_id = sg_response["SecurityGroups"][0]["GroupId"]
            sg = self.ec2r.SecurityGroup(group_id)
            if len(sg.ip_permissions):
                sg.revoke_ingress(IpPermissions=sg.ip_permissions)

            while True:
                try:
                    self.ec2.delete_security_group(GroupId=group_id)
                    break
                except ClientError:
                    logger.info("Waiting for security group ingress rules to be removed ...")
                    time.sleep(5)

            logger.info(f"Deleted security group {self.emr_cluster_security_group_name}.")
        except Exception as e:
            if "does not exist" in str(e) or "list index out of range" in str(e):
                logger.info(f"Security group {self.emr_cluster_security_group_name} does not exist - skipping...")
            else:
                raise

        try:
            self.aws_lambda.delete_function(FunctionName=self.lambda_emr_job_step_function_name)
        except Exception as e:
            if "Function not found" in str(e):
                logger.info(f"Function {self.lambda_emr_job_step_function_name} not found, skipping...")
            else:
                raise

        try:
            self.s3.delete_object(Bucket=self.s3_bucket, Key=self.s3_lambda_code_emr_cluster_key)
            logger.info(
                f"S3 object {self.s3_lambda_code_emr_cluster_key} for bucket {self.s3_bucket} deleted."  # noqa E501
            )
        except Exception as e:
            if "NoSuchBucket" in str(e):
                logger.info(
                    f"S3 object {self.s3_lambda_code_emr_cluster_key} for bucket {self.s3_bucket} missing - not deleted."  # noqa E501
                )
            else:
                raise

        self.iam_helper.delete_role(self.lambda_emr_job_step_execution_role)

        state_machines = self.step_functions.list_state_machines()

        for sm in state_machines["stateMachines"]:
            if sm["name"] == self.state_machine_name:
                self.state_machine_arn = sm["stateMachineArn"]
                self.step_functions.delete_state_machine(stateMachineArn=self.state_machine_arn)
                logger.info(f"Deleted state machine {self.state_machine_name}.")
                break

        self.iam_helper.delete_role(self.state_machine_role_name)

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

        self.iam_helper.delete_role(self.batch_service_role_name)
        self.iam_helper.delete_role(self.batch_spot_service_role_name)
        self.iam_helper.delete_role(self.batch_ecs_task_role_name)
        # Instance profile order of removal
        self.iam_helper.remove_role_from_instance_profile(self.batch_instance_profile_name)
        self.iam_helper.delete_role(self.batch_instance_role_name)
        self.iam_helper.delete_instance_profile(self.batch_instance_profile_name)

        # Find Nat Gateways and VPCs

        response = AWSRetry.backoff()(self.ec2.describe_vpcs)(
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

            ng_response = AWSRetry.backoff()(self.ec2.describe_nat_gateways)(
                Filters=[{"Name": "vpc-id", "Values": [this_vpc]}]
            )

            for natgw in ng_response["NatGateways"]:
                this_natgw = natgw["NatGatewayId"]

                if natgw["State"] != "deleted":
                    self.ec2.delete_nat_gateway(NatGatewayId=this_natgw)

            rtas_response = AWSRetry.backoff()(self.ec2.describe_route_tables)(
                Filters=[{"Name": "vpc-id", "Values": [this_vpc]}]
            )

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
                                        "Waiting for association to be released before deleting route table.  "
                                        "Sleeping..."
                                    )
                                    time.sleep(5)
                                else:
                                    raise

            igw_response = AWSRetry.backoff()(self.ec2.describe_internet_gateways)(
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

            AWSRetry.backoff()(self.ec2.delete_vpc)(VpcId=this_vpc)

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

    def create_emr_security_groups(self):
        try:
            response = self.ec2.create_security_group(
                Description="EMR Job Flow Security Group (full cluster access)",
                GroupName=self.emr_cluster_security_group_name,
                VpcId=self.vpc_id,
            )
            self.emr_cluster_security_group_id = response["GroupId"]

        except Exception as e:
            if "already exists for VPC" in str(e):
                logger.info("Security group for EMR already exists, skipping ...")
                response = self.ec2.describe_security_groups(
                    Filters=[
                        {
                            "Name": "group-name",
                            "Values": [
                                self.emr_cluster_security_group_name,
                            ],
                        },
                    ]
                )

                self.emr_cluster_security_group_id = response["SecurityGroups"][0]["GroupId"]
            else:
                raise

        try:
            response = self.ec2.authorize_security_group_ingress(
                GroupId=self.emr_cluster_security_group_id,
                IpPermissions=[
                    dict(
                        IpProtocol="-1",
                        UserIdGroupPairs=[
                            dict(
                                GroupId=self.emr_cluster_security_group_id,
                                UserId=self.account,
                            )
                        ],
                    )
                ],
            )
        except Exception as e:
            if "already exists" in str(e):
                logger.info("Security group egress rule for EMR already exists, skipping ...")
            else:
                raise

    def create_emr_iam_roles(self):
        self.emr_service_role_arn = self.iam_helper.role_stitcher(
            self.emr_service_role_name,
            "elasticmapreduce",
            f"EMR Service Role {self.job_identifier}",
            managed_policie_arns=["arn:aws:iam::aws:policy/service-role/AmazonElasticMapReduceRole"],
        )

        emr_policy = """{
    "Version": "2012-10-17",
    "Statement": [
        {
            "Sid": "VisualEditor0",
            "Effect": "Allow",
            "Action": [
                "glue:GetCrawler",
                "glue:CreateTable",
                "glue:DeleteCrawler",
                "glue:StartCrawler",
                "glue:StopCrawler",
                "glue:DeleteTable",
                "glue:ListCrawlers",
                "glue:UpdateCrawler",
                "glue:CreateCrawler",
                "glue:GetCrawlerMetrics",
                "glue:BatchDeleteTable"
            ],
            "Resource": "*"
        },
        {
            "Sid": "VisualEditor1",
            "Effect": "Allow",
            "Action": [
                "iam:PassRole"
            ],
            "Resource": "arn:aws:iam::*:role/service-role/AWSGlueServiceRole-default"
        }
    ]
}"""

        self.emr_job_flow_role_arn = self.iam_helper.role_stitcher(
            self.emr_job_flow_role_name,
            "ec2",
            f"EMR Job Flow Role {self.job_identifier}",
            managed_policie_arns=["arn:aws:iam::aws:policy/service-role/AmazonElasticMapReduceforEC2Role"],
            policies_list=[emr_policy],
        )

        try:
            response = self.iam.create_instance_profile(InstanceProfileName=self.emr_instance_profile_name)

            self.emr_instance_profile_arn = response["InstanceProfile"]["Arn"]

            logger.info("EMR Instance Profile created")

            response = self.iam.add_role_to_instance_profile(
                InstanceProfileName=self.emr_instance_profile_name,
                RoleName=self.emr_job_flow_role_name,
            )

        except Exception as e:
            if "EntityAlreadyExists" in str(e):
                logger.info("EMR Instance Profile not created - already exists")
                response = self.iam.get_instance_profile(InstanceProfileName=self.emr_instance_profile_name)
                self.emr_instance_profile_arn = response["InstanceProfile"]["Arn"]

    def upload_assets(self):
        logger.info("Uploading EMR support assets...")
        fs = S3FileSystem()
        here = os.path.dirname(os.path.abspath(__file__))
        emr_folder = f"{self.s3_bucket}/{self.s3_bucket_prefix}/{self.s3_emr_folder_name}"
        fs.makedirs(emr_folder)

        # bsb_post.sh
        bsb_post_bash = f"""#!/bin/bash

aws s3 cp "s3://{self.s3_bucket}/{self.s3_bucket_prefix}/emr/bsb_post.py" bsb_post.py
/home/hadoop/miniconda/bin/python bsb_post.py "{self.s3_bucket}" "{self.s3_bucket_prefix}"

        """
        with fs.open(f"{emr_folder}/bsb_post.sh", "w", encoding="utf-8") as f:
            f.write(bsb_post_bash)

        # bsb_post.py
        fs.put(os.path.join(here, "s3_assets", "bsb_post.py"), f"{emr_folder}/bsb_post.py")

        # bootstrap-dask-custom
        fs.put(
            os.path.join(here, "s3_assets", "bootstrap-dask-custom"),
            f"{emr_folder}/bootstrap-dask-custom",
        )

        # postprocessing.py
        with fs.open(f"{emr_folder}/postprocessing.tar.gz", "wb") as f:
            with tarfile.open(fileobj=f, mode="w:gz") as tarf:
                tarf.add(
                    os.path.join(here, "..", "postprocessing.py"),
                    arcname="postprocessing.py",
                )
                tarf.add(
                    os.path.join(here, "s3_assets", "setup_postprocessing.py"),
                    arcname="setup.py",
                )

        logger.info("EMR support assets uploaded.")

    def create_emr_cluster_function(self):
        script_name = f"s3://{self.s3_bucket}/{self.s3_bucket_prefix}/{self.s3_emr_folder_name}/bsb_post.sh"
        bootstrap_action = f"s3://{self.s3_bucket}/{self.s3_bucket_prefix}/{self.s3_emr_folder_name}/bootstrap-dask-custom"  # noqa E501

        run_job_flow_args = dict(
            Name=self.emr_cluster_name,
            LogUri=self.emr_log_uri,
            ReleaseLabel="emr-5.23.0",
            Instances={
                "InstanceGroups": [
                    {
                        "Market": "SPOT" if self.batch_use_spot else "ON_DEMAND",
                        "InstanceRole": "MASTER",
                        "InstanceType": self.emr_manager_instance_type,
                        "InstanceCount": 1,
                    },
                    {
                        "Market": "SPOT" if self.batch_use_spot else "ON_DEMAND",
                        "InstanceRole": "CORE",
                        "InstanceType": self.emr_worker_instance_type,
                        "InstanceCount": self.emr_worker_instance_count,
                    },
                ],
                "Ec2SubnetId": self.priv_vpc_subnet_id_1,
                "KeepJobFlowAliveWhenNoSteps": False,
                "EmrManagedMasterSecurityGroup": self.emr_cluster_security_group_id,
                "EmrManagedSlaveSecurityGroup": self.emr_cluster_security_group_id,
                "ServiceAccessSecurityGroup": self.batch_security_group,
            },
            Applications=[
                {"Name": "Hadoop"},
            ],
            BootstrapActions=[
                {
                    "Name": "launchFromS3",
                    "ScriptBootstrapAction": {
                        "Path": bootstrap_action,
                        "Args": [f"s3://{self.s3_bucket}/{self.s3_bucket_prefix}/emr/postprocessing.tar.gz"],
                    },
                },
            ],
            Steps=[
                {
                    "Name": "Dask",
                    "ActionOnFailure": "TERMINATE_CLUSTER",
                    "HadoopJarStep": {
                        "Jar": "s3://us-east-1.elasticmapreduce/libs/script-runner/script-runner.jar",
                        "Args": [script_name],
                    },
                },
            ],
            VisibleToAllUsers=True,
            JobFlowRole=self.emr_instance_profile_name,
            ServiceRole=self.emr_service_role_name,
            Tags=[
                {"Key": "org", "Value": "ops"},
            ],
            AutoScalingRole="EMR_AutoScaling_DefaultRole",
            ScaleDownBehavior="TERMINATE_AT_TASK_COMPLETION",
            EbsRootVolumeSize=100,
        )

        with io.BytesIO() as f:
            f.write(json.dumps(run_job_flow_args).encode())
            f.seek(0)
            self.s3.upload_fileobj(f, self.s3_bucket, self.s3_lambda_emr_config_key)

        lambda_filename = os.path.join(
            os.path.dirname(os.path.abspath(__file__)),
            "s3_assets",
            "lambda_function.py",
        )
        with open(lambda_filename, "r") as f:
            function_script = f.read()
        with io.BytesIO() as f:
            with zipfile.ZipFile(f, mode="w", compression=zipfile.ZIP_STORED) as zf:
                zi = zipfile.ZipInfo("emr_function.py")
                zi.date_time = time.localtime()
                zi.external_attr = 0o100755 << 16
                zf.writestr(zi, function_script, zipfile.ZIP_DEFLATED)
            f.seek(0)
            self.s3.upload_fileobj(f, self.s3_bucket, self.s3_lambda_code_emr_cluster_key)

        while True:
            try:
                self.aws_lambda.create_function(
                    FunctionName=self.lambda_emr_job_step_function_name,
                    Runtime="python3.7",
                    Role=self.lambda_emr_job_step_execution_role_arn,
                    Handler="emr_function.lambda_handler",
                    Code={
                        "S3Bucket": self.s3_bucket,
                        "S3Key": self.s3_lambda_code_emr_cluster_key,
                    },
                    Description=f"Lambda for emr cluster execution on job {self.job_identifier}",
                    Timeout=900,
                    MemorySize=128,
                    Publish=True,
                    Environment={
                        "Variables": {
                            "REGION": self.region,
                            "BUCKET": self.s3_bucket,
                            "EMR_CONFIG_JSON_KEY": self.s3_lambda_emr_config_key,
                        }
                    },
                    Tags={"job": self.job_identifier},
                )

                logger.info(f"Lambda function {self.lambda_emr_job_step_function_name} created.")
                break

            except Exception as e:
                if "role defined for the function cannot be assumed" in str(e):
                    logger.info(
                        f"Lambda role not registered for {self.lambda_emr_job_step_function_name} - sleeping ..."
                    )
                    time.sleep(5)
                elif "Function already exist" in str(e):
                    logger.info(f"Lambda function {self.lambda_emr_job_step_function_name} exists, skipping...")
                    break
                elif "ARN does not refer to a valid principal" in str(e):
                    logger.info("Waiting for roles/permissions to propagate to allow Lambda function creation ...")
                    time.sleep(5)
                else:
                    raise


class AwsSNS(AwsJobBase):
    def __init__(self, job_name, aws_config, boto3_session):
        super().__init__(job_name, aws_config, boto3_session)
        self.sns = self.session.client("sns")
        self.sns_state_machine_topic_arn = None

    def create_topic(self):
        response = self.sns.create_topic(Name=self.sns_state_machine_topic)

        logger.info(f"Simple notifications topic {self.sns_state_machine_topic} created.")

        self.sns_state_machine_topic_arn = response["TopicArn"]

    def subscribe_to_topic(self):
        self.sns.subscribe(
            TopicArn=self.sns_state_machine_topic_arn,
            Protocol="email",
            Endpoint=self.operator_email,
        )

        logger.info(
            f"Operator {self.operator_email} subscribed to topic - please confirm via email to recieve state machine progress messages."  # noqa 501
        )

    def clean(self):
        self.sns.delete_topic(TopicArn=f"arn:aws:sns:{self.region}:{self.account}:{self.sns_state_machine_topic}")

        logger.info(f"Simple notifications topic {self.sns_state_machine_topic} deleted.")


class AwsBatch(DockerBatchBase):
    def __init__(self, project_filename):
        super().__init__(project_filename)

        self.job_identifier = re.sub("[^0-9a-zA-Z]+", "_", self.cfg["aws"]["job_identifier"])[:10]

        self.project_filename = project_filename
        self.region = self.cfg["aws"]["region"]
        self.ecr = boto3.client("ecr", region_name=self.region)
        self.s3 = boto3.client("s3", region_name=self.region)
        self.s3_bucket = self.cfg["aws"]["s3"]["bucket"]
        self.s3_bucket_prefix = self.cfg["aws"]["s3"]["prefix"].rstrip("/")
        self.batch_env_use_spot = self.cfg["aws"]["use_spot"]
        self.batch_array_size = self.cfg["aws"]["batch_array_size"]
        self.boto3_session = boto3.Session(region_name=self.region)

    @staticmethod
    def validate_instance_types(project_file):
        cfg = get_project_configuration(project_file)
        aws_config = cfg["aws"]
        boto3_session = boto3.Session(region_name=aws_config["region"])
        ec2 = boto3_session.client("ec2")
        job_base = AwsJobBase("genericjobid", aws_config, boto3_session)
        instance_types_requested = set()
        instance_types_requested.add(job_base.emr_manager_instance_type)
        instance_types_requested.add(job_base.emr_worker_instance_type)
        inst_type_resp = ec2.describe_instance_type_offerings(
            Filters=[{"Name": "instance-type", "Values": list(instance_types_requested)}]
        )
        instance_types_available = set([x["InstanceType"] for x in inst_type_resp["InstanceTypeOfferings"]])
        if not instance_types_requested == instance_types_available:
            instance_types_not_available = instance_types_requested - instance_types_available
            raise ValidationError(
                f"The instance type(s) {', '.join(instance_types_not_available)} are not available in region {aws_config['region']}."  # noqa E501
            )

    @staticmethod
    def validate_project(project_file):
        super(AwsBatch, AwsBatch).validate_project(project_file)
        AwsBatch.validate_instance_types(project_file)

    @property
    def docker_image(self):
        return "nrel/buildstockbatch"

    @property
    def weather_dir(self):
        return self._weather_dir

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

    def build_image(self):
        """
        Build the docker image to use in the batch simulation
        """
        root_path = pathlib.Path(os.path.abspath(__file__)).parent.parent.parent
        if not (root_path / "Dockerfile").exists():
            raise RuntimeError(f"The needs to be run from the root of the repo, found {root_path}")
        logger.debug("Building docker image")
        self.docker_client.images.build(path=str(root_path), tag=self.docker_image, rm=True)

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

        sns_env = AwsSNS(self.job_identifier, self.cfg["aws"], self.boto3_session)
        sns_env.clean()

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

        image_url = "{}:{}".format(self.container_repo["repositoryUri"], self.job_identifier)

        job_env_cfg = self.cfg["aws"].get("job_environment", {})
        batch_env.create_job_definition(
            image_url,
            command=["python3.8", "-m", "buildstockbatch.aws.aws"],
            vcpus=job_env_cfg.get("vcpus", 1),
            memory=job_env_cfg.get("memory", 1024),
            env_vars=env_vars,
        )

        # SNS Topic
        sns_env = AwsSNS(self.job_identifier, self.cfg["aws"], self.boto3_session)
        sns_env.create_topic()
        sns_env.subscribe_to_topic()

        # State machine
        batch_env.create_state_machine_roles()
        batch_env.create_state_machine()

        # EMR Function
        batch_env.upload_assets()
        batch_env.create_emr_iam_roles()
        batch_env.create_emr_security_groups()
        batch_env.create_emr_lambda_roles()
        batch_env.create_emr_cluster_function()

        # start job
        batch_env.start_state_machine_execution(batch_info.job_count)

        logger.info("Batch job submitted. Check your email to subscribe to notifications.")

    @classmethod
    def run_job(cls, job_id, bucket, prefix, job_name, region):
        """
        Run a few simulations inside a container.

        This method is called from inside docker container in AWS. It will
        go get the necessary files from S3, run the simulation, and post the
        results back to S3.
        """

        logger.debug(f"region: {region}")
        s3 = boto3.client("s3")

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
                            "The epw files are specified in options_lookup.tsv under more than one parameter type: "
                            f"{param_name}, {row[0]}"
                        )
                    epw_filename = row[row_has_epw.index(True) + 2].split("=")[1]
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
        asset_dirs = os.listdir(sim_dir)

        fs = S3FileSystem()
        local_fs = LocalFileSystem()
        reporting_measures = cls.get_reporting_measures(cfg)
        dpouts = []
        simulation_output_tar_filename = sim_dir.parent / "simulation_outputs.tar.gz"
        with tarfile.open(str(simulation_output_tar_filename), "w:gz") as simout_tar:
            for building_id, upgrade_idx in jobs_d["batch"]:
                upgrade_id = 0 if upgrade_idx is None else upgrade_idx + 1
                sim_id = f"bldg{building_id:07d}up{upgrade_id:02d}"

                # Create OSW
                osw = cls.create_osw(cfg, jobs_d["n_datapoints"], sim_id, building_id, upgrade_idx)
                with open(os.path.join(sim_dir, "in.osw"), "w") as f:
                    json.dump(osw, f, indent=4)

                # Run Simulation
                with open(sim_dir / "os_stdout.log", "w") as f_out:
                    try:
                        logger.debug("Running {}".format(sim_id))
                        subprocess.run(
                            ["openstudio", "run", "-w", "in.osw"],
                            check=True,
                            stdout=f_out,
                            stderr=subprocess.STDOUT,
                            cwd=str(sim_dir),
                        )
                    except subprocess.CalledProcessError:
                        logger.debug(f"Simulation failed: see {sim_id}/os_stdout.log")

                # Clean Up simulation directory
                cls.cleanup_sim_dir(
                    sim_dir,
                    fs,
                    f"{bucket}/{prefix}/results/simulation_output/timeseries",
                    upgrade_id,
                    building_id,
                )

                # Read data_point_out.json
                dpout = postprocessing.read_simulation_outputs(
                    local_fs, reporting_measures, str(sim_dir), upgrade_id, building_id
                )
                dpouts.append(dpout)

                # Add the rest of the simulation outputs to the tar archive
                logger.info("Archiving simulation outputs")
                for dirpath, dirnames, filenames in os.walk(sim_dir):
                    if dirpath == str(sim_dir):
                        for dirname in set(dirnames).intersection(asset_dirs):
                            dirnames.remove(dirname)
                    for filename in filenames:
                        abspath = os.path.join(dirpath, filename)
                        relpath = os.path.relpath(abspath, sim_dir)
                        simout_tar.add(abspath, os.path.join(sim_id, relpath))

                # Clear directory for next simulation
                logger.debug("Clearing out simulation directory")
                for item in set(os.listdir(sim_dir)).difference(asset_dirs):
                    if os.path.isdir(item):
                        shutil.rmtree(item)
                    elif os.path.isfile(item):
                        os.remove(item)

        # Upload simulation outputs tarfile to s3
        fs.put(
            str(simulation_output_tar_filename),
            f"{bucket}/{prefix}/results/simulation_output/simulations_job{job_id}.tar.gz",
        )

        # Upload aggregated dpouts as a json file
        with fs.open(
            f"{bucket}/{prefix}/results/simulation_output/results_job{job_id}.json.gz",
            "wb",
        ) as f1:
            with gzip.open(f1, "wt", encoding="utf-8") as f2:
                json.dump(dpouts, f2)

        # Remove files (it helps docker if we don't leave a bunch of files laying around)
        os.remove(simulation_output_tar_filename)
        for item in os.listdir(sim_dir):
            if os.path.isdir(item):
                shutil.rmtree(item)
            elif os.path.isfile(item):
                os.remove(item)


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
    else:
        parser = argparse.ArgumentParser()
        parser.add_argument("project_filename")
        parser.add_argument(
            "-c",
            "--clean",
            action="store_true",
            help="After the simulation is done, run with --clean to clean up AWS environment",
        )
        parser.add_argument(
            "--validateonly",
            help="Only validate the project YAML file and references. Nothing is executed",
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
        else:
            batch.build_image()
            batch.push_image()
            batch.run_batch()


if __name__ == "__main__":
    main()
