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
from fs import open_fs
from fs.copy import copy_fs
from fs.walk import Walker
import gzip
import itertools
from joblib import Parallel, delayed
import json
import logging
import math
from multiprocessing import cpu_count
import os
import pandas as pd
import pathlib
import random
import shutil
import subprocess
import tarfile
import tempfile
import re
import time
import io

from buildstockbatch.localdocker import DockerBatchBase
from buildstockbatch.postprocessing import (
    read_data_point_out_json,
    to_camelcase,
    flatten_datapoint_json,
    read_out_osw
)

from buildstockbatch.aws.awsbase import (
    AwsJobBase
)

logger = logging.getLogger(__name__)


def upload_file_to_s3(*args, **kwargs):
    s3 = boto3.client('s3')
    s3.upload_file(*args, **kwargs)


def compress_file(in_filename, out_filename):
    with gzip.open(str(out_filename), 'wb') as f_out:
        with open(str(in_filename), 'rb') as f_in:
            shutil.copyfileobj(f_in, f_out)


class AwsDynamo(AwsJobBase):

    def __init__(self, job_name, aws_config, boto3_session):
        super().__init__(job_name, aws_config, boto3_session)
        self.dynamodb = self.session.client('dynamodb')

    def create_summary_table(self):

        # Does the table exist?  If so it will be replaced.
        try:
            self.dynamodb.delete_table(
                TableName=self.dynamo_table_name
            )

            logger.warning("Previously existing dynamo table deleted.")

        except Exception as e:
            if 'ResourceNotFoundException' not in str(e):
                raise

        while True:
            try:
                self.dynamodb.create_table(
                    AttributeDefinitions=[
                        {
                            'AttributeName': 'upgrade_idx',
                            'AttributeType': 'S'
                        },
                        {
                            'AttributeName': 'building_id',
                            'AttributeType': 'S'
                        },
                    ],
                    TableName=self.dynamo_table_name,
                    KeySchema=[
                        {
                            'AttributeName': 'upgrade_idx',
                            'KeyType': 'HASH'
                        },
                        {
                            'AttributeName': 'building_id',
                            'KeyType': 'RANGE'
                        },
                    ],
                    BillingMode='PAY_PER_REQUEST',
                )
                logger.info(f"Dynamo table {self.dynamo_table_name} created.")
                break
            except Exception as e:
                if 'ResourceInUseException' in str(e):
                    logger.info('Waiting for deletion of existing Dynamo table.  Sleeping...')
                    time.sleep(5)
                else:
                    raise

    def add_dynamo_task_permissions(self):
        dynamo_role_policy = f'''{{
            "Version": "2012-10-17",
            "Statement": [
                {{
                    "Sid": "TaskFH",
                    "Effect": "Allow",
                    "Action": [
                        "dynamodb:PutItem"
                    ],
                    "Resource": [
                        "{self.dynamo_table_arn}"
                    ]
                }}
            ]
        }}'''

        self.iam.put_role_policy(
            RoleName=self.batch_ecs_task_role_name,
            PolicyName=self.dynamo_task_policy_name,
            PolicyDocument=dynamo_role_policy
        )

    def clean(self):
        try:
            self.dynamodb.delete_table(
                TableName=self.dynamo_table_name
            )

            logger.info("Dynamo table deleted.")

        except Exception as e:
            if 'ResourceNotFoundException' not in str(e):
                raise


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

        self.batch = self.session.client('batch')
        self.ec2 = self.session.client('ec2')
        self.ec2r = self.session.resource('ec2')
        self.emr = self.session.client('emr')
        self.step_functions = self.session.client('stepfunctions')
        self.aws_lambda = self.session.client('lambda')
        self.s3 = self.session.client('s3')
        self.s3_res = self.session.resource('s3')

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

        lambda_policy = f'''{{
            "Version": "2012-10-17",
            "Statement": [
                {{
                    "Effect": "Allow",
                    "Action": "logs:CreateLogGroup",
                    "Resource": "arn:aws:logs:{self.region}:{self.account}:*"
                }},
                {{
                    "Effect": "Allow",
                    "Action": [
                        "logs:CreateLogStream",
                        "logs:PutLogEvents"
                    ],
                    "Resource": [
                        "arn:aws:logs:{self.region}:{self.account}:log-group:/aws/lambda/launchemr:*"
                    ]
                }},
                {{
                    "Effect": "Allow",
                    "Action": "elasticmapreduce:RunJobFlow",
                    "Resource": "*"
                }},
                {{

                    "Effect": "Allow",
                    "Action": "iam:PassRole",
                    "Resource": [
                        "arn:aws:iam::{self.account}:role/EMR_DefaultRole",
                        "arn:aws:iam::{self.account}:role/EMR_EC2_DefaultRole",
                        "arn:aws:iam::{self.account}:role/EMR_AutoScaling_DefaultRole",
                        "{self.emr_job_flow_role_arn}",
                        "{self.emr_service_role_arn}"
                    ]
                }}
            ]
        }}

        '''

        self.lambda_emr_job_step_execution_role_arn = self.iam_helper.role_stitcher(
            self.lambda_emr_job_step_execution_role,
            'lambda',
            f'Lambda execution role for {self.lambda_emr_job_step_function_name}',
            policies_list=[lambda_policy]
        )


    def create_vpc(self):

        cidrs_in_use = set()
        vpc_response = self.ec2.describe_vpcs()
        for vpc in vpc_response['Vpcs']:
            cidrs_in_use.add(vpc['CidrBlock'])
            for cidr_assoc in vpc['CidrBlockAssociationSet']:
                cidrs_in_use.add(cidr_assoc['CidrBlock'])

        need_to_find_cidr = True
        while need_to_find_cidr:
            self.vpc_cidr = '172.{}.0.0/16'.format(random.randrange(100, 200))
            need_to_find_cidr = self.vpc_cidr in cidrs_in_use

        self.pub_subnet_cidr = self.vpc_cidr.replace('/16', '/17')
        self.priv_subnet_cidr_1 = self.vpc_cidr.replace('.0.0/16', '.128.0/18')
        self.priv_subnet_cidr_2 = self.vpc_cidr.replace('.0.0/16', '.192.0/18')

        # Create the VPC

        response = self.ec2.create_vpc(
            CidrBlock=self.vpc_cidr,
            AmazonProvidedIpv6CidrBlock=False,
            InstanceTenancy='default'
        )
        self.vpc_id = response['Vpc']['VpcId']
        logger.info(f"VPC {self.vpc_id} created")

        while True:
            try:
                self.ec2.create_tags(
                    Resources=[
                        self.vpc_id
                    ],
                    Tags=[
                        {
                            'Key': 'Name',
                            'Value': self.job_identifier
                        }
                    ]
                )
                break
            except Exception as e:
                if 'InvalidVpcID.NotFound' in str(e):
                    logger.info("Cannot tag VPC.  VPC not yet created. Sleeping...")
                    time.sleep(5)
                else:
                    raise

        # Find the default security group

        sec_response = self.ec2.describe_security_groups(
            Filters=[
                {
                    'Name': 'vpc-id',
                    'Values': [
                        self.vpc_id
                    ]
                },
            ]
        )

        self.batch_security_group = sec_response['SecurityGroups'][0]['GroupId']

        logger.info(f'Security group {self.batch_security_group} created for vpc/job.')

        response = self.ec2.authorize_security_group_ingress(

            GroupId=self.batch_security_group,
            IpPermissions=[
                {
                    'FromPort': 0,
                    'IpProtocol': 'tcp',
                    'IpRanges': [
                        {
                            'CidrIp': '0.0.0.0/0'
                        },
                    ],

                    'ToPort': 65535
                },
            ]
        )

        # Create the private subnets

        priv_response_1 = self.ec2.create_subnet(
            CidrBlock=self.priv_subnet_cidr_1,
            AvailabilityZone=f"{self.region}a",
            VpcId=self.vpc_id
        )

        #print(priv_response_1)

        self.priv_vpc_subnet_id_1 = priv_response_1['Subnet']['SubnetId']

        logger.info("Private subnet created.")

        priv_response_2 = self.ec2.create_subnet(
            CidrBlock=self.priv_subnet_cidr_2,
            AvailabilityZone=f"{self.region}b",
            VpcId=self.vpc_id
        )

        self.priv_vpc_subnet_id_2 = priv_response_2['Subnet']['SubnetId']

        logger.info("Private subnet created.")

        self.ec2.create_tags(
            Resources=[
                self.priv_vpc_subnet_id_1
            ],
            Tags=[
                {
                    'Key': 'Name',
                    'Value': self.job_identifier
                }
            ]
        )

        self.ec2.create_tags(
            Resources=[
                self.priv_vpc_subnet_id_2
            ],
            Tags=[
                {
                    'Key': 'Name',
                    'Value': self.job_identifier
                }
            ]
        )

        ig_response = self.ec2.create_internet_gateway()

        self.internet_gateway_id = ig_response['InternetGateway']['InternetGatewayId']

        self.ec2.create_tags(
            Resources=[
                self.internet_gateway_id
            ],
            Tags=[
                {
                    'Key': 'Name',
                    'Value': self.job_identifier
                }
            ]
        )

        logger.info(f'Internet gateway {self.internet_gateway_id} created.')

        # Create the public subnet

        pub_response = self.ec2.create_subnet(
            CidrBlock=self.pub_subnet_cidr,
            VpcId=self.vpc_id
        )

        logger.info("EIP allocated.")

        self.pub_vpc_subnet_id = pub_response['Subnet']['SubnetId']

        self.ec2.create_tags(
            Resources=[
                self.pub_vpc_subnet_id
            ],
            Tags=[
                {
                    'Key': 'Name',
                    'Value': self.job_identifier
                }
            ]
        )

        # Create and elastic IP for the NAT Gateway

        try:

            ip_response = self.ec2.allocate_address(
                Domain='vpc'
            )

            self.nat_ip_allocation = ip_response['AllocationId']

            logger.info("EIP allocated.")

            self.ec2.create_tags(
                Resources=[
                    self.nat_ip_allocation
                ],
                Tags=[
                    {
                        'Key': 'Name',
                        'Value': self.job_identifier
                    }
                ]
            )

        except Exception as e:
            if 'AddressLimitExceeded' in str(e):
                raise

        # Create an internet gateway

        self.ec2.attach_internet_gateway(
            InternetGatewayId=self.internet_gateway_id,
            VpcId=self.vpc_id
        )

        logger.info("Internet Gateway attached.")

        # Find the only/default route table

        drt_response = self.ec2.describe_route_tables(
            Filters=[
                {
                    'Name': 'vpc-id',
                    'Values': [
                        self.vpc_id
                    ]
                },
            ]
        )

        self.pub_route_table_id = drt_response['RouteTables'][0]['RouteTableId']

        # Modify the default route table to be used as the public route

        while True:
            try:
                self.ec2.create_route(
                    DestinationCidrBlock='0.0.0.0/0',
                    GatewayId=self.internet_gateway_id,
                    RouteTableId=self.pub_route_table_id
                )
                logger.info("Route created for Internet Gateway.")
                break

            except Exception as e:
                if 'NotFound' in str(e):
                    time.sleep(5)
                    logger.info("Internet Gateway not yet created. Sleeping...")
                else:
                    raise

        # Create a NAT Gateway

        nat_response = self.ec2.create_nat_gateway(
            AllocationId=self.nat_ip_allocation,
            SubnetId=self.pub_vpc_subnet_id
        )

        self.nat_gateway_id = nat_response['NatGateway']['NatGatewayId']

        logger.info("NAT Gateway created.")

        # Create a new private route table

        prt_response = self.ec2.create_route_table(
            VpcId=self.vpc_id
        )

        self.priv_route_table_id = prt_response['RouteTable']['RouteTableId']

        logger.info("Route table created.")

        self.ec2.create_tags(
            Resources=[
                self.priv_route_table_id
            ],
            Tags=[
                {
                    'Key': 'Name',
                    'Value': self.job_identifier
                }
            ]
        )

        # Associate the private route to the private subnet

        self.ec2.associate_route_table(
            RouteTableId=self.priv_route_table_id,
            SubnetId=self.priv_vpc_subnet_id_1
        )
        logger.info("Route table associated with subnet.")

        self.ec2.associate_route_table(
            RouteTableId=self.priv_route_table_id,
            SubnetId=self.priv_vpc_subnet_id_2
        )
        logger.info("Route table associated with subnet.")

        # Associate the NAT gateway with the private route

        while True:
            try:
                self.ec2.create_route(
                    DestinationCidrBlock='0.0.0.0/0',
                    NatGatewayId=self.nat_gateway_id,
                    RouteTableId=self.priv_route_table_id
                )
                logger.info("Route created for subnet.")
                break
            except Exception as e:
                if 'InvalidNatGatewayID.NotFound' in str(e):
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
            managed_policie_arns=['arn:aws:iam::aws:policy/service-role/AWSBatchServiceRole']
        )

        # Instance Role for Batch compute environment

        self.instance_role_arn = self.iam_helper.role_stitcher(
            self.batch_instance_role_name,
            "ec2",
            f"Instance role for Batch compute environment {self.job_identifier}",
            managed_policie_arns=['arn:aws:iam::aws:policy/service-role/AmazonEC2ContainerServiceforEC2Role']
        )

        # Instance Profile

        try:
            response = self.iam.create_instance_profile(
                InstanceProfileName=self.batch_instance_profile_name
            )

            self.instance_profile_arn = response['InstanceProfile']['Arn']

            logger.info("Instance Profile created")

            response = self.iam.add_role_to_instance_profile(
                InstanceProfileName=self.batch_instance_profile_name,
                RoleName=self.batch_instance_role_name
            )

        except Exception as e:
            if 'EntityAlreadyExists' in str(e):
                logger.info('ECS Instance Profile not created - already exists')
                response = self.iam.get_instance_profile(
                    InstanceProfileName=self.batch_instance_profile_name
                )
                self.instance_profile_arn = response['InstanceProfile']['Arn']

        # ECS Task Policy

        task_permissions_policy = f'''{{
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
            }}'''

        self.task_role_arn = self.iam_helper.role_stitcher(self.batch_ecs_task_role_name,
                                                           "ecs-tasks",
                                                           f"Task role for Batch job {self.job_identifier}",
                                                           policies_list=[task_permissions_policy])

        if self.batch_use_spot != 'false':
            # Spot Fleet Role
            self.spot_service_role_arn = self.iam_helper.role_stitcher(
                self.batch_spot_service_role_name,
                "spotfleet",
                f"Spot Fleet role for Batch compute environment {self.job_identifier}",
                managed_policie_arns=['arn:aws:iam::aws:policy/service-role/AmazonEC2SpotFleetTaggingRole']
            )

    def create_compute_environment(self, maxCPUs=10000):
        """
        Creates a compute environment suffixed with the job name
        :param subnets: list of subnet IDs
        :param maxCPUs: numeric value for max VCPUs for the envionment

        """

        if self.batch_use_spot == 'true':
            type = 'SPOT'
            try:
                self.batch.create_compute_environment(
                    computeEnvironmentName=self.batch_compute_environment_name,
                    type='MANAGED',
                    state='ENABLED',
                    computeResources={
                        'type': type,
                        'minvCpus': 0,
                        'maxvCpus': maxCPUs,
                        'desiredvCpus': 0,
                        'instanceTypes': [
                            'optimal',
                        ],
                        'imageId': self.batch_compute_environment_ami,
                        'subnets': [self.priv_vpc_subnet_id_1, self.priv_vpc_subnet_id_2],
                        'securityGroupIds': [self.batch_security_group],
                        'instanceRole': self.instance_profile_arn,
                        'bidPercentage': 100,
                        'spotIamFleetRole': self.spot_service_role_arn
                    },
                    serviceRole=self.service_role_arn
                )

                logger.info('Service Role created')

            except Exception as e:
                if 'Object already exists' in str(e):
                    logger.info('Compute environment not created - already exists')
                else:
                    raise

        else:
            type = 'EC2'
            try:
                self.batch.create_compute_environment(
                    computeEnvironmentName=self.batch_compute_environment_name,
                    type='MANAGED',
                    state='ENABLED',
                    computeResources={
                        'type': type,
                        'minvCpus': 0,
                        'maxvCpus': maxCPUs,
                        'desiredvCpus': 0,
                        'instanceTypes': [
                            'optimal',
                        ],
                        'imageId': self.batch_compute_environment_ami,
                        'subnets': [self.priv_vpc_subnet_id_1, self.priv_vpc_subnet_id_2],
                        'securityGroupIds': [self.batch_security_group],
                        'ec2KeyPair': 'nrel-aws-dev-us-west-2',
                        'instanceRole': self.instance_profile_arn
                    },
                    serviceRole=self.service_role_arn
                )

                logger.info(f'Compute environment {self.batch_compute_environment_name} created.')

            except Exception as e:
                if 'Object already exists' in str(e):
                    logger.info(
                        f'Compute environment {self.batch_compute_environment_name} not created - already exists')
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
                    state='ENABLED',
                    priority=1,
                    computeEnvironmentOrder=[
                        {
                            'order': 1,
                            'computeEnvironment': self.batch_compute_environment_name
                        },
                    ]
                )

                # print("JOB QUEUE")
                # print(response)
                self.job_queue_arn = response['jobQueueArn']
                logger.info(f'Job queue {self.batch_job_queue_name} created')
                break

            except Exception as e:
                if 'Object already exists' in str(e):
                    logger.info(f'Job queue {self.batch_job_queue_name} not created - already exists')
                    response = self.batch.describe_job_queues(
                        jobQueues=[
                            self.batch_job_queue_name,
                        ]
                    )
                    self.job_queue_arn = response['jobQueues'][0]['jobQueueArn']
                    break

                elif 'is not valid' in str(e):
                    # Need to wait a second for the compute environment to complete registration
                    logger.warning(
                        '5 second sleep initiated to wait for compute environment creation due to error: ' + str(e))
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
            type='container',
            # parameters={
            #    'string': 'string'
            # },
            containerProperties={
                'image': docker_image,
                'vcpus': vcpus,
                'memory': memory,
                'command': command,
                'jobRoleArn': self.task_role_arn,
                'environment': self.generate_name_value_inputs(env_vars)
            },
            retryStrategy={
                'attempts': 5
            }
        )

        self.job_definition_arn = response['jobDefinitionArn']

    def submit_job(self, array_size=4):
        """
        Submits the created job definition and version to be run.
        """

        while True:
            try:
                self.batch.submit_job(
                    jobName=self.job_identifier,
                    jobQueue=self.batch_job_queue_name,
                    arrayProperties={
                        'size': array_size
                    },
                    jobDefinition=self.job_definition_arn
                )

                logger.info(f"Job {self.job_identifier} submitted.")
                break

            except Exception as e:

                if 'not in VALID state' in str(e):
                    # Need to wait a second for the compute environment to complete registration
                    logger.warning('5 second sleep initiated to wait for job queue creation due to error: ' + str(e))
                    time.sleep(5)
                else:
                    raise

    def create_state_machine_roles(self):

        lambda_policy = f'''{{
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

        '''

        batch_policy = f'''{{
    "Version": "2012-10-17",
    "Statement": [
        {{
            "Effect": "Allow",
            "Action": [
                "batch:SubmitJob",
                "batch:DescribeJobs",
                "batch:TerminateJob"
            ],
            "Resource": "*"
        }},
        {{
            "Effect": "Allow",
            "Action": [
                "events:PutTargets",
                "events:PutRule",
                "events:DescribeRule"
            ],
            "Resource": [
               "arn:aws:events:*:*:rule/StepFunctionsGetEventsForBatchJobsRule"
            ]
        }}
    ]
}}

        '''


        sns_policy = f'''{{
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
        '''

        policies_list = [lambda_policy, batch_policy, sns_policy]

        self.state_machine_role_arn = self.iam_helper.role_stitcher(self.state_machine_role_name, 'states',
                                                                    'Permissions for statemachine to run jobs',
                                                                    policies_list=policies_list)

    def create_state_machine(self):

        job_definition = f'''{{
  "Comment": "An example of the Amazon States Language for notification on an AWS Batch job completion",
  "StartAt": "Submit Batch Job",
  "TimeoutSeconds": 3600,
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

        '''

        #print(job_definition)


        while True:

            try:
                response = self.step_functions.create_state_machine(
                    name=self.state_machine_name,
                    definition=job_definition,
                    roleArn=self.state_machine_role_arn
                )

                # print(response)
                self.state_machine_arn = response['stateMachineArn']
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
            name=f'{self.state_machine_name}_execution_{int(time.time())}',
            input=f'{{"array_size": {array_size}}}'
        )

        logger.info(f"Starting state machine {self.state_machine_name}.")

    def clean(self):

        # Get our vpc:

        response = self.ec2.describe_vpcs(
            Filters=[
                {
                    'Name': 'tag:Name',
                    'Values': [
                        self.vpc_name,
                    ]
                },
            ]
        )

        self.vpc_id = response['Vpcs'][0]['VpcId']

        """
        Responsible for cleaning artifacts for EMR.
        """
        logger.info("Cleaning up EMR.")

        try:
            self.emr.terminate_job_flows(
                JobFlowIds=[
                    self.emr_cluster_name
                ]
            )
            logger.info(f"EMR cluster {self.emr_cluster_name} deleted.")

        except Exception as e:
            if 'ResourceNotFoundException' in str(e):
                logger.info(f"EMR cluster {self.emr_cluster_name} already MIA - skipping...")

        self.iam_helper.remove_role_from_instance_profile(self.emr_instance_profile_name)
        self.iam_helper.delete_instance_profile(self.emr_instance_profile_name)
        self.iam_helper.delete_role(self.emr_job_flow_role_name)
        self.iam_helper.delete_role(self.emr_service_role_name)


        logger.info(
            f"EMR clean complete.  Results bucket and data {self.s3_bucket} have not been deleted.")

        logger.info(f'Deleting Security group {self.emr_cluster_security_group_name}.')

        default_sg_response = self.ec2.describe_security_groups(
            Filters=[
                {
                    'Name': 'group-name',
                    'Values': [
                        'default',
                    ]
                },
            ]
        )
        logger.info("Removing egress from default security group.")
        for group in default_sg_response['SecurityGroups']:
            #print(group)
            #print(self.vpc_id)
            if group['VpcId'] == self.vpc_id:
                default_group_id = group['GroupId']
                dsg = self.ec2r.SecurityGroup(default_group_id)
                #print(dsg)
                #print(len(dsg.ip_permissions_egress))
                if len(dsg.ip_permissions_egress):
                    response =  dsg.revoke_egress(IpPermissions=dsg.ip_permissions_egress)


        sg_response = self.ec2.describe_security_groups(
            Filters=[
                {
                    'Name': 'group-name',
                    'Values': [
                        self.emr_cluster_security_group_name,
                    ]
                },
            ]
        )


        try:

            group_id = sg_response['SecurityGroups'][0]['GroupId']

            sg = self.ec2r.SecurityGroup(group_id)
            if len(sg.ip_permissions):
                sg.revoke_ingress(IpPermissions=sg.ip_permissions)

            while True:
                try:
                    self.ec2.delete_security_group(
                        GroupId=group_id
                    )
                    break
                except:

                    logger.info("Waiting for security group ingress rules to be removed ...")
                    time.sleep(5)

            logger.info(f"Deleted security group {self.emr_cluster_security_group_name}.")
        except Exception as e:
            if 'does not exist' in str(e) or 'list index out of range' in str(e):
                logger.info(f'Security group {self.emr_cluster_security_group_name} does not exist - skipping...')
            else:
                raise


        try:
            self.aws_lambda.delete_function(
                FunctionName=self.lambda_emr_job_step_function_name
            )
        except Exception as e:
            if 'Function not found' in str(e):
                logger.info(f"Function {self.lambda_emr_job_step_function_name} not found, skipping...")
            else:
                raise

        try:
            self.s3.delete_object(Bucket=self.s3_bucket, Key=self.s3_lambda_code_emr_cluster_key)
            logger.info(
                f"S3 object {self.s3_lambda_code_emr_cluster_key} for bucket {self.s3_bucket} deleted."  # noqa E501
            )
        except Exception as e:
            if 'NoSuchBucket' in str(e):
                logger.info(
                    f"S3 object {self.s3_lambda_code_emr_cluster_key} for bucket {self.s3_bucket} missing - not deleted."  # noqa E501
                )
            else:
                raise

        self.iam_helper.delete_role(self.lambda_emr_job_step_execution_role)




        state_machines = self.step_functions.list_state_machines()

        for sm in state_machines['stateMachines']:
            if sm['name'] == self.state_machine_name:
                self.state_machine_arn = sm['stateMachineArn']
                self.step_functions.delete_state_machine(
                    stateMachineArn=self.state_machine_arn
                )
                logger.info(f"Deleted state machine {self.state_machine_name}.")
                break

        self.iam_helper.delete_role(self.state_machine_role_name)

        try:

            self.batch.update_job_queue(
                jobQueue=self.batch_job_queue_name,
                state='DISABLED'
            )

            while True:
                try:
                    response = self.batch.delete_job_queue(
                        jobQueue=self.batch_job_queue_name
                    )
                    logger.info(f"Job queue {self.batch_job_queue_name} deleted.")
                    break
                except Exception as e:
                    if 'Cannot delete, resource is being modified' in str(e):
                        logger.info("Job queue being modified - sleeping until ready...")
                        time.sleep(5)
                    else:
                        raise
        except Exception as e:
            if 'does not exist' in str(e):
                logger.info(f"Job queue {self.batch_job_queue_name} missing, skipping...")

        # Delete compute enviornment

        try:

            self.batch.update_compute_environment(
                computeEnvironment=self.batch_compute_environment_name,
                state='DISABLED'
            )

            while True:
                try:
                    response = self.batch.delete_compute_environment(
                        computeEnvironment=self.batch_compute_environment_name
                    )
                    logger.info(f"Compute environment {self.batch_compute_environment_name} deleted.")
                    break
                except Exception as e:
                    if 'Cannot delete, resource is being modified' in str(e) or 'found existing JobQueue' in str(e):
                        logger.info("Compute environment being modified - sleeping until ready...")
                        time.sleep(5)
                    else:
                        raise
        except Exception as e:
            if 'does not exist' in str(e):
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

        response = self.ec2.describe_vpcs(
            Filters=[
                {
                    'Name': 'tag:Name',
                    'Values': [
                        self.job_identifier,
                    ]
                },
            ],

        )

        for vpc in response['Vpcs']:
            this_vpc = vpc['VpcId']

            ng_response = self.ec2.describe_nat_gateways(
                Filters=[
                    {
                        'Name': 'vpc-id',
                        'Values': [
                            this_vpc
                        ]
                    }
                ]
            )

            for natgw in ng_response['NatGateways']:
                this_natgw = natgw['NatGatewayId']

                if natgw['State'] != 'deleted':
                    self.ec2.delete_nat_gateway(
                        NatGatewayId=this_natgw
                    )

            rtas_response = self.ec2.describe_route_tables(
                Filters=[
                    {
                        'Name': 'vpc-id',
                        'Values': [
                            this_vpc
                        ]
                    }
                ]

            )

            #print("ROUTE TABLES", rtas_response)

            for route_table in rtas_response['RouteTables']:
                route_table_id = route_table['RouteTableId']
                for association in route_table['Associations']:


                    if not association['Main']:
                        response = self.ec2.disassociate_route_table(
                            AssociationId=association['RouteTableAssociationId']
                        )
                        rt_counter = 10
                        while rt_counter:
                            try:
                                response = self.ec2.delete_route_table(
                                    RouteTableId=route_table_id
                                )
                                logger.info("Route table removed.")
                                break
                            except Exception as e:
                                rt_counter = rt_counter -1
                                if 'DependencyViolation' in str(e):
                                    #print(str(e))
                                    logger.info(
                                        "Waiting for association to be released before deleting route table.  Sleeping...")
                                    time.sleep(5)
                                else:
                                    raise


            igw_response = self.ec2.describe_internet_gateways(
                Filters=[
                    {
                        'Name': 'tag:Name',
                        'Values': [
                            self.job_identifier
                        ]
                    }
                ]
            )

            for internet_gateway in igw_response['InternetGateways']:
                for attachment in internet_gateway['Attachments']:
                    if attachment['VpcId'] == this_vpc:
                        while True:
                            try:
                                try:
                                    self.ec2.detach_internet_gateway(
                                        InternetGatewayId=internet_gateway['InternetGatewayId'],
                                        VpcId=attachment['VpcId']
                                    )
                                except Exception as e:
                                    logger.info(f"Error on Internet Gateway disassociation - ignoring... {str(e)}")

                                self.ec2.delete_internet_gateway(
                                    InternetGatewayId=internet_gateway['InternetGatewayId']
                                )
                                logger.info("Internet Gateway deleted.")
                                break

                            except Exception as e:
                                if 'DependencyViolation' in str(e):
                                    logger.info(
                                        "Waiting for IPs to be released before deleting Internet Gateway.  Sleeping...")
                                    time.sleep(5)
                                else:
                                    raise

            subn_response = self.ec2.describe_subnets(
                Filters=[
                    {
                        'Name': 'vpc-id',
                        'Values': [
                            this_vpc
                        ]
                    }
                ]
            )

            for subnet in subn_response['Subnets']:
                while True:
                    try:
                        self.ec2.delete_subnet(
                            SubnetId=subnet['SubnetId']
                        )
                        break
                    except Exception as e:
                        if 'DependencyViolation' in str(e):
                            logger.info('Subnet cannot be deleted as dependencies are still being deleted. Sleeping...')
                            time.sleep(10)
                        else:
                            raise

            while True:
                try:

                    self.ec2.delete_vpc(
                        VpcId=this_vpc
                    )

                    break

                except Exception:
                    raise

        # Find the Elastic IP from the NAT
        response = self.ec2.describe_addresses(
            Filters=[
                {
                    'Name': 'tag:Name',
                    'Values': [
                        self.job_identifier,
                    ]
                },
            ],

        )
        for address in response['Addresses']:
            this_address = address['AllocationId']

            response = self.ec2.release_address(
                AllocationId=this_address
            )


    def create_emr_security_groups(self):

        try:

            response = self.ec2.create_security_group(
                Description='EMR Job Flow Security Group (full cluster access)',
                GroupName=self.emr_cluster_security_group_name,
                VpcId=self.vpc_id
            )
            #print(response)
            self.emr_cluster_security_group_id = response['GroupId']

        except Exception as e:
            if 'already exists for VPC' in str(e):
                logger.info("Security group for EMR already exists, skipping ...")
                response = self.ec2.describe_security_groups(
                    Filters=[
                        {
                            'Name': 'group-name',
                            'Values': [
                                self.emr_cluster_security_group_name,
                            ]
                        },
                    ]
                )

                self.emr_cluster_security_group_id = response['SecurityGroups'][0]['GroupId']
            else:
                raise

        try:
            response = self.ec2.authorize_security_group_ingress(
                GroupId=self.emr_cluster_security_group_id,
                IpPermissions= [dict(IpProtocol='-1',
                                UserIdGroupPairs=[dict(GroupId=self.emr_cluster_security_group_id, UserId = self.account)])],
            )
        except Exception as e:
            if 'already exists' in str(e):
                logger.info("Security group egress rule for EMR already exists, skipping ...")
            else:
                raise

    def create_emr_iam_roles(self):

        self.emr_service_role_arn = self.iam_helper.role_stitcher(
            self.emr_service_role_name,
            "elasticmapreduce",
            f"EMR Service Role {self.job_identifier}",
            managed_policie_arns=['arn:aws:iam::aws:policy/service-role/AmazonElasticMapReduceRole']
        )

        emr_policy = '''{
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
                "glue:CreateCrawler"
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
}'''

        self.emr_job_flow_role_arn = self.iam_helper.role_stitcher(
            self.emr_job_flow_role_name,
            "ec2",
            f"EMR Job Flow Role {self.job_identifier}",
            managed_policie_arns=['arn:aws:iam::aws:policy/service-role/AmazonElasticMapReduceforEC2Role'],
            policies_list=[emr_policy]
        )

        try:
            response = self.iam.create_instance_profile(
                InstanceProfileName=self.emr_instance_profile_name
            )

            self.emr_instance_profile_arn = response['InstanceProfile']['Arn']

            logger.info("EMR Instance Profile created")

            response = self.iam.add_role_to_instance_profile(
                InstanceProfileName=self.emr_instance_profile_name,
                RoleName=self.emr_job_flow_role_name
            )

        except Exception as e:
            if 'EntityAlreadyExists' in str(e):
                logger.info('EMR Instance Profile not created - already exists')
                response = self.iam.get_instance_profile(
                    InstanceProfileName=self.emr_instance_profile_name
                )
                self.emr_instance_profile_arn = response['InstanceProfile']['Arn']

    def upload_assets(self):

        upload_path = os.path.dirname(os.path.abspath(__file__))

        bsb_post_script = f'''
from dask_yarn import YarnCluster
from dask.distributed import Client

cluster = YarnCluster(
    deploy_mode='local',
    worker_vcores=2,
    worker_memory='{self.emr_worker_memory} GB',
    n_workers={self.emr_total_workers}
)

client = Client(cluster)
from buildstockbatch.postprocessing import combine_results, create_athena_tables
results_s3_loc = 's3://{self.s3_bucket}/{self.s3_bucket_prefix}/results/'

from fs import open_fs
import pandas as pd
import os
from fs.copy import copy_file

s3fs = open_fs(results_s3_loc)

s3fs.getinfo(full_path).is_file

with s3fs.open(full_path, 'rb') as f:
    df = pd.read_parquet(f, engine='pyarrow')

combine_results(results_s3_loc)

conf = dict(
    region_name='us-west-2',
    athena=dict(database_name='{self.job_identifier}',
                glue_service_role='service-role/AWSGlueServiceRole-default',
                max_crawling_time=600)
    )

create_athena_tables(conf, None, '{self.s3_bucket}', '{self.s3_bucket_prefix}/results/')

'''

        bsb_post_bash = f'''#!/bin/bash

aws s3 cp s3://{self.s3_bucket}/{self.s3_bucket_prefix}/emr/bsb_post.py bsb_post.py
/home/hadoop/miniconda/bin/python bsb_post.py

        '''

        self.write_file(f'{upload_path}/s3_assets/bsb_post.py', bsb_post_script)
        self.write_file(f'{upload_path}/s3_assets/bsb_post.sh', bsb_post_bash)
        logger.info('Uploading EMR support assets...')
        self.upload_s3_file(f'{upload_path}/s3_assets/bsb_post.py', self.s3_bucket, f'{self.s3_bucket_prefix}/{self.s3_emr_folder_name}/bsb_post.py')
        self.upload_s3_file(f'{upload_path}/s3_assets/bsb_post.sh', self.s3_bucket, f'{self.s3_bucket_prefix}/{self.s3_emr_folder_name}/bsb_post.sh')
        self.upload_s3_file(f'{upload_path}/s3_assets/bootstrap-dask-custom', self.s3_bucket, f'{self.s3_bucket_prefix}/{self.s3_emr_folder_name}/bootstrap-dask-custom')
        logger.info('EMR support assets uploaded.')

    def create_emr_cluster_function(self):
        script_name = f"s3://{self.s3_bucket}/{self.s3_bucket_prefix}/{self.s3_emr_folder_name}/bsb_post.sh"
        bootstrap_action = f's3://{self.s3_bucket}/{self.s3_bucket_prefix}/{self.s3_emr_folder_name}/bootstrap-dask-custom'

        function_script = f''' 

import json
import boto3
from pprint import pprint

def lambda_handler(event, context):

    # some prep work needed for this - check your security groups - there may default groups if any EMR cluster
    # was launched from the console - also prepare a bucket for logs

    # https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/emr.html

    session = boto3.Session(region_name='{self.region}')

    client = session.client("emr")

    response = client.run_job_flow(
        Name='{self.emr_cluster_name}',
        LogUri='{self.emr_log_uri}',

        ReleaseLabel='emr-5.23.0',
        Instances={{
            'MasterInstanceType': '{self.emr_master_instance_type}',
            'SlaveInstanceType': '{self.emr_slave_instance_type}',
            'InstanceCount': {self.emr_cluster_instance_count},
            'Ec2SubnetId': '{self.priv_vpc_subnet_id_1}',
            'KeepJobFlowAliveWhenNoSteps': True,
            'EmrManagedMasterSecurityGroup': '{self.emr_cluster_security_group_id}',
            'EmrManagedSlaveSecurityGroup': '{self.emr_cluster_security_group_id}',
            'ServiceAccessSecurityGroup': '{self.batch_security_group}'
        }},

        Applications=[
            {{
                'Name': 'Hadoop'
            }},

        ],

        BootstrapActions=[
            {{
                'Name': 'launchFromS3',
                'ScriptBootstrapAction': {{
                    'Path': '{bootstrap_action}',
                }}
            }},
        ],

        Steps=[
            {{
                'Name': 'Dask',
                'ActionOnFailure': 'TERMINATE_CLUSTER',

                'HadoopJarStep': {{
                    'Jar': 's3://us-east-1.elasticmapreduce/libs/script-runner/script-runner.jar',
                    'Args': ['{script_name}']
                }}
            }},
        ],

        VisibleToAllUsers=True,
        JobFlowRole='{self.emr_instance_profile_name}',
        ServiceRole='{self.emr_service_role_name}',
        Tags=[
            {{
                'Key': 'org',
                'Value': 'ops'
            }},
        ],
        AutoScalingRole='EMR_AutoScaling_DefaultRole',
        ScaleDownBehavior='TERMINATE_AT_TASK_COMPLETION',
        EbsRootVolumeSize=100


    )
    pprint(response)

'''

        self.zip_and_s3_load(function_script,
                             'emr_function.py',
                             'emr_function.py.zip',
                             self.s3_bucket,
                             f'{self.s3_lambda_code_emr_cluster_key}')

        while True:
            try:
                #print(self.lambda_emr_job_step_function_name)
                #print('lambda role', self.lambda_emr_job_step_execution_role_arn)

                self.aws_lambda.create_function(
                    FunctionName=self.lambda_emr_job_step_function_name,
                    Runtime='python3.7',
                    Role=self.lambda_emr_job_step_execution_role_arn,
                    Handler='emr_function.lambda_handler',
                    Code={
                        'S3Bucket': self.s3_bucket,
                        'S3Key': self.s3_lambda_code_emr_cluster_key
                    },
                    Description=f'Lambda for emr cluster execution on job {self.job_identifier}',
                    Timeout=900,
                    MemorySize=128,
                    Publish=True,
                    Tags={
                        'job': self.job_identifier
                    }
                )

                logger.info(f"Lambda function {self.lambda_emr_job_step_function_name} created.")
                break

            except Exception as e:
                if 'role defined for the function cannot be assumed' in str(e):
                    logger.info(
                        f"Lambda role not registered for {self.lambda_emr_job_step_function_name} - sleeping ...")
                    time.sleep(5)
                elif 'Function already exist' in str(e):
                    logger.info(f'Lambda function {self.lambda_emr_job_step_function_name} exists, skipping...')
                    break
                else:
                    raise


class AwsSNS(AwsJobBase):

    def __init__(self, job_name, aws_config, boto3_session):
        super().__init__(job_name, aws_config, boto3_session)
        self.sns = self.session.client("sns")
        self.sns_state_machine_topic_arn = None

    def create_topic(self):
        response = self.sns.create_topic(
            Name=self.sns_state_machine_topic
        )

        logger.info(f"Simple notifications topic {self.sns_state_machine_topic} created.")

        self.sns_state_machine_topic_arn = response['TopicArn']

    def subscribe_to_topic(self):
        self.sns.subscribe(
            TopicArn=self.sns_state_machine_topic_arn,
            Protocol='email',
            Endpoint=self.operator_email
        )

        logger.info(
            f"Operator {self.operator_email} subscribed to topic - please confirm via email to recieve state machine progress messages."  # noqa 501
        )

    def clean(self):
        self.sns.delete_topic(
            TopicArn=f"arn:aws:sns:{self.region}:{self.account}:{self.sns_state_machine_topic}"
        )

        logger.info(f"Simple notifications topic {self.sns_state_machine_topic} deleted.")


class AwsBatch(DockerBatchBase):

    def __init__(self, project_filename):
        super().__init__(project_filename)

        self.job_identifier = re.sub('[^0-9a-zA-Z]+', '_', self.cfg['aws']['job_identifier'])[:10]

        self.project_filename = project_filename
        self.region = self.cfg['aws']['region']
        self.ecr = boto3.client('ecr', region_name=self.region)
        self.s3 = boto3.client('s3', region_name=self.region)
        self.s3_bucket = self.cfg['aws']['s3']['bucket']
        self.s3_bucket_prefix = self.cfg['aws']['s3']['prefix']
        self.batch_env_use_spot = self.cfg['aws']['use_spot']
        self.batch_array_size = self.cfg['aws']['batch_array_size']
        self.boto3_session = boto3.Session(region_name=self.region)

    @classmethod
    def docker_image(cls):
        return 'nrel/buildstockbatch'

    @property
    def weather_dir(self):
        return self._weather_dir

    @property
    def container_repo(self):
        repo_name = self.docker_image()
        repos = self.ecr.describe_repositories()
        repo = None
        for repo in repos['repositories']:
            if repo['repositoryName'] == repo_name:
                break
        if repo is None:
            resp = self.ecr.create_repository(repositoryName=repo_name)
            repo = resp['repository']
        return repo

    def push_image(self):
        """
        Push the locally built docker image to the AWS docker repo
        """
        auth_token = self.ecr.get_authorization_token()
        dkr_user, dkr_pass = base64.b64decode(auth_token['authorizationData'][0]['authorizationToken']). \
            decode('ascii').split(':')
        repo_url = self.container_repo['repositoryUri']
        registry_url = 'https://' + repo_url.split('/')[0]
        resp = self.docker_client.login(
            username=dkr_user,
            password=dkr_pass,
            registry=registry_url
        )
        logger.debug(resp)
        image = self.docker_client.images.get(self.docker_image())
        image.tag(repo_url)
        last_status = None
        for x in self.docker_client.images.push(repo_url, stream=True):
            try:
                y = json.loads(x)
            except json.JSONDecodeError:
                continue
            else:
                if y.get('status') is not None and y.get('status') != last_status:
                    logger.debug(y['status'])
                    last_status = y['status']

    def clean(self):
        """
        Clean up the AWS environment
        :return:
        """
        logger.info("Beginning cleanup of AWS resources...")

        batch_env = AwsBatchEnv(self.job_identifier, self.cfg['aws'], self.boto3_session)
        batch_env.clean()

        sns_env = AwsSNS(self.job_identifier, self.cfg['aws'], self.boto3_session)
        sns_env.clean()

        dynamo_env = AwsDynamo(self.job_identifier, self.cfg['aws'], self.boto3_session)
        dynamo_env.clean()

    def run_batch(self):
        """
        Run a batch of simulations using AWS Batch

        This will
            - perform the sampling
            - package and upload the assets, including weather
            - kick off a batch simulation on AWS
        """

        # Generate buildstock.csv
        if 'downselect' in self.cfg:
            buildstock_csv_filename = self.downselect()
        else:
            buildstock_csv_filename = self.run_sampling()

        # Compress and upload assets to S3
        with tempfile.TemporaryDirectory() as tmpdir, tempfile.TemporaryDirectory() as tmp_weather_dir:
            self._weather_dir = tmp_weather_dir
            self._get_weather_files()
            tmppath = pathlib.Path(tmpdir)
            logger.debug('Creating assets tarfile')
            with tarfile.open(tmppath / 'assets.tar.gz', 'x:gz') as tar_f:
                project_path = pathlib.Path(self.project_dir)
                buildstock_path = pathlib.Path(self.buildstock_dir)
                tar_f.add(buildstock_path / 'measures', 'measures')
                tar_f.add(buildstock_path / 'resources', 'lib/resources')
                tar_f.add(project_path / 'housing_characteristics', 'lib/housing_characteristics')
                tar_f.add(project_path / 'seeds', 'seeds')
                tar_f.add(project_path / 'weather', 'weather')
            logger.debug('Compressing weather files')
            weather_path = tmppath / 'weather'
            os.makedirs(weather_path)
            Parallel(n_jobs=-1, verbose=9)(
                delayed(compress_file)(
                    pathlib.Path(self.weather_dir) / epw_filename,
                    str(weather_path / epw_filename) + '.gz'
                )
                for epw_filename
                in filter(lambda x: x.endswith('.epw'), os.listdir(self.weather_dir))
            )
            logger.debug('Writing project configuration for upload')
            with open(tmppath / 'config.json', 'wt', encoding='utf-8') as f:
                json.dump(self.cfg, f)

            # Collect simulations to queue
            df = pd.read_csv(buildstock_csv_filename, index_col=0)
            building_ids = df.index.tolist()
            n_datapoints = len(building_ids)
            n_sims = n_datapoints * (len(self.cfg.get('upgrades', [])) + 1)
            logger.debug('Total number of simulations = {}'.format(n_sims))

            # This is the maximum number of jobs that can be in an array
            if self.batch_array_size <= 10000:
                max_array_size = self.batch_array_size
            else:
                max_array_size = 10000
            n_sims_per_job = math.ceil(n_sims / max_array_size)
            n_sims_per_job = max(n_sims_per_job, 2)
            logger.debug('Number of simulations per array job = {}'.format(n_sims_per_job))

            baseline_sims = zip(building_ids, itertools.repeat(None))
            upgrade_sims = itertools.product(building_ids, range(len(self.cfg.get('upgrades', []))))
            all_sims = list(itertools.chain(baseline_sims, upgrade_sims))
            random.shuffle(all_sims)
            all_sims_iter = iter(all_sims)

            os.makedirs(tmppath / 'jobs')

            for i in itertools.count(0):
                batch = list(itertools.islice(all_sims_iter, n_sims_per_job))
                if not batch:
                    break
                logger.info('Queueing job {} ({} simulations)'.format(i, len(batch)))
                job_json_filename = tmppath / 'jobs' / 'job{:05d}.json'.format(i)
                with open(job_json_filename, 'w') as f:
                    json.dump({
                        'job_num': i,
                        'batch': batch,
                    }, f, indent=4)
            array_size = i
            logger.debug('Array size = {}'.format(array_size))

            # Compress job jsons
            jobs_dir = tmppath / 'jobs'
            logger.debug('Compressing job jsons using gz')
            tick = time.time()
            with tarfile.open(tmppath / 'jobs.tar.gz', 'w:gz') as tf:
                tf.add(jobs_dir, arcname='jobs')
            tick = time.time() - tick
            logger.debug('Done compressing job jsons using gz {:.1f} seconds'.format(tick))
            shutil.rmtree(jobs_dir)

            os.makedirs(tmppath / 'results' / 'simulation_output')

            logger.debug('Uploading files to S3')
            copy_fs(
                str(tmppath),
                f"s3://{self.cfg['aws']['s3']['bucket']}/{self.cfg['aws']['s3']['prefix']}",
                workers=cpu_count(),
                on_copy=lambda src_fs, src_path, dst_fs, dst_path: logger.debug(f'Uploaded {dst_path}')
            )

        # Define the batch environment
        batch_env = AwsBatchEnv(self.job_identifier, self.cfg['aws'], self.boto3_session)
        logger.info(
            "Launching Batch environment - (resource configs will not be updated on subsequent executions, but new job revisions will be created):"  # noqa 501
        )
        logger.debug(str(batch_env))
        batch_env.create_batch_service_roles()
        batch_env.create_vpc()
        batch_env.create_compute_environment()
        batch_env.create_job_queue()

        # Pass through config for the Docker containers
        env_vars = dict(S3_BUCKET=self.s3_bucket, S3_PREFIX=self.s3_bucket_prefix, JOB_NAME=self.job_identifier,
                        REGION=self.region)

        image_url = '{}:latest'.format(
            self.container_repo['repositoryUri']
        )

        batch_env.create_job_definition(
            image_url,
            command=['python3', '/buildstock-batch/buildstockbatch/aws.py'],
            vcpus=1,
            memory=1024,
            env_vars=env_vars
        )

        # SNS Topic
        sns_env = AwsSNS(self.job_identifier, self.cfg['aws'], self.boto3_session)
        sns_env.create_topic()
        sns_env.subscribe_to_topic()

        # Dynamo DB
        dynamo_env = AwsDynamo(self.job_identifier, self.cfg['aws'], self.boto3_session)
        dynamo_env.create_summary_table()
        dynamo_env.add_dynamo_task_permissions()

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

        batch_env.start_state_machine_execution(array_size)

        # old method to submit the job
        # batch_env.submit_job(array_size)

    @classmethod
    def create_dynamo_field(cls, key, value):

        if value == '':
            value = ' '

        if isinstance(value, bool):
            this_type = 'BOOL'
        elif isinstance(value, (int, float)):
            this_type = 'N'
        else:
            this_type = "S"

        # this is an override fix for our keys - not yet sure what changed
        if key == 'building_id' or key == 'upgrade_idx':
            this_type = "S"

        if this_type == 'BOOL':
            this_item = {key: {this_type: value}}
        else:
            this_item = {key: {this_type: str(value)}}

        return this_item

    @classmethod
    def run_job(cls, job_id, bucket, prefix, job_name, region):
        """
        Run a few simulations inside a container.

        This method is called from inside docker container in AWS. It will
        go get the necessary files from S3, run the simulation, and post the
        results back to S3.
        """

        logger.debug(f"region: {region}")
        s3 = boto3.client('s3')
        # firehose = boto3.client('firehose', region_name=region)
        dynamo = boto3.client('dynamodb', region_name=region)
        sim_dir = pathlib.Path('/var/simdata/openstudio')

        # firehose_name = f"{job_name.replace(' ', '_').replace('_yml','')}_firehose"

        logger.debug('Downloading assets')
        assets_file_path = sim_dir.parent / 'assets.tar.gz'
        s3.download_file(bucket, '{}/assets.tar.gz'.format(prefix), str(assets_file_path))
        with tarfile.open(assets_file_path, 'r') as tar_f:
            tar_f.extractall(sim_dir)
        os.remove(assets_file_path)
        asset_dirs = os.listdir(sim_dir)

        logger.debug('Reading config')
        with io.BytesIO() as f:
            s3.download_fileobj(bucket, '{}/config.json'.format(prefix), f)
            cfg = json.loads(f.getvalue(), encoding='utf-8')

        logger.debug('Getting job information')
        jobs_file_path = sim_dir.parent / 'jobs.tar.gz'
        s3.download_file(bucket, '{}/jobs.tar.gz'.format(prefix), str(jobs_file_path))
        with tarfile.open(jobs_file_path, 'r') as tar_f:
            jobs_d = json.load(tar_f.extractfile('jobs/job{:05d}.json'.format(job_id)), encoding='utf-8')
        logger.debug('Number of simulations = {}'.format(len(jobs_d['batch'])))

        logger.debug('Getting weather files')
        df = pd.read_csv(str(sim_dir / 'lib' / 'housing_characteristics' / 'buildstock.csv'), index_col=0)
        epws_to_download = df.loc[[x[0] for x in jobs_d['batch']], 'Location Weather Filename'].unique().tolist()
        for epw_filename in epws_to_download:
            with io.BytesIO() as f_gz:
                logger.debug('Downloading {}.gz'.format(epw_filename))
                s3.download_fileobj(bucket, '{}/weather/{}.gz'.format(prefix, epw_filename), f_gz)
                with open(sim_dir / 'weather' / epw_filename, 'wb') as f_out:
                    logger.debug('Extracting {}'.format(epw_filename))
                    f_out.write(gzip.decompress(f_gz.getvalue()))

        for building_id, upgrade_idx in jobs_d['batch']:
            sim_id = 'bldg{:07d}up{:02d}'.format(building_id, 0 if upgrade_idx is None else upgrade_idx + 1)
            osw = cls.create_osw(cfg, sim_id, building_id, upgrade_idx)
            with open(os.path.join(sim_dir, 'in.osw'), 'w') as f:
                json.dump(osw, f, indent=4)
            with open(sim_dir / 'os_stdout.log', 'w') as f_out:
                try:
                    logger.debug('Running {}'.format(sim_id))
                    subprocess.run(
                        ['openstudio', 'run', '-w', 'in.osw'],
                        check=True,
                        stdout=f_out,
                        stderr=subprocess.STDOUT,
                        cwd=str(sim_dir)
                    )
                except subprocess.CalledProcessError:
                    pass

            cls.cleanup_sim_dir(sim_dir)

            logger.debug('Uploading simulation outputs for building_id = {}, upgrade_id = {}'.format(
                building_id, upgrade_idx
            ))
            walker = Walker(exclude_dirs=asset_dirs)
            s3fs = open_fs(f"s3://{bucket}/{prefix}")
            upload_fs = s3fs.makedirs(
                f"results/simulation_output/up{0 if upgrade_idx is None else upgrade_idx + 1:02d}/bldg{building_id:07d}"
            )
            copy_fs(
                str(sim_dir),
                upload_fs,
                walker=walker,
                on_copy=lambda src_fs, src_path, dst_fs, dst_path: logger.debug('Upload {dst_path}')
            )

            logger.debug('Writing output data to Firehose')
            datapoint_out_filepath = sim_dir / 'run' / 'data_point_out.json'
            out_osw_filepath = sim_dir / 'out.osw'
            if os.path.isfile(out_osw_filepath):
                out_osw = read_out_osw(str(sim_dir), str(out_osw_filepath.relative_to(sim_dir)))
                dp_out = flatten_datapoint_json(
                    read_data_point_out_json(str(sim_dir), str(datapoint_out_filepath.relative_to(sim_dir)))
                )
                if dp_out is None:
                    dp_out = {}
                dp_out.update(out_osw)
                dp_out['_id'] = sim_id
                for key in dp_out.keys():
                    dp_out[to_camelcase(key)] = dp_out.pop(key)
                '''
                try:
                    response = firehose.put_record(
                        DeliveryStreamName=firehose_name,
                        Record={
                            'Data': json.dumps(dp_out) + '\n'
                        }
                    )
                    logger.info(response)

                except Exception as e:
                    logger.error(str(e))
                '''

                item_def = {}

                item_def.update(cls.create_dynamo_field('building_id', str(building_id)))
                item_def.update(cls.create_dynamo_field('upgrade_idx', str(upgrade_idx)))

                for key, value in dp_out.items():
                    item_def.update(cls.create_dynamo_field(key, value))

                dynamo.put_item(
                    TableName=f"{job_name}_summary_table",
                    Item=item_def
                )
                '''
                try:
                    response = dynamo.put_record(
                        DeliveryStreamName=firehose_name,
                        Record={
                            'Data': json.dumps(dp_out) + '\n'
                        }
                    )
                    logger.info(response)

                except Exception as e:
                    logger.error(str(e))
                '''

            logger.debug('Clearing out simulation directory')
            for item in set(os.listdir(sim_dir)).difference(asset_dirs):
                if os.path.isdir(item):
                    shutil.rmtree(item)
                elif os.path.isfile(item):
                    os.remove(item)


def main():
    logging.config.dictConfig({
        'version': 1,
        'disable_existing_loggers': True,
        'formatters': {
            'defaultfmt': {
                'format': '%(levelname)s:%(asctime)s:%(name)s:%(message)s',
                'datefmt': '%Y-%m-%d %H:%M:%S'
            }
        },
        'handlers': {
            'console': {
                'class': 'logging.StreamHandler',
                'formatter': 'defaultfmt',
                'level': 'DEBUG',
                'stream': 'ext://sys.stdout',
            }
        },
        'loggers': {
            '__main__': {
                'level': 'DEBUG',
                'propagate': True,
                'handlers': ['console']
            },
            'buildstockbatch': {
                'level': 'DEBUG',
                'propagate': True,
                'handlers': ['console']
            }
        },
    })
    print(AwsBatch.LOGO)
    if 'AWS_BATCH_JOB_ARRAY_INDEX' in os.environ:
        job_id = int(os.environ['AWS_BATCH_JOB_ARRAY_INDEX'])
        s3_bucket = os.environ['S3_BUCKET']
        s3_prefix = os.environ['S3_PREFIX']
        job_name = os.environ['JOB_NAME']
        region = os.environ['REGION']
        AwsBatch.run_job(job_id, s3_bucket, s3_prefix, job_name, region)
    else:
        parser = argparse.ArgumentParser()
        parser.add_argument('project_filename')
        parser.add_argument('-c', '--clean', action='store_true')
        args = parser.parse_args()
        batch = AwsBatch(args.project_filename)
        if args.clean:
            batch.clean()
        else:
            batch.push_image()
            batch.run_batch()


if __name__ == '__main__':
    main()
