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
import gzip
import io
import itertools
from joblib import Parallel, delayed
import json
import logging
import math
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
import zipfile
import io
import stat

from buildstockbatch.localdocker import DockerBatchBase
from buildstockbatch.base import (
    read_data_point_out_json,
    to_camelcase,
    flatten_datapoint_json,
    read_out_osw
)

from buildstockbatch.awsbase import (
    AwsJobBase
)

logger = logging.getLogger(__name__)


def upload_file_to_s3(*args, **kwargs):
    s3 = boto3.client('s3')
    s3.upload_file(*args, **kwargs)


def upload_directory_to_s3(local_directory, bucket, prefix):
    local_dir_abs = pathlib.Path(local_directory).absolute()

    def filename_generator():
        for dirpath, dirnames, filenames in os.walk(local_dir_abs):
            for filename in filenames:
                if filename.startswith('.'):
                    continue
                local_filepath = pathlib.Path(dirpath, filename)
                s3_key = pathlib.PurePosixPath(
                    prefix,
                    local_filepath.relative_to(local_dir_abs)
                )
                yield local_filepath, s3_key

    logger.debug('Uploading {} => {}/{}'.format(local_dir_abs, bucket, prefix))

    Parallel(n_jobs=-1, verbose=9)(
        delayed(upload_file_to_s3)(str(local_file), bucket, s3_key.as_posix())
        for local_file, s3_key
        in filename_generator()
    )


def compress_file(in_filename, out_filename):
    with gzip.open(str(out_filename), 'wb') as f_out:
        with open(str(in_filename), 'rb') as f_in:
            shutil.copyfileobj(f_in, f_out)


class AwsFirehose(AwsJobBase):

    logger.propagate = False

    def __init__(self, job_name, s3_bucket, s3_bucket_prefix, region):

        """
        Initializes the Firehose configuration.
        :param job_name:  Name of the job being run
        :param s3_bucket: Bucket to land results into
        :param s3_bucket_prefix: Prefix to land results into

        """
        super().__init__(job_name, s3_bucket, s3_bucket_prefix, region)

        self.firehose = self.session.client('firehose')
        #self.iam = self.session.client('iam')
        self.s3 = self.session.client('s3')
        self.s3_results_bucket = self.get_name('s3_results_bucket')
        self.s3_results_bucket_arn = self.get_name('s3_results_bucket_arn')
        self.s3_results_backup_bucket = self.get_name('s3_results_backup_bucket')
        self.s3_results_backup_bucket_arn = self.get_name('s3_results_backup_bucket_arn')
        self.firehose_role = self.get_name('firehose_role')
        self.firehose_name = self.get_name('firehose_name')
        self.firehose_role_policy_name = self.get_name('firehose_role_policy_name')
        self.firehost_task_policy_name = self.get_name('firehost_task_policy_name')
        # Initialize with create_firehose_delivery_role
        self.firehose_role_arn = None
        self.firehose_arn = None

    def __repr__(self):

        return f"""
The following objects compose the environment to support the Firehose collection for {self.job_name}:
Job Definition Name: {self.job_identifier}
Firehose: {self.firehose_name}
s3 Results Bucket: {self.s3_results_bucket}
s3 Backup Bucket: {self.s3_results_backup_bucket}
Firehose Role Name: {self.firehose_role}

"""

    def create_firehose_delivery_role(self):
        """
        Generate the firehose role with permissions to the endpoints - in this case cloudwatch and project s3 buckets.
        """

        delivery_role_policy = f'''{{
                        "Version": "2012-10-17",
                        "Statement": [
                            {{
                                "Sid": "S3AllowForFH",
                                "Effect": "Allow",
                                "Action": [
                                    "s3:AbortMultipartUpload",
                                    "s3:GetBucketLocation",
                                    "s3:GetObject",
                                    "s3:ListBucket",
                                    "s3:ListBucketMultipartUploads",
                                    "s3:PutObject"
                                ],
                                "Resource": [
                                    "{self.s3_results_bucket_arn}",
                                    "{self.s3_results_bucket_arn}/*",
                                    "{self.s3_results_backup_bucket_arn}",
                                    "{self.s3_results_backup_bucket_arn}/*"
                                ]
                            }},

                            {{
                                "Sid": "CWAllowForCW",
                                "Effect": "Allow",
                                "Action": [
                                    "logs:PutLogEvents"
                                ],
                                "Resource": [
                                    "arn:aws:logs:{self.region}:*:log-group:/aws/kinesisfirehose/{self.job_name}:*:*",
                                    "arn:aws:logs:{self.region}:*:log-group:/aws/kinesisfirehose/{self.job_name}:*:*"
                                ]
                            }}    
                        ]
                    }}'''

        self.firehose_role_arn = self.iam_helper.role_stitcher(self.firehose_role, 'firehose', f"Service role for Firehose support {self.job_identifier}", policies_list=[delivery_role_policy])

    def create_firehose_buckets(self):
        """
        Creates the output and backup buckets for the data.
        Failed processing stream events will land in the backup.
        """

        try:

            self.s3.create_bucket(
                Bucket=self.s3_results_bucket,
                CreateBucketConfiguration={
                    'LocationConstraint': self.session.region_name
                }
            )

        except Exception as e:

            if 'BucketAlreadyOwnedByYou' in str(e):
                logger.info(f'Bucket {self.s3_results_bucket}  not created - already exists')

            else:
                logger.error(str(e))

        try:

            self.s3.create_bucket(
                Bucket=self.s3_results_backup_bucket,
                CreateBucketConfiguration={
                    'LocationConstraint': self.session.region_name
                }
            )
        except Exception as e:

            if 'BucketAlreadyOwnedByYou' in str(e):
                logger.info(f'Bucket {self.s3_results_backup_bucket} not created - already exists')

            else:
                logger.error(str(e))

    def create_firehose(self):
        """
        Creates a simple firehose with S3 endpoints in AWS and waits for success.
        There appears to be a race condition with creation of the role, so firehose
        will re-try until created.
        """
        while 1 == 1:
            time.sleep(5)
            try:
                response = self.firehose.create_delivery_stream(
                    DeliveryStreamName=self.firehose_name,
                    DeliveryStreamType='DirectPut',
                    ExtendedS3DestinationConfiguration={
                        'RoleARN': self.firehose_role_arn,
                        'BucketARN': self.s3_results_bucket_arn,
                        'Prefix': self.s3_bucket_prefix + '/',
                        'BufferingHints': {
                            'SizeInMBs': 128,
                            'IntervalInSeconds': 900
                        },
                        'CompressionFormat': 'GZIP',
                        'CloudWatchLoggingOptions': {
                            'Enabled': True,
                            'LogGroupName': self.job_name,
                            'LogStreamName': self.s3_results_bucket
                        },

                        'S3BackupMode': 'Enabled',
                        'S3BackupConfiguration': {
                            'RoleARN': self.firehose_role_arn,
                            'BucketARN': self.s3_results_backup_bucket_arn,
                            'Prefix': self.s3_bucket_prefix,
                            'BufferingHints': {
                                'SizeInMBs': 128,
                                'IntervalInSeconds': 900
                            },
                            'CompressionFormat': 'GZIP',

                            'CloudWatchLoggingOptions': {
                                'Enabled': True,
                                'LogGroupName': self.job_name,
                                'LogStreamName': self.s3_results_backup_bucket
                            }
                        },
                    },

                    Tags=[
                        {
                            'Key': 'batch_job',
                            'Value': self.job_name
                        },
                    ]
                )

            except Exception as e:
                if 'ResourceInUseException' in str(e):
                    logger.info('Firehose stream operation in progress...')
                    break
                else:
                    logger.error("Problem creating stream - retrying after 5 seconds.  Error is:")
                    logger.error(str(e))
                    time.sleep(5)

        logger.info('Waiting for firehose delivery stream activation')

        while 1 == 1:
            time.sleep(5)
            try:
                # We need to give it a second to start

                cresponse = self.firehose.describe_delivery_stream(
                    DeliveryStreamName=self.firehose_name,
                    Limit=1
                )
            except Exception as e:
                if 'ResourceNotFoundException' in str(e):
                    logger.info(f"Firehose delivery stream {self.firehose_name} is not found.  Trying again...")
                    time.sleep(5)
                else:
                    logger.error(str(e))


            # If this fails there is an issue with creation
            if cresponse['DeliveryStreamDescription']['DeliveryStreamStatus'] == 'ACTIVE':
                self.firehose_arn = cresponse['DeliveryStreamDescription']['DeliveryStreamARN']
                logger.info(f"Firehose delivery stream {self.firehose_name} is active.")
                break

    def add_firehose_task_permissions(self, task_role):
        delivery_role_policy = f'''{{
            "Version": "2012-10-17",
            "Statement": [
                {{
                    "Sid": "TaskFH",
                    "Effect": "Allow",
                    "Action": [
                        "firehose:PutRecord"
                    ],
                    "Resource": [
                        "{self.firehose_arn}"
                    ]
                }}
            ]
        }}'''

        # TODO: probably adding duplicate policies - may need a manual check for existance
        response = self.iam.put_role_policy(
            RoleName=task_role,
            PolicyName=self.firehost_task_policy_name,
            PolicyDocument=delivery_role_policy
        )


    def put_record(self, data):
        """
        :param data: dictionary of data to record in the firehose
        """
        try:
            response = self.firehose.put_record(
                DeliveryStreamName=self.firehose_name,
                Record={
                    'Data': json.dumps(data)
                }
            )
        except Exception as e:
            logger.debug(str(e))


class AWSGlueTransform(AwsJobBase):

    def __init__(self, job_name, s3_bucket, s3_prefix, region):
        super().__init__(job_name, s3_bucket, s3_prefix, region)

        self.glue = self.session.client('glue')
        #self.s3_bucket = s3_bucket
        #self.s3_bucket prefix = s3_prefix
        self.md_crawler_role_name = self.get_name('glue_metadata_crawler_role_name')
        self.md_crawler_name = self.get_name('glue_metadata_crawler_name')
        self.glue_metadata_parquet_crawler_name = self.get_name('glue_metadata_parquet_crawler_name')
        self.glue_metadata_parqeut_results_s3_path = self.get_name('glue_metadata_parqeut_results_s3_path')
        self.database_name = self.get_name('glue_database_name')
        self.s3_results_bucket = self.get_name('s3_results_bucket')
        self.md_crawler_role_arn = None

    def __repr__(self):
        return f"""
The following objects compose the Glue to support the Batch run for {self.job_name}:
Crawler Role Name: {self.md_crawler_role_name}
S3 Bucket For Source Data and Glue Tables: {self.s3_results_bucket}
Source S3 Bucket Prefix: {self.s3_bucket_prefix}
    """

    def create_database(self):

        try:
            response = self.glue.create_database(
                DatabaseInput={
                    'Name': self.database_name,
                    'Description': f'Database created for job: {self.job_name}'
                }
            )
        except Exception as e:
            if 'AlreadyExistsException' in str(e):
                logger.info(f'Database {self.database_name} already exists, skipping...')
            else:
                logger.error(str(e))
                raise

    def create_roles(self):

        service_policy = f'''{{"Version": "2012-10-17",
                "Statement": [
                    {{
                        "Effect": "Allow",
                        "Action": [
                            "glue:*",
                            "s3:GetBucketLocation",
                            "s3:ListBucket",
                            "s3:ListAllMyBuckets",
                            "s3:GetBucketAcl",
                            "ec2:DescribeVpcEndpoints",
                            "ec2:DescribeRouteTables",
                            "ec2:CreateNetworkInterface",
                            "ec2:DeleteNetworkInterface",
                            "ec2:DescribeNetworkInterfaces",
                            "ec2:DescribeSecurityGroups",
                            "ec2:DescribeSubnets",
                            "ec2:DescribeVpcAttribute",
                            "iam:ListRolePolicies",
                            "iam:GetRole",
                            "iam:GetRolePolicy",
                            "cloudwatch:PutMetricData"
                        ],
                        "Resource": [
                            "*"
                        ]
                    }},
                    {{
                        "Effect": "Allow",
                        "Action": [
                            "s3:CreateBucket"
                        ],
                        "Resource": [
                            "arn:aws:s3:::aws-glue-*"
                        ]
                    }},
                    {{
                        "Effect": "Allow",
                        "Action": [
                            "s3:GetObject",
                            "s3:PutObject",
                            "s3:DeleteObject"
                        ],
                        "Resource": [
                            "arn:aws:s3:::aws-glue-*/*",
                            "arn:aws:s3:::*/*aws-glue-*/*"
                        ]
                    }},
                    {{
                        "Effect": "Allow",
                        "Action": [
                            "s3:GetObject"
                        ],
                        "Resource": [
                            "arn:aws:s3:::crawler-public*",
                            "arn:aws:s3:::aws-glue-*"
                        ]
                    }},
                    {{
                        "Effect": "Allow",
                        "Action": [
                            "logs:CreateLogGroup",
                            "logs:CreateLogStream",
                            "logs:PutLogEvents"
                        ],
                        "Resource": [
                            "arn:aws:logs:*:*:/aws-glue/*"
                        ]
                    }},
                    {{
                        "Effect": "Allow",
                        "Action": [
                            "ec2:CreateTags",
                            "ec2:DeleteTags"
                        ],
                        "Condition": {{
                            "ForAllValues:StringEquals": {{
                                "aws:TagKeys": [
                                    "aws-glue-service-resource"
                                ]
                            }}
                        }},
                        "Resource": [
                            "arn:aws:ec2:*:*:network-interface/*",
                            "arn:aws:ec2:*:*:security-group/*",
                            "arn:aws:ec2:*:*:instance/*"
                        ]
                    }}
                ]
            }}

            '''

        data_access_policy = f'''{{"Version": "2012-10-17",
                "Statement": [
                    {{
                        "Effect": "Allow",
                        "Action": [
                            "s3:GetBucketLocation",
                            "s3:ListBucket",
                            "s3:ListAllMyBuckets",
                            "s3:GetBucketAcl" 
                        ],
                        "Resource": [
                            "*"
                        ]
                    }},
                    {{
                        "Effect": "Allow",
                        "Action": [
                            "s3:Get*",
                            "s3:List*"
                        ],
                        "Resource": [
                            "arn:aws:s3:::{self.s3_results_bucket}/*",
                            "arn:aws:s3:::{self.s3_results_bucket}/*/*"
                        ]
                    }},
                    {{
                        "Effect": "Allow",
                        "Action": [
                            "s3:Get*",
                            "s3:List*",
                            "s3:Put*"
                        ],
                        "Resource": [
                            "arn:aws:s3:::{self.s3_results_bucket}/*",
                            "arn:aws:s3:::{self.s3_results_bucket}/*/*"
                        ]
                    }}

                ]

            }}
            '''

        policies = []
        policies.append(service_policy)
        policies.append(data_access_policy)

        self.crawler_role_arn = self.iam_helper.role_stitcher(
            self.md_crawler_role_name,
            'glue',
            f'IAM role for {self.md_crawler_role_name} and {self.glue_metadata_parquet_crawler_name}',
            policies_list=policies
        )

    def create_crawler(self):
        # Identified a race condition here - will add some sleep time to work around it.
        while True:
            try:
                response = self.glue.create_crawler(
                    Name=self.md_crawler_name,
                    Role=self.md_crawler_role_name,
                    DatabaseName=self.database_name,
                    Description=f"{self.job_name} crawler",
                    Targets={
                        'S3Targets': [
                            {
                                'Path': f's3://{self.s3_results_bucket}/{self.s3_bucket_prefix}/',
                            },
                        ],
                    },
                    TablePrefix=self.s3_bucket_prefix,
                    SchemaChangePolicy={
                        'UpdateBehavior': 'UPDATE_IN_DATABASE',
                        'DeleteBehavior': 'DELETE_FROM_DATABASE'
                    }
                )

                logger.info(f'Crawler {self.md_crawler_name} created')
                break

            except Exception as e:
                if "Service is unable to assume role" in str(e):
                    logger.info('Sleeping to allow role to register correctly before crawler creation...')
                    time.sleep(5)
                elif 'AlreadyExistsException' in str(e):
                    logger.info(f'Crawler {self.md_crawler_name} already exists, skipping...')
                    break
                else:
                    logger.error(str(e))
                    raise

    def create_parquet_crawler(self):
        # Identified a race condition here - will add some sleep time to work around it.
        while True:
            try:
                response = self.glue.create_crawler(
                    Name=self.md_crawler_name,
                    Role=self.md_crawler_role_name,
                    DatabaseName=self.database_name,
                    Description=f"{self.job_name} crawler",
                    Targets={
                        'S3Targets': [
                            {
                                'Path': f'{self.glue_metadata_parqeut_results_s3_path}',
                            },
                        ],
                    },
                    TablePrefix=self.s3_bucket_prefix,
                    SchemaChangePolicy={
                        'UpdateBehavior': 'UPDATE_IN_DATABASE',
                        'DeleteBehavior': 'DELETE_FROM_DATABASE'
                    }
                )

                logger.info(f'Crawler {self.md_crawler_name} created')
                break

            except Exception as e:
                if "Service is unable to assume role" in str(e):
                    logger.info('Sleeping to allow role to register correctly before crawler creation...')
                    time.sleep(5)
                elif 'AlreadyExistsException' in str(e):
                    logger.info(f'Crawler {self.md_crawler_name} already exists, skipping...')
                    break
                else:
                    logger.error(str(e))
                    raise

    def delete_crawler(self):
        response = self.glue.delete_crawler(
            Name=self.md_crawler_name
        )
        logger.info(f'Crawler {self.md_crawler_name} deleted')


class AwsLambda(AwsJobBase):
    def __init__(self, job_name, s3_bucket, s3_prefix, region):
        super().__init__(job_name, s3_bucket, s3_prefix, region)
        self.aws_lambda = self.session.client('lambda')
        self.md_crawler_function_name = self.get_name('lambda_metadata_crawler_function_name')
        self.md_lambda_crawler_role_name = self.get_name('lambda_metadata_crawler_role_name')
        self.md_crawler_name = self.get_name('glue_metadata_crawler_name')
        self.s3 = self.session.client('s3')
        self.s3_res = self.session.resource('s3')
        self.s3_lambda_code_bucket = self.get_name('s3_lambda_code_bucket')
        self.s3_lambda_md_crawler_key = self.get_name('s3_lambda_code_metadata_crawler_key')
        self.database_name = self.get_name('glue_database_name')
        self.glue_md_table_name = self.get_name('glue_metadata_table_name')
        self.s3_results_bucket = self.get_name('s3_results_bucket')
        self.md_lambda_crawler_role_arn = None
        self.lambda_metadata_etl_role_arn = None
        self.lambda_metadata_etl_role_name = self.get_name('lambda_metadata_etl_role_name')
        self.lambda_metadata_etl_function_name = self.get_name('lambda_metadata_etl_function_name')
        self.glue_metadata_parquet_results_s3_prefix = self.get_name('glue_metadata_parquet_results_s3_prefix')
        self.glue_metadata_parqeut_results_s3_path = self.get_name('glue_metadata_parqeut_results_s3_path')
        self.s3_lambda_code_metadata_etl_key = self.get_name('s3_lambda_code_metadata_etl_key')
        self.s3_lambda_code_metadata_parquet_crawler_key = self.get_name('s3_lambda_code_metadata_parquet_crawler_key')
        self.lambda_metadata_parquet_crawler_function_name = self.get_name('lambda_metadata_parquet_crawler_function_name')

        self.lambda_ts_crawler_function_name = self.get_name('lambda_ts_crawler_function_name')

        #TODO write script to bucket for ETL job:
        self.s3_glue_scripts_bucket = self.get_name('s3_glue_scripts_bucket')
        self.s3_glue_scripts_md_etl_key = self.get_name('s3_glue_scripts_md_etl_key')


    def create_roles(self):

        lambda_policy = f'''{{"Version": "2012-10-17",
        "Statement": [
          {{
            "Sid": "VisualEditor0",
            "Effect": "Allow",
            "Action": [
              "logs:CreateLogStream",
              "logs:PutLogEvents"
            ],
            "Resource": "arn:aws:logs:*:*:*"
          }},
          {{
            "Sid": "VisualEditor1",
            "Effect": "Allow",
            "Action": "glue:StartCrawler",
            "Resource": "*"
          }},
          {{
            "Sid": "VisualEditor2",
            "Effect": "Allow",
            "Action": "logs:CreateLogGroup",
            "Resource": "arn:aws:logs:*:*:*"
          }},
          {{
            "Sid": "VisualEditor3",
            "Effect": "Allow",
            "Action": "glue:GetCrawlerMetrics",
            "Resource": "*"
          }}
        ]
    }}


        '''
        self.md_lambda_crawler_role_arn = self.iam_helper.role_stitcher(self.md_lambda_crawler_role_name, 'lambda',
                                                              f'Lambda execution role for {self.md_crawler_function_name}',
                                                              policies_list=[lambda_policy])

        lambda_etl_policy = f'''{{"Version": "2012-10-17",
        "Statement": [
          {{
            "Sid": "VisualEditor0",
            "Effect": "Allow",
            "Action": [
              "logs:CreateLogStream",
              "logs:PutLogEvents"
            ],
            "Resource": "arn:aws:logs:*:*:*"
          }},
          {{
            "Sid": "VisualEditor1",
            "Effect": "Allow",
            "Action": [
                "glue:CreateJob",
                "glue:CreateScript",
                "glue:Get*"
            ],
            "Resource": "*"
          }},
          {{
            "Sid": "VisualEditor2",
            "Effect": "Allow",
            "Action": "logs:CreateLogGroup",
            "Resource": "arn:aws:logs:*:*:*"
          }}
        ]
    }}
    '''

        self.lambda_metadata_etl_role_arn = self.iam_helper.role_stitcher(self.lambda_metadata_etl_function_name,
                                                                          'lambda',
                                                                          f'Lambda execution role for {self.md_lambda_crawler_role_name}',
                                                                          policies_list=[lambda_etl_policy])

    def delete_roles(self):
        self.iam.delete_role(self.md_lambda_crawler_role_name)

    def create_crawler_function(self):
        '''
        This is the crawler running on the Firehose JSON data to define the first table to seed our ETL job
        :return:
        '''

        try:
            self.s3.create_bucket(Bucket=self.s3_lambda_code_bucket,
                                  CreateBucketConfiguration={'LocationConstraint': self.session.region_name})
            logger.info(f'{self.s3_lambda_code_bucket} s3 bucket created')

        except Exception as e:
            if 'BucketAlreadyOwnedByYou' in str(e):
                logger.info(f'{self.s3_lambda_code_bucket} s3 bucket not created - already exists')
            else:
                logger.error(str(e))
                raise

        function_script = f'''
import json
from pprint import pprint
import boto3
import time

def lambda_handler(event, context):
    client = boto3.client('glue')
    response = client.start_crawler(Name='{self.md_crawler_name}')
    
    while True:
        response = client.get_crawler_metrics(
            CrawlerNameList=[
                '{self.md_crawler_name}'
            ]
        )
        print(response)
        if response['CrawlerMetricsList'][0]['StillEstimating'] == False and response['CrawlerMetricsList'][0]['TimeLeftSeconds'] == 0.0:
            tables_response = client.get_tables(
                DatabaseName='{self.database_name}',
                Expression='*{self.s3_bucket_prefix}*'
            )
            for table in tables_response['TableList']:
                if table['Parameters']['UPDATED_BY_CRAWLER'] == '{self.md_crawler_name}':
                    return table
            print(tables_response)
            break
        else:
            print("Crawler still running")
            time.sleep(5)

        '''

        self.zip_and_s3_load(function_script,
                             'run_md_crawler.py',
                             'run_md_crawler.py.zip',
                             self.s3_lambda_code_bucket,
                             self.s3_lambda_md_crawler_key)
        '''
        zip_archive = zipfile.ZipFile('run_md_crawler.py.zip', mode='w', compression=zipfile.ZIP_STORED)

        info = zipfile.ZipInfo('run_md_crawler.py')
        info.external_attr = 0o777 << 16
        zip_archive.writestr(info, function_script)

        zip_archive.close()
        '''

        #os.chmod('run_crawler.py.zip', stat.S_IXUSR | stat.S_IXGRP | stat.S_IXOTH)

        #self.s3.upload_file('run_md_crawler.py.zip', self.s3_lambda_code_bucket, self.s3_lambda_md_crawler_key)

        print(self.md_lambda_crawler_role_arn)

        while 1 == 1:
            try:

                response = self.aws_lambda.create_function(
                    FunctionName=self.md_crawler_function_name,
                    Runtime='python3.7',
                    Role=self.md_lambda_crawler_role_arn,
                    Handler='run_md_crawler.lambda_handler',
                    Code={
                        'S3Bucket': self.s3_lambda_code_bucket,
                        'S3Key': self.s3_lambda_md_crawler_key
                    },
                    Description=f'Lambda for crawler exection on job {self.job_name}',
                    Timeout=900,
                    MemorySize=128,
                    Publish=True,
                    Tags={
                        'job': self.job_name
                    }
                )

                print(response)
                break

            except Exception as e:
                if 'role defined for the function cannot be assumed' in str(e):
                    logger.info(f"Lamda role not registered for {self.md_lambda_crawler_role_name} - sleeping ...")
                    time.sleep(5)
                elif 'Function already exist' in str(e):
                    logger.info(f'Lambda function {self.md_crawler_function_name} exists, skipping...')
                    break
                else:
                    logger.error(str(e))
                    raise

    def create_etl_script_function(self):

        try:
            self.s3.create_bucket(Bucket=self.s3_lambda_code_bucket,
                                  CreateBucketConfiguration={'LocationConstraint': self.session.region_name})
            logger.info(f'{self.s3_lambda_code_bucket} s3 bucket created')

        except Exception as e:
            if 'BucketAlreadyOwnedByYou' in str(e):
                logger.info(f'{self.s3_lambda_code_bucket} s3 bucket not created - already exists')
            else:
                logger.error(str(e))
                raise
        try:
            self.s3.create_bucket(Bucket=self.s3_glue_scripts_bucket,
                                  CreateBucketConfiguration={'LocationConstraint': self.session.region_name})
            logger.info(f'{self.s3_glue_scripts_bucket} s3 bucket created')

        except Exception as e:
            if 'BucketAlreadyOwnedByYou' in str(e):
                logger.info(f'{self.s3_lambda_code_bucket} s3 bucket not created - already exists')
            else:
                logger.error(str(e))
                raise

        function_script = '''
import json
from pprint import pprint
import boto3

def lambda_handler(event, context):
    client = boto3.client('glue')
    response = client.get_table(DatabaseName='%s', Name='%s')
    pprint(response['Table']['StorageDescriptor']['Columns'])

    columns = []
    for column in response['Table']['StorageDescriptor']['Columns']:
        columns.append((column['Name'], column['Type'], column['Name'], column['Type']))
        

    
    response = client.create_script(
        DagNodes=[
            {
                'NodeType': 'DataSource',
                'Id': 'datasource0',
                'Args': [{
                    'Param': False,
                    'Name': 'database',
                    'Value': '"%s"'
                }, {
                    'Param': False,
                    'Name': 'table_name',
                    'Value': '"%s"'
                }, {
                    'Param': False,
                    'Name': 'transformation_ctx',
                    'Value': '"datasource0"'
                }],
                'LineNumber': 18
            },
            {
                'NodeType': 'ApplyMapping',
                'Id': 'applymapping1',
                'Args': [{
                    'Param': False,
                    'Name': 'mappings',
                    'Value': f'{columns}'
                }, {
                    'Param': False,
                    'Name': 'transformation_ctx',
                    'Value': '"applymapping1"'
                }],
    
                'LineNumber': 20
            },
            {
                'Args': [{
                    'Param': False,
                    'Name': 'choice',
                    'Value': '"make_struct"'
                }, {
                    'Param': False,
                    'Name': 'transformation_ctx',
                    'Value': '"resolvechoice2"'
                }],
                'NodeType': 'ResolveChoice',
                'Id': 'resolvechoice2',
                'LineNumber': 22
            },
            {
                'Args': [{
                    'Param': False,
                    'Name': 'transformation_ctx',
                    'Value': '"dropnullfields3"'
                }],
                'NodeType': 'DropNullFields',
                'Id': 'dropnullfields3',
                'LineNumber': 25
            },
            {
                'NodeType': 'DataSink',
                'Id': 'datasink4',
                'Args': [{
                    'Param': False,
                    'Name': 'connection_type',
                    'Value': '"s3"'
                }, {
                    'Param': False,
                    'Name': 'format',
                    'Value': '"parquet"'
                }, {
                    'Param': False,
                    'Name': 'connection_options',
                    'Value': '%s'
                }, {
                    'Param': False,
                    'Name': 'transformation_ctx',
                    'Value': '"datasink4"'
                }],
    
                'LineNumber': 27
            }
        ],
    
        DagEdges=[
            {
                'Source': 'datasource0',
                'TargetParameter': 'frame',
                'Target': 'applymapping1'
            },
            {
                'Source': 'applymapping1',
                'TargetParameter': 'frame',
                'Target': 'resolvechoice2'
            },
            {
                'Source': 'resolvechoice2',
                'TargetParameter': 'frame',
                'Target': 'dropnullfields3'
            },
            {
                'Source': 'dropnullfields3',
                'TargetParameter': 'frame',
                'Target': 'datasink4'
            }
        ],
    
        Language='PYTHON')
    
    print(response['PythonScript'])

        '''  % (self.database_name, self.glue_md_table_name, self.database_name, self.glue_md_table_name, self.glue_metadata_parqeut_results_s3_path)

        self.zip_and_s3_load(function_script,
                             'run_etl_job_creation.py',
                             'run_etl_job_creation.py.zip',
                             self.s3_lambda_code_bucket,
                             self.s3_lambda_code_metadata_etl_key)

        while True:
            try:
                response = self.aws_lambda.create_function(
                    FunctionName=self.lambda_metadata_etl_function_name,
                    Runtime='python3.7',
                    Role=self.lambda_metadata_etl_role_arn,
                    Handler='run_etl_job_creation.lambda_handler',
                    Code={
                        'S3Bucket': self.s3_lambda_code_bucket,
                        'S3Key': self.s3_lambda_code_metadata_etl_key
                    },
                    Description=f'Lambda for etl job creation on job {self.job_name}',
                    Timeout=900,
                    MemorySize=128,
                    Publish=True,
                    Tags={
                        'job': self.job_name
                    }
                )

                print(response)
                break

            except Exception as e:
                if 'role defined for the function cannot be assumed' in str(e):
                    logger.info(f"Lamda role not registered for {self.lambda_metadata_etl_role_name} - sleeping ...")
                    time.sleep(5)

                elif 'Function already exist' in str(e):
                    logger.info(f'Lambda function {self.lambda_metadata_etl_function_name} exists, skipping...')
                    break

                else:
                    logger.error(str(e))
                    raise

    def create_parquet_crawler_function(self):
        '''
        This is the crawler running on the Firehose JSON data to define the first table to seed our ETL job
        :return:
        '''

        try:
            self.s3.create_bucket(Bucket=self.s3_lambda_code_bucket,
                                  CreateBucketConfiguration={'LocationConstraint': self.session.region_name})
            logger.info(f'{self.s3_lambda_code_bucket} s3 bucket created')

        except Exception as e:
            if 'BucketAlreadyOwnedByYou' in str(e):
                logger.info(f'{self.s3_lambda_code_bucket} s3 bucket not created - already exists')
            else:
                logger.error(str(e))
                raise

        function_script = f'''
import json
from pprint import pprint
import boto3
import time

def lambda_handler(event, context):
    client = boto3.client('glue')
    response = client.start_crawler(Name='{self.lambda_ts_crawler_function_name}')

    while True:
        response = client.get_crawler_metrics(
            CrawlerNameList=[
                '{self.md_crawler_name}'
            ]
        )
        print(response)
        if response['CrawlerMetricsList'][0]['StillEstimating'] == False and response['CrawlerMetricsList'][0]['TimeLeftSeconds'] == 0.0:
            tables_response = client.get_tables(
                DatabaseName='{self.database_name}',
                Expression='*{self.s3_bucket_prefix}*'
            )
            for table in tables_response['TableList']:
                if table['Parameters']['UPDATED_BY_CRAWLER'] == '{self.md_crawler_name}':
                    return table
            print(tables_response)
            break
        else:
            print("Crawler still running")
            time.sleep(5)

        '''

        self.zip_and_s3_load(function_script,
                             'run_md_crawler.py',
                             'run_md_crawler.py.zip',
                             self.s3_lambda_code_bucket,
                             self.s3_lambda_code_metadata_parquet_crawler_key)

        while 1 == 1:
            try:

                response = self.aws_lambda.create_function(
                    FunctionName=self.lambda_metadata_parquet_crawler_function_name,
                    Runtime='python3.7',
                    Role=self.md_lambda_crawler_role_arn,
                    Handler='run_md_crawler.lambda_handler',
                    Code={
                        'S3Bucket': self.s3_lambda_code_bucket,
                        'S3Key': self.s3_lambda_code_metadata_parquet_crawler_key
                    },
                    Description=f'Lambda for crawler execution on job {self.job_name}',
                    Timeout=900,
                    MemorySize=128,
                    Publish=True,
                    Tags={
                        'job': self.job_name
                    }
                )

                print(response)
                break

            except Exception as e:
                if 'role defined for the function cannot be assumed' in str(e):
                    logger.info(f"Lamda role not registered for {self.md_lambda_crawler_role_name} - sleeping ...")
                    time.sleep(5)
                elif 'Function already exist' in str(e):
                    logger.info(f'Lambda function {self.lambda_metadata_parquet_crawler_function_name} exists, skipping...')
                    break
                else:
                    logger.error(str(e))
                    raise


class AwsBatchEnv(AwsJobBase):

    def __init__(self, job_name, s3_bucket, s3_prefix, region, use_spot=True):
        super().__init__(job_name, s3_bucket, s3_prefix, region)
        self.job_name = job_name
        self.s3_bucket = s3_bucket
        # TODO should build in more controls for names to comply with AWS standards:
        self.job_identifier = re.sub('[^0-9a-zA-Z]+', '_', self.job_name)
        self.use_spot = use_spot
        # AWS clients
        #self.region = self.session.region
        self.batch = self.session.client('batch')
        #self.iam = self.session.client('iam')
        # Naming conventions
        self.compute_environment_name = self.get_name('batch_compute_environment_name')
        self.job_queue_name = self.get_name('batch_job_queue_name')
        self.service_role_name = self.get_name('batch_service_role_name')
        self.instance_role_name = self.get_name('batch_instance_role_name')
        self.instance_profile_name = self.get_name('batch_instance_profile_name')
        self.spot_service_role_name = self.get_name('batch_spot_service_role_name')
        self.task_role_name = self.get_name('batch_ecs_task_role_name')
        self.task_policy_name = self.get_name('batch_task_policy_name')
        # Bucket information
        #self.s3_bucket_arn = self.get_name('batch_compute_environment_name')
        #self.s3_prefix = self.s3_bucket_prefix
        # These are populated by functions below - although there is no controller the order of operation is reflected by order in the file.
        self.task_role_arn = None
        self.job_definition_arn = None
        self.instance_role_arn = None
        self.spot_service_role_arn = None
        self.service_role_arn = None
        self.instance_profile_arn = None

        logger.propagate = False

    def __repr__(self):

        return f"""
The following objects compose the environment to support the Batch run for {self.job_name}:
Job Definition Name: {self.job_identifier}
Compute Environment: {self.compute_environment_name}
Job Queue Name: {self.job_queue_name}
Batch Service Role Name: {self.service_role_name}
Instance Role Name: {self.instance_role_name}
Instance Profile Name: {self.instance_profile_name}
Instance Profile ARN:  {self.instance_profile_arn}
Spot Fleet Service Role (if use_spot): {self.spot_service_role_name}
Task Role Name: {self.task_role_name}
Task Role Policy Name: {self.task_policy_name}
S3 Bucket: {self.s3_bucket}
S3 Bucket ARN: {self.s3_bucket_arn}
"""

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
        Creates the IAM roles used in the various areas of the batch service. This currently will not try to overwrite or update existing roles.
        """
        self.service_role_arn = self.iam_helper.role_stitcher(self.service_role_name,
                                      "batch",
                                      f"Service role for Batch environment {self.job_identifier}",
                                      managed_policie_arns=['arn:aws:iam::aws:policy/service-role/AWSBatchServiceRole'])

        ## Instance Role for Batch compute environment

        self.instance_role_arn = self.iam_helper.role_stitcher(self.instance_role_name,
                                                               "ec2",
                                                               f"Instance role for Batch compute environment {self.job_identifier}",
                                                               managed_policie_arns=[
                                                                   'arn:aws:iam::aws:policy/service-role/AmazonEC2ContainerServiceforEC2Role'])


        # Instance Profile

        try:
            response = self.iam.create_instance_profile(
                InstanceProfileName=self.instance_profile_name
            )

            self.instance_profile_arn = response['InstanceProfile']['Arn']

            logger.info("Instance Profile created")

            response = self.iam.add_role_to_instance_profile(
                InstanceProfileName=self.instance_profile_name,
                RoleName=self.instance_role_name
            )

        except Exception as e:
            if 'EntityAlreadyExists' in str(e):
                logger.info('ECS Instance Profile not created - already exists')
                response = self.iam.get_instance_profile(
                    InstanceProfileName=self.instance_profile_name
                )
                self.instance_profile_arn = response['InstanceProfile']['Arn']

        # ECS Task Policy

        print(self.s3_bucket)
        print(self.s3_bucket_arn)

        # TODO: slim this down


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

        self.task_role_arn = self.iam_helper.role_stitcher(self.task_role_name,
                                                               "ecs-tasks",
                                                               f"Task role for Batch job {self.job_identifier}",
                                                               policies_list=[task_permissions_policy])


        if self.use_spot:

            # Spot Fleet Role
            self.spot_service_role_arn = self.iam_helper.role_stitcher(self.spot_service_role_name,
                                                               "spotfleet",
                                                               f"Spot Fleet role for Batch compute environment {self.job_identifier}",
                                                               managed_policie_arns=[
                                                                   'arn:aws:iam::aws:policy/service-role/AmazonEC2SpotFleetTaggingRole'])


    def create_compute_environment(self, subnets, security_groups, maxCPUs=10000):
        """
        Creates a compute environment suffixed with the job name
        :param subnets: list of subnet IDs
        :param security_groups: list of security group IDs
        :param maxCPUs: numeric value for max VCPUs for the envionment

        """
        # Trying to accomodate a list or a string here:
        if not isinstance(security_groups, list):
            security_groups = [security_groups]

        if len(subnets) != len(set(subnets)):
            raise ValueError("There are duplicate subnets listed.  Please correct and resubmit.")

        if len(security_groups) != len(set(security_groups)):
            raise ValueError("There are duplicate security groups listed.  Please correct and resubmit.")

        if self.use_spot:
            type = 'SPOT'
            try:
                response = self.batch.create_compute_environment(
                    computeEnvironmentName=self.compute_environment_name,
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
                        'subnets': subnets,
                        'securityGroupIds': security_groups,
                        # 'ec2KeyPair': key_pair,
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
                    logger.error(str(e))

        else:
            type = 'EC2'
            try:
                response = self.batch.create_compute_environment(
                    computeEnvironmentName=self.compute_environment_name,
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
                        'subnets': subnets,
                        'securityGroupIds': security_groups,
                        # 'ec2KeyPair': key_pair,
                        'instanceRole': self.instance_profile_arn
                    },
                    serviceRole=self.service_role_arn
                )

                logger.info('Service Role created')

            except Exception as e:
                if 'Object already exists' in str(e):
                    logger.info('Compute environment not created - already exists')
                else:
                    logger.error(str(e))

    def create_job_queue(self):
        """
        Creates a job queue based on the Batch environment definition
        """
        go = True
        while go:
            try:
                response = self.batch.create_job_queue(
                    jobQueueName=self.job_queue_name,
                    state='ENABLED',
                    priority=1,
                    computeEnvironmentOrder=[
                        {
                            'order': 1,
                            'computeEnvironment': self.compute_environment_name
                        },
                    ]
                )

                logger.info('Job queue created')
                go = False

            except Exception as e:
                if 'Object already exists' in str(e):
                    logger.info('Job queue not created - already exists')
                    go = False

                elif 'is not valid' in str(e):
                    # Need to wait a second for the compute environment to complete registration
                    logger.error(
                        '5 second sleep initiated to wait for compute environment creation due to error: ' + str(e))
                    time.sleep(5)

                else:
                    logger.error(str(e))
                    go = False

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
            }
        )

        self.job_definition_arn = response['jobDefinitionArn']

    def submit_job(self, array_size=4):
        """
        Submits the created job definition and version to be run.
        """
        go = True
        while go:
            try:
                response = self.batch.submit_job(
                    jobName=self.job_identifier,
                    jobQueue=self.job_queue_name,
                    arrayProperties={
                        'size': array_size
                    },
                    jobDefinition=self.job_definition_arn
                )

                go = False

                logger.info(f"Job {self.job_identifier} submitted.")

            except Exception as e:

                if 'not in VALID state' in str(e):
                    # Need to wait a second for the compute environment to complete registration
                    logger.error('5 second sleep initiated to wait for job queue creation due to error: ' + str(e))
                    time.sleep(5)
                else:
                    logger.error(str(e))
                    go = False


class AwsBatch(DockerBatchBase):

    def __init__(self, project_filename):
        super().__init__(project_filename)
        self.job_identifier = re.sub('[^0-9a-zA-Z]+', '_', project_filename)

        self.project_filename = project_filename
        self.region = self.cfg['aws']['region']
        self.ecr = boto3.client('ecr', region_name=self.region)
        self.s3 = boto3.client('s3', region_name=self.region)
        self.s3_bucket = self.cfg['aws']['s3']['bucket']
        self.s3_bucket_prefix = self.cfg['aws']['s3']['prefix']
        self.batch_env_subnet = self.cfg['aws']['subnet']
        self.batch_env_use_spot = self.cfg['aws']['use_spot']
        self.security_group = self.cfg['aws']['security_group']
        self.batch_array_size = self.cfg['aws']['batch_array_size']


    @classmethod
    def docker_image(cls):
        return 'nrel/buildstockbatch'

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
        with tempfile.TemporaryDirectory() as tmpdir:
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

            logger.debug('Uploading files to S3')
            upload_directory_to_s3(
                tmppath,
                self.cfg['aws']['s3']['bucket'],
                self.cfg['aws']['s3']['prefix']
            )

        # TODO: Review Compute Environment, Job queue, IAM Roles, Job Defn, Start Job

        # Define the batch environment
        batch_env = AwsBatchEnv(self.job_identifier, self.s3_bucket, self.s3_bucket_prefix, self.region, self.batch_env_use_spot)
        logger.info(
            "Launching Batch environment - (resource configs will not be updated on subsequent executions, but new job revisions will be created):")
        logger.debug(str(batch_env))
        batch_env.create_batch_service_roles()
        batch_env.create_compute_environment([self.batch_env_subnet], self.security_group)
        batch_env.create_job_queue()

        # Pass through config for the Docker containers
        env_vars = dict(S3_BUCKET=self.s3_bucket, S3_PREFIX=self.s3_bucket_prefix, JOB_NAME=self.job_identifier, REGION=self.region)

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

        # Initialize the firehose environment and try to create it
        firehose_env = AwsFirehose(self.job_identifier, self.s3_bucket, self.s3_bucket_prefix, self.region)
        logger.debug(str(firehose_env))
        firehose_env.create_firehose_delivery_role()
        firehose_env.create_firehose_buckets()
        firehose_env.create_firehose()
        firehose_env.add_firehose_task_permissions(batch_env.task_role_name)

        # Create database and 2 crawlers to manage the resulting data
        glue_env = AWSGlueTransform(self.job_identifier, self.s3_bucket, self.s3_bucket_prefix, self.region)
        glue_env.create_database()
        glue_env.create_roles()
        glue_env.create_crawler()
        glue_env.create_parquet_crawler()


        # Set up 3 functions to manage ETL after the Batch job completes
        lambda_env = AwsLambda(self.job_identifier, self.s3_bucket, self.s3_bucket_prefix, self.region)
        lambda_env.create_roles()
        lambda_env.create_crawler_function()
        lambda_env.create_etl_script_function()
        lambda_env.create_parquet_crawler_function()

        batch_env.submit_job(array_size)

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
        firehose = boto3.client('firehose', region_name=region)
        sim_dir = pathlib.Path('/var/simdata/openstudio')

        firehose_name = f"{job_name.replace(' ', '_')}_firehose"

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
        epws_to_download = df.loc[[x[0] for x in jobs_d['batch']], 'Location EPW'].unique().tolist()
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

            logger.debug('Uploading simulation outputs')
            for dirpath, dirnames, filenames in os.walk(sim_dir):
                # Remove the asset directories from upload
                if pathlib.Path(dirpath) == sim_dir:
                    for dirname in asset_dirs:
                        dirnames.remove(dirname)
                for filename in filenames:
                    filepath = pathlib.Path(dirpath, filename)
                    logger.debug('Uploading {}'.format(filepath.relative_to(sim_dir)))
                    s3.upload_file(
                        str(filepath),
                        bucket,
                        str(pathlib.Path(prefix, 'results', sim_id, filepath.relative_to(sim_dir)))
                    )

            logger.debug('Writing output data to Firehose')
            datapoint_out_filepath = sim_dir / 'run' / 'data_point_out.json'
            out_osw_filepath = sim_dir / 'out.osw'
            if os.path.isfile(out_osw_filepath):
                out_osw = read_out_osw(out_osw_filepath)
                dp_out = flatten_datapoint_json(read_data_point_out_json(datapoint_out_filepath))
                if dp_out is None:
                    dp_out = {}
                dp_out.update(out_osw)
                dp_out['_id'] = sim_id
                for key in dp_out.keys():
                    dp_out[to_camelcase(key)] = dp_out.pop(key)
                # TODO: write dp_out to firehose

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


            logger.debug('Clearing out simulation directory')
            for item in set(os.listdir(sim_dir)).difference(asset_dirs):
                if os.path.isdir(item):
                    shutil.rmtree(item)
                elif os.path.isfile(item):
                    os.remove(item)


if __name__ == '__main__':
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
        args = parser.parse_args()
        batch = AwsBatch(args.project_filename)
        batch.push_image()
        batch.run_batch()
