import boto3
import logging
import re
import time
import zipfile

from buildstockbatch.localdocker import DockerBatchBase

logger = logging.getLogger(__name__)



class AWSIAMHelper():
    def __init__(self, session):
        '''
        Initialize the AWSIAM class with a boto3 Session
        :param session: boto3 Session from 'parent' job base class
        '''
        self.session = session
        self.iam = self.session.client('iam')

    def role_stitcher(self, role_name, trust_service, description, policies_list=[], managed_policie_arns=[]):
        '''
        Creates a role and attached the policies - will catch errors and skip if role already exists
        :param role_name: Name of service role to create
        :param trust_service: Trusted service to associate with the service role
        :param description: Description of role
        :param policies_list: List of JSON policies (optional)
        :param managed_policie_arns: Managed policies to attach (optional)
        :return: Role ARN is returned
        '''
        role_arn = None
        trust_policy = f'''{{
                        "Version": "2012-10-17",
                        "Statement": [{{
                            "Effect": "Allow",
                            "Principal": {{
                                "Service": "{trust_service}.amazonaws.com"
                            }},
                            "Action": "sts:AssumeRole"
                        }}]
                    }}
                '''

        try:
            response = self.iam.create_role(
                Path='/',
                RoleName=role_name,
                AssumeRolePolicyDocument=trust_policy,
                Description=description
            )
            role_arn = response['Role']['Arn']

            p_counter = 1
            for policy in policies_list:
                # print(p_counter)
                print(policy)
                response = self.iam.put_role_policy(
                    RoleName=role_name,
                    PolicyName=f'{role_name}_policy_{p_counter}',
                    PolicyDocument=policy
                )
                p_counter = p_counter + 1

            for managed_policy_arn in managed_policie_arns:
                response = self.iam.attach_role_policy(
                    PolicyArn=managed_policy_arn,
                    RoleName=role_name
                )

            logger.info(f'Role {role_name} created')

            return role_arn

        except Exception as e:
            if 'EntityAlreadyExists' in str(e):
                logger.info(f'Role {role_name} not created - already exists')
                response = self.iam.get_role(
                    RoleName=role_name
                )
                role_arn = response['Role']['Arn']
                return role_arn

            else:
                logger.error(str(e))
                raise

    def delete_role(self, role_name):
        '''
        Delete a role
        :param role_name: name of the role to delete
        :return: None
        '''

        response = self.iam.list_role_policies(
            RoleName=role_name
        )
        print(response)

        for policy in response['PolicyNames']:
            del_response = self.iam.delete_role_policy(
                RoleName=role_name,
                PolicyName=policy
            )

        logger.info(f'Policies removed from role {role_name}.')

        response = self.iam.delete_role(
            RoleName=role_name
        )
        logger.info(f'Role {role_name} deleted.')



class AwsJobBase():
    def __init__(self, job_name, s3_bucket, s3_bucket_prefix, region):

        self.job_name = job_name
        self.job_identifier = re.sub('[^0-9a-zA-Z]+', '_', self.job_name).replace('_yml','')
        self.s3_bucket = s3_bucket
        self.s3_bucket_arn = f"arn:aws:s3:::{self.s3_bucket}"
        self.s3_bucket_prefix = s3_bucket_prefix
        self.region = region

        self.lambda_metadata_crawler_function_name = f'{self.job_identifier}_start_metadata_crawler'
        self.lambda_metadata_crawler_role_name = f'{self.job_identifier}_lambda_metadata_execution_role'
        self.lambda_metadata_etl_function_name = f'{self.job_identifier}_lambda_metadata_etl'
        self.lambda_metadata_etl_role_name = f'{self.job_identifier}_lambda_metadata_etl_role'
        self.lambda_metadata_summary_crawler_function_name = f'{self.job_identifier}_lambda_metadata_summary_crawler'


        self.lambda_ts_crawler_function_name = f'{self.job_identifier}_start_ts_crawler'
        self.lambda_ts_crawler_role_name = f'{self.job_identifier}_lambda_ts_execution_role'


        self.batch_job_name = f'{self.job_identifier}_batch_job'
        self.s3_results_bucket = f'{self.s3_bucket}-result'
        self.s3_results_bucket_arn = f"arn:aws:s3:::{self.s3_bucket}-result"
        self.s3_results_backup_bucket = f"{self.s3_bucket}-backups"
        self.s3_results_backup_bucket_arn = f"arn:aws:s3:::{self.s3_bucket}-backups"

        self.s3_lambda_code_bucket = f'nrel-{self.job_identifier}_lambda_functions'.replace('_', '-')
        self.s3_lambda_code_metadata_crawler_key = f'{self.job_identifier}/run_md_crawler.py.zip'
        self.s3_lambda_code_metadata_etl_key = f'{self.job_identifier}/run_md_etl.py.zip'
        self.s3_lambda_code_metadata_summary_crawler_key = f'{self.job_identifier}/run_md_summary_crawler.py.zip'

        self.s3_glue_scripts_bucket = f'nrel-{self.job_identifier}_glue_scripts'.replace('_', '-')
        self.s3_glue_scripts_md_etl_key = f'nrel-{self.job_identifier}_glue_etl'.replace('_', '-')
        self.glue_etl_script_name = 'glue_etl_script'
        self.s3_glue_etl_script_path = f's3://{self.s3_glue_scripts_bucket}/{self.s3_bucket_prefix}/{self.glue_etl_script_name}'


        self.glue_metadata_crawler_name = f'{self.job_identifier}_md_crawler'
        self.glue_metadata_crawler_role_name = f'{self.job_identifier}_md_crawler_role'
        self.glue_database_name = f'{self.job_identifier}_data'
        self.glue_metadata_job_name = f'{self.job_identifier}_glue_job'
        self.glue_metadata_etl_role_name = f'{self.job_identifier}_md_etl_role'
        self.glue_metadata_etl_job_name = f'{self.job_identifier}_md_etl_job'

        #self.glue_metadata_table_name = f'{self.job_identifier}_md_table'
        # Set by crawler - should be this:
        self.glue_metadata_table_name = self.s3_bucket_prefix

        self.glue_metadata_summary_table_name = f'{self.job_identifier}_md_summary_table'
        self.glue_metadata_summary_crawler_name = f'{self.job_identifier}_md_summary_crawler'
        #self.glue_metadata_summary_crawler_role_name = f'{self.job_identifier}_md_parquet_crawler_role'
        #self.glue_metadata_summary_results_s3_prefix = f'{self.s3_bucket_prefix}_parquet/metadata/'
        self.glue_metadata_etl_output_type = "csv"
        self.glue_metadata_etl_results_s3_path = f's3//{self.s3_bucket}-result/{self.s3_bucket_prefix}_summary/{self.glue_metadata_etl_output_type}/'


        self.s3_lambda_code_ts_crawler_key = f'{self.job_identifier}/run_crawler.py.zip'
        self.glue_ts_table_name = f'{self.job_identifier}_ts_table'
        self.glue_ts_parquet_table_name = f'{self.job_identifier}_ts_parquet_table'
        self.glue_ts_parquet_crawler_name = f'{self.job_identifier}_ts_parquet_crawler'
        self.glue_ts_parquet_crawler_role_name = f'{self.job_identifier}_ts_parquet_crawler_role'
        self.glue_ts_parquet_results_s3_prefix = f'{self.s3_bucket_prefix}_parquet/timeseries/'

        # crawler=f'{self.job_identifier}',
        # crawler=f'{self.job_identifier}',

        self.batch_compute_environment_name = f"computeenvionment_{self.job_identifier}"
        self.batch_job_queue_name = f"job_queue_{self.job_identifier}"
        self.batch_service_role_name = f"batch_service_role_{self.job_identifier}"
        self.batch_instance_role_name = f"batch_instance_role_{self.job_identifier}"
        self.batch_instance_profile_name = f"batch_instance_profile_{self.job_identifier}"
        self.batch_spot_service_role_name = f"spot_fleet_role_{self.job_identifier}"
        self.batch_ecs_task_role_name = f"ecs_task_role_{self.job_identifier}"
        self.batch_task_policy_name = f"ecs_task_policy_{self.job_identifier}"

        # crawler=f'{self.job_identifier}',
        # crawler=f'{self.job_identifier}'

        self.firehose_role = f"{self.job_identifier}_firehose_delivery_role"
        self.firehose_name = f"{self.job_identifier}_firehose"
        self.firehose_role_policy_name = f"{self.job_identifier}_firehose_delivery_policy"
        self.firehost_task_policy_name = f"{self.job_identifier}_firehose_task_policy"

        self.state_machine_name = f"{self.job_identifier}_state_machine"
        self.state_machine_role_name = f"{self.job_identifier}_state_machine_role"

        self.sns_state_machine_topic =  f"{self.job_identifier}_state_machine_notifications"
        self.operator_email = 'david.rager@nrel.gov'


        '''
        self.named_items = dict(

            lambda_metadata_crawler_function_name=f'{self.job_identifier}_start_metadata_crawler',
            lambda_metadata_crawler_role_name=f'{self.job_identifier}_lambda_metadata_execution_role',
            lambda_metadata_etl_function_name=f'{self.job_identifier}_lambda_metadata_etl',
            lambda_metadata_etl_role_name=f'{self.job_identifier}_lambda_metadata_etl_role',
            lambda_metadata_parquet_crawler_function_name=f'{self.job_identifier}_lambda_metadata_parquet_crawler',

            #TODO:
            lambda_ts_crawler_function_name=f'{self.job_identifier}_start_ts_crawler',
            lambda_ts_crawler_role_name=f'{self.job_identifier}_lambda_ts_execution_role',

            batch_job_name=f'{self.job_identifier}_batch_job',
            s3_results_bucket=f'{self.s3_bucket}-result',
            s3_results_bucket_arn=f"arn:aws:s3:::{self.s3_bucket}-result",
            s3_results_backup_bucket=f"{self.s3_bucket}-backups",
            s3_results_backup_bucket_arn=f"arn:aws:s3:::{self.s3_bucket}-backups",

            s3_lambda_code_bucket=f'nrel-{self.job_identifier}_lambda_functions'.replace('_','-'),
            s3_lambda_code_metadata_crawler_key= f'{self.job_identifier}/run_md_crawler.py.zip',
            s3_lambda_code_metadata_etl_key=f'{self.job_identifier}/run_md_etl.py.zip',
            s3_lambda_code_metadata_parquet_crawler_key=f'{self.job_identifier}/run_md_parquet_crawler.py.zip',

            s3_glue_scripts_bucket=f'nrel-{self.job_identifier}_glue_scripts'.replace('_','-'),
            s3_glue_scripts_md_etl_key=f'nrel-{self.job_identifier}_glue_etl'.replace('_', '-'),

            glue_metadata_crawler_name=f'{self.job_identifier}_md_crawler',
            glue_metadata_crawler_role_name=f'{self.job_identifier}_md_crawler_role',
            glue_metadata_etl_role_name=f'{self.job_identifier}_md_crawler_role',
            glue_database_name=f'{self.job_identifier}_data',
            glue_metadata_job_name=f'{self.job_identifier}_glue_job',
            glue_metadata_table_name = f'{self.job_identifier}_md_table',
            glue_metadata_consolidated_table_name = f'{self.job_identifier}_md_parquet_table',
            glue_metadata_consolidated_crawler_name = f'{self.job_identifier}_md_parquet_crawler',
            glue_metadata_parquet_crawler_role_name = f'{self.job_identifier}_md_parquet_crawler_role',
            glue_metadata_parquet_results_s3_prefix = f'{self.s3_bucket_prefix}_parquet/metadata/',
            glue_metadata_parqeut_results_s3_path = f's3//{self.s3_bucket}-result/{self.s3_bucket_prefix}_parquet/metadata/',


            s3_lambda_code_ts_crawler_key=f'{self.job_identifier}/run_crawler.py.zip',
            glue_ts_table_name=f'{self.job_identifier}_ts_table',
            glue_ts_parquet_table_name=f'{self.job_identifier}_ts_parquet_table',
            glue_ts_parquet_crawler_name=f'{self.job_identifier}_ts_parquet_crawler',
            glue_ts_parquet_crawler_role_name=f'{self.job_identifier}_ts_parquet_crawler_role',
            glue_ts_parquet_results_s3_prefix=f'{self.s3_bucket_prefix}_parquet/timeseries/',


            #crawler=f'{self.job_identifier}',
            #crawler=f'{self.job_identifier}',

            batch_compute_environment_name = f"computeenvionment_{self.job_identifier}",
            batch_job_queue_name=f"job_queue_{self.job_identifier}",
            batch_service_role_name=f"batch_service_role_{self.job_identifier}",
            batch_instance_role_name=f"batch_instance_role_{self.job_identifier}",
            batch_instance_profile_name = f"batch_instance_profile_{self.job_identifier}",
            batch_spot_service_role_name=f"spot_fleet_role_{self.job_identifier}",
            batch_ecs_task_role_name=f"ecs_task_role_{self.job_identifier}",
            batch_task_policy_name = f"ecs_task_policy_{self.job_identifier}",

            #crawler=f'{self.job_identifier}',
            #crawler=f'{self.job_identifier}'




            firehose_role=f"{self.job_identifier}_firehose_delivery_role",
            firehose_name=f"{self.job_identifier}_firehose",
            firehose_role_policy_name = f"{self.job_identifier}_firehose_delivery_policy",
            firehost_task_policy_name = f"{self.job_identifier}_firehose_task_policy"
        )
        
        '''

        self.session = boto3.Session(region_name=region)
        self.iam_helper = AWSIAMHelper(self.session)
        self.iam = self.iam_helper.iam
        self.s3 = self.session.client('s3')


    def __repr__(self):

        return f"""
Job Identifier: {self.job_identifier}
Job Name: {self.job_name}
S3 Bucket for Source:  {self.s3_bucket}
S3 Prefix for Source:  {self.s3_bucket_prefix}
"""
    '''
    def get_name(self,type):

        return self.named_items[type]
    '''
    def set_name(self,type, name):
        self.named_items[type] = name

    def zip_and_s3_load(self, string_to_zip, file_name, zip_name, s3_destination_bucket, s3_destination_key):
        zip_archive = zipfile.ZipFile(zip_name, mode='w', compression=zipfile.ZIP_STORED)

        info = zipfile.ZipInfo(file_name)
        info.date_time = time.localtime()
        info.external_attr = 0o100755 << 16

        zip_archive.writestr(info, string_to_zip, zipfile.ZIP_DEFLATED)

        zip_archive.close()

        #os.chmod('run_crawler.py.zip', stat.S_IXUSR | stat.S_IXGRP | stat.S_IXOTH)

        self.s3.upload_file(zip_name, s3_destination_bucket, s3_destination_key)

        logger.info(f'{zip_name} uploaded to bucket {s3_destination_bucket} under key {s3_destination_key}' )
