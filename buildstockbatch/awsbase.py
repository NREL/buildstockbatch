import logging
import time
import zipfile


logger = logging.getLogger(__name__)


class AWSIAMHelper():

    logger.propagate = False

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
                raise

    def delete_role(self, role_name):
        '''
        Delete a role
        :param role_name: name of the role to delete
        :return: None
        '''
        try:
            response = self.iam.list_role_policies(
                RoleName=role_name
            )

            for policy in response['PolicyNames']:
                self.iam.delete_role_policy(
                    RoleName=role_name,
                    PolicyName=policy
                )

            response = self.iam.list_attached_role_policies(
                RoleName=role_name
            )

            for policy in response['AttachedPolicies']:
                self.iam.detach_role_policy(
                        RoleName=role_name,
                        PolicyArn=policy['PolicyArn']
                    )

            logger.info(f'Policies detached from role {role_name}.')

            response = self.iam.delete_role(
                RoleName=role_name
            )
            logger.info(f'Role {role_name} deleted.')
        except Exception as e:
            if 'NoSuchEntity' in str(e):
                logger.info(f'Role {role_name} missing, skipping...')
            else:
                raise

    def delete_instance_profile(self, instance_profile_name):

        try:
            self.iam.delete_instance_profile(
                InstanceProfileName=instance_profile_name
            )
            logger.info(f"Instance profile {instance_profile_name} deleted.")
        except Exception as e:
            if 'NoSuchEntity' in str(e):
                logger.info(f"Instance profile {instance_profile_name} missing, skipping...")
            else:
                raise

    def remove_role_from_instance_profile(self, instance_profile_name):
        try:
            response = self.iam.get_instance_profile(
                InstanceProfileName=instance_profile_name
            )

            for role in response['InstanceProfile']['Roles']:
                response = self.iam.remove_role_from_instance_profile(
                    InstanceProfileName=instance_profile_name,
                    RoleName=role['RoleName']
                )
            logger.info(f"Roles removed from instance profile {instance_profile_name}")
        except Exception as e:
            if 'NoSuchEntity' in str(e):
                logger.info(f"Instance profile {instance_profile_name} does not exist. Skipping...")
            else:
                raise


class AwsJobBase():

    logger.propagate = False

    def __init__(self, job_identifier, aws_config, boto3_session):
        self.aws_config = aws_config
        self.session = boto3_session
        self.iam_helper = AWSIAMHelper(self.session)
        self.iam = self.iam_helper.iam
        self.s3 = self.session.client('s3')
        self.job_identifier = job_identifier
        self.account = self.session.client('sts').get_caller_identity().get('Account')
        self.s3_bucket = aws_config['s3']['bucket']
        self.s3_bucket_arn = f"arn:aws:s3:::{self.s3_bucket}"
        self.s3_bucket_prefix = aws_config['s3']['prefix']
        self.region = aws_config['region']
        self.operator_email = aws_config['notifications_email']

        self.s3_results_bucket = f'{self.s3_bucket}'
        self.s3_results_bucket_arn = f"arn:aws:s3:::{self.s3_bucket}"
        self.s3_results_backup_bucket = f"{self.s3_bucket}-backups"
        self.s3_results_backup_bucket_arn = f"arn:aws:s3:::{self.s3_bucket}-backups"
        self.s3_athena_query_results_path = f"s3://aws-athena-query-results-{self.account}-{self.region}"
        self.s3_athena_query_results_arn = f"arn:aws:s3:::aws-athena-query-results-{self.account}-{self.region}"
        self.s3_lambda_code_bucket = f'nrel-{self.job_identifier}_lambda_functions'.replace('_', '-')
        self.s3_lambda_code_metadata_crawler_key = f'{self.job_identifier}/run_md_crawler.py.zip'
        self.s3_lambda_code_athena_summary_key = f'{self.job_identifier}/create_table.py.zip'

        self.lambda_metadata_crawler_function_name = f'{self.job_identifier}_start_metadata_crawler'
        self.lambda_metadata_crawler_role_name = f'{self.job_identifier}_lambda_metadata_execution_role'
        self.lambda_metadata_etl_function_name = f'{self.job_identifier}_lambda_metadata_etl'
        self.lambda_metadata_summary_crawler_function_name = f'{self.job_identifier}_lambda_metadata_summary_crawler'
        self.lambda_athena_metadata_summary_execution_role = f'{self.job_identifier}_athena_summary_execution_role'
        self.lambda_athena_function_name = f'{self.job_identifier}_athena_summary_execution'

        self.glue_metadata_crawler_name = f'{self.job_identifier}_md_crawler'
        self.glue_metadata_crawler_role_name = f'{self.job_identifier}_md_crawler_role'
        self.glue_database_name = f'{self.job_identifier}_data'
        self.glue_metadata_summary_table_name = f'{self.job_identifier}_md_summary_table'
        self.glue_metadata_etl_output_type = "csv"
        self.glue_metadata_etl_results_s3_path = f's3://{self.s3_bucket}/{self.s3_bucket_prefix}/summary-results/{self.glue_metadata_etl_output_type}/'  # noqa 501

        self.batch_compute_environment_name = f"computeenvionment_{self.job_identifier}"
        self.batch_compute_environment_ami = 'ami-0a859713f8259be72'
        self.batch_job_queue_name = f"job_queue_{self.job_identifier}"
        self.batch_service_role_name = f"batch_service_role_{self.job_identifier}"
        self.batch_instance_role_name = f"batch_instance_role_{self.job_identifier}"
        self.batch_instance_profile_name = f"batch_instance_profile_{self.job_identifier}"
        self.batch_spot_service_role_name = f"spot_fleet_role_{self.job_identifier}"
        self.batch_ecs_task_role_name = f"ecs_task_role_{self.job_identifier}"
        self.batch_task_policy_name = f"ecs_task_policy_{self.job_identifier}"
        self.batch_use_spot = aws_config['use_spot']
        self.batch_spot_bid_percent = aws_config['spot_bid_percent']

        self.firehose_role = f"{self.job_identifier}_firehose_delivery_role"
        self.firehose_name = f"{self.job_identifier}_firehose"
        self.firehost_task_policy_name = f"{self.job_identifier}_firehose_task_policy"

        self.state_machine_name = f"{self.job_identifier}_state_machine"
        self.state_machine_role_name = f"{self.job_identifier}_state_machine_role"

        self.sns_state_machine_topic = f"{self.job_identifier}_state_machine_notifications"

        self.dynamo_table_name = f"{self.job_identifier}_summary_table"
        self.dynamo_table_arn = f"arn:aws:dynamodb:{self.region}:{self.account}:table/{self.dynamo_table_name}"
        self.dynamo_task_policy_name = f"{self.job_identifier}_dynamod_db_task_policy"

        self.vpc_name = self.job_identifier

    def __repr__(self):

        return f"""
Job Identifier: {self.job_identifier}
S3 Bucket for Source Data:  {self.s3_bucket}
S3 Prefix for Source Data:  {self.s3_bucket_prefix}

A state machine {self.state_machine_name} will execute an AWS Batch job {self.job_identifier} against the source data.
Notifications of execution progress will be sent to {self.operator_email} once the email subscription is confirmed.
Summary results are transimitted via the Firehose stream {self.firehose_name} to S3 in JSON format.
Athena table {self.glue_database_name}.{self.s3_bucket_prefix} will be created from the source JSON.
The summary results will be transformed into a set of csv files placed in S3 at
{self.glue_metadata_etl_results_s3_path}.
Additionally, an Athena table {self.glue_database_name}.{self.glue_metadata_summary_table_name} will be created on the
csv output.
"""
    '''
    def get_name(self,type):

        return self.named_items[type]
    '''
    def set_name(self, type, name):
        self.named_items[type] = name

    def zip_and_s3_load(self, string_to_zip, file_name, zip_name, s3_destination_bucket, s3_destination_key):
        zip_archive = zipfile.ZipFile(zip_name, mode='w', compression=zipfile.ZIP_STORED)

        info = zipfile.ZipInfo(file_name)
        info.date_time = time.localtime()
        info.external_attr = 0o100755 << 16

        zip_archive.writestr(info, string_to_zip, zipfile.ZIP_DEFLATED)

        zip_archive.close()

        self.s3.upload_file(zip_name, s3_destination_bucket, s3_destination_key)

        logger.info(f'{zip_name} uploaded to bucket {s3_destination_bucket} under key {s3_destination_key}')
