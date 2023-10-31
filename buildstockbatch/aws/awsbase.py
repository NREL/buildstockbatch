import logging


logger = logging.getLogger(__name__)


class AWSIAMHelper:
    logger.propagate = False

    def __init__(self, session):
        """
        Initialize the AWSIAM class with a boto3 Session
        :param session: boto3 Session from 'parent' job base class
        """
        self.session = session
        self.iam = self.session.client("iam")

    def role_stitcher(
        self,
        role_name,
        trust_service,
        description,
        policies_list=[],
        managed_policie_arns=[],
    ):
        """
        Creates a role and attached the policies - will catch errors and skip if role already exists
        :param role_name: Name of service role to create
        :param trust_service: Trusted service to associate with the service role
        :param description: Description of role
        :param policies_list: List of JSON policies (optional)
        :param managed_policie_arns: Managed policies to attach (optional)
        :return: Role ARN is returned
        """
        role_arn = None
        trust_policy = f"""{{
                        "Version": "2012-10-17",
                        "Statement": [{{
                            "Effect": "Allow",
                            "Principal": {{
                                "Service": "{trust_service}.amazonaws.com"
                            }},
                            "Action": "sts:AssumeRole"
                        }}]
                    }}
                """

        try:
            response = self.iam.create_role(
                Path="/",
                RoleName=role_name,
                AssumeRolePolicyDocument=trust_policy,
                Description=description,
            )
            role_arn = response["Role"]["Arn"]

            p_counter = 1
            for policy in policies_list:
                response = self.iam.put_role_policy(
                    RoleName=role_name,
                    PolicyName=f"{role_name}_policy_{p_counter}",
                    PolicyDocument=policy,
                )
                p_counter = p_counter + 1

            for managed_policy_arn in managed_policie_arns:
                response = self.iam.attach_role_policy(PolicyArn=managed_policy_arn, RoleName=role_name)

            logger.info(f"Role {role_name} created")

            return role_arn

        except Exception as e:
            if "EntityAlreadyExists" in str(e):
                logger.info(f"Role {role_name} not created - already exists")
                response = self.iam.get_role(RoleName=role_name)
                role_arn = response["Role"]["Arn"]
                return role_arn

            else:
                raise

    def delete_role(self, role_name):
        """
        Delete a role
        :param role_name: name of the role to delete
        :return: None
        """
        try:
            response = self.iam.list_role_policies(RoleName=role_name)

            for policy in response["PolicyNames"]:
                self.iam.delete_role_policy(RoleName=role_name, PolicyName=policy)

            response = self.iam.list_attached_role_policies(RoleName=role_name)

            for policy in response["AttachedPolicies"]:
                self.iam.detach_role_policy(RoleName=role_name, PolicyArn=policy["PolicyArn"])

            logger.info(f"Policies detached from role {role_name}.")

            response = self.iam.delete_role(RoleName=role_name)
            logger.info(f"Role {role_name} deleted.")
        except Exception as e:
            if "NoSuchEntity" in str(e):
                logger.info(f"Role {role_name} missing, skipping...")
            else:
                raise

    def delete_instance_profile(self, instance_profile_name):
        try:
            self.iam.delete_instance_profile(InstanceProfileName=instance_profile_name)
            logger.info(f"Instance profile {instance_profile_name} deleted.")
        except Exception as e:
            if "NoSuchEntity" in str(e):
                logger.info(f"Instance profile {instance_profile_name} missing, skipping...")
            else:
                raise

    def remove_role_from_instance_profile(self, instance_profile_name):
        try:
            response = self.iam.get_instance_profile(InstanceProfileName=instance_profile_name)

            for role in response["InstanceProfile"]["Roles"]:
                response = self.iam.remove_role_from_instance_profile(
                    InstanceProfileName=instance_profile_name, RoleName=role["RoleName"]
                )
            logger.info(f"Roles removed from instance profile {instance_profile_name}")
        except Exception as e:
            if "NoSuchEntity" in str(e):
                logger.info(f"Instance profile {instance_profile_name} does not exist. Skipping...")
            else:
                raise


class AwsJobBase:
    logger.propagate = False

    def __init__(self, job_identifier, aws_config, boto3_session):
        self.aws_config = aws_config
        self.session = boto3_session
        self.iam_helper = AWSIAMHelper(self.session)
        self.iam = self.iam_helper.iam
        self.s3 = self.session.client("s3")
        self.job_identifier = job_identifier
        self.account = self.session.client("sts").get_caller_identity().get("Account")
        self.region = aws_config["region"]
        self.operator_email = aws_config["notifications_email"]

        # S3
        self.s3_bucket = aws_config["s3"]["bucket"]
        self.s3_bucket_arn = f"arn:aws:s3:::{self.s3_bucket}"
        self.s3_bucket_prefix = aws_config["s3"]["prefix"].rstrip("/")
        self.s3_lambda_code_emr_cluster_key = f"{self.s3_bucket_prefix}/lambda_functions/emr_function.py.zip"
        self.s3_lambda_emr_config_key = f"{self.s3_bucket_prefix}/lambda_functions/emr_config.json"
        self.s3_emr_folder_name = "emr"

        # EMR
        emr_config = aws_config.get("emr", {})
        self.emr_manager_instance_type = emr_config.get("manager_instance_type", "m5.4xlarge")
        self.emr_worker_instance_type = emr_config.get("worker_instance_type", "r5.4xlarge")
        self.emr_worker_instance_count = emr_config.get("worker_instance_count", 4)
        self.emr_cluster_security_group_name = f"{self.job_identifier}_emr_security_group"
        self.emr_cluster_name = f"{self.job_identifier}_emr_dask_cluster"
        self.emr_job_flow_role_name = f"{self.job_identifier}_emr_job_flow_role"
        self.emr_job_flow_role_arn = ""
        self.emr_service_role_name = f"{self.job_identifier}_emr_service_role"
        self.emr_service_role_arn = ""
        self.emr_cluster_security_group_id = ""
        self.emr_log_uri = f"s3://{self.s3_bucket}/{self.s3_bucket_prefix}/emrlogs/"
        self.emr_instance_profile_name = f"{self.job_identifier}_emr_instance_profile"

        # Lambda
        self.lambda_emr_job_step_execution_role = f"{self.job_identifier}_emr_job_step_execution_role"
        self.lambda_emr_job_step_function_name = f"{self.job_identifier}_emr_job_step_submission"
        self.lambda_emr_job_step_execution_role_arn = ""

        # Batch
        self.batch_compute_environment_name = f"computeenvionment_{self.job_identifier}"
        self.batch_compute_environment_ami = "ami-0184013939261b626"
        self.batch_job_queue_name = f"job_queue_{self.job_identifier}"
        self.batch_service_role_name = f"batch_service_role_{self.job_identifier}"
        self.batch_instance_role_name = f"batch_instance_role_{self.job_identifier}"
        self.batch_instance_profile_name = f"batch_instance_profile_{self.job_identifier}"
        self.batch_spot_service_role_name = f"spot_fleet_role_{self.job_identifier}"
        self.batch_ecs_task_role_name = f"ecs_task_role_{self.job_identifier}"
        self.batch_task_policy_name = f"ecs_task_policy_{self.job_identifier}"
        self.batch_use_spot = aws_config.get("use_spot", True)
        self.batch_spot_bid_percent = aws_config.get("spot_bid_percent", 100)

        # Step Functions
        self.state_machine_name = f"{self.job_identifier}_state_machine"
        self.state_machine_role_name = f"{self.job_identifier}_state_machine_role"

        # SNS
        self.sns_state_machine_topic = f"{self.job_identifier}_state_machine_notifications"

        # VPC
        self.vpc_name = self.job_identifier
        self.vpc_id = ""  # will be available after VPC creation
        self.priv_subnet_cidr_1 = ""  # will be available after VPC creation
        self.priv_vpc_subnet_id_1 = "REPL"  # will be available after VPC creation
        self.priv_vpc_subnet_id_2 = "REPL"  # will be available after VPC creation

    def __repr__(self):
        return f"""
Job Identifier: {self.job_identifier}
S3 Bucket for Source Data:  {self.s3_bucket}
S3 Prefix for Source Data:  {self.s3_bucket_prefix}

A state machine {self.state_machine_name} will execute an AWS Batch job {self.job_identifier} against the source data.
Notifications of execution progress will be sent to {self.operator_email} once the email subscription is confirmed.
Once processing is complete the
state machine will then launch an EMR cluster with a job to combine the results and create an AWS Glue table.
"""
