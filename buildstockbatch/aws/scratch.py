import boto3

session = boto3.Session(region_name='us-west-2', profile_name='nrel-aws-dev')

iam = session.client('iam')

response = iam.get_instance_profile(
    InstanceProfileName='ragertest1_emr_instance_profile'
)

from pprint import pprint
pprint(response)

response = iam.add_role_to_instance_profile(
                InstanceProfileName='ragertest1_emr_instance_profile',
                RoleName='ragertest1_emr_job_flow_role'
            )