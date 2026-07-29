import boto3
import os
import sys
import time

def deploy():
    session = boto3.Session(
        aws_access_key_id=os.environ.get('AWS_ACCESS_KEY_ID'),
        aws_secret_access_key=os.environ.get('AWS_SECRET_ACCESS_KEY'),
        region_name='us-east-1'
    )
    
    ec2 = session.client('ec2')
    ec2_resource = session.resource('ec2')
    
    # 1. Create Security Group
    print("Creating Security Group...")
    try:
        sg = ec2.create_security_group(GroupName='kafka-sg', Description='Kafka Security Group')
        sg_id = sg['GroupId']
        print(f"Created Security Group {sg_id}")
    except Exception as e:
        if 'InvalidGroup.Duplicate' in str(e):
            print("Security Group 'kafka-sg' already exists.")
            response = ec2.describe_security_groups(GroupNames=['kafka-sg'])
            sg_id = response['SecurityGroups'][0]['GroupId']
        else:
            raise e
            
    # 2. Authorize Ports
    try:
        print("Authorizing ports 22 and 9092...")
        ec2.authorize_security_group_ingress(
            GroupId=sg_id,
            IpPermissions=[
                {'IpProtocol': 'tcp', 'FromPort': 22, 'ToPort': 22, 'IpRanges': [{'CidrIp': '0.0.0.0/0'}]},
                {'IpProtocol': 'tcp', 'FromPort': 9092, 'ToPort': 9092, 'IpRanges': [{'CidrIp': '0.0.0.0/0'}]}
            ]
        )
        print("Ports authorized.")
    except Exception as e:
        if 'InvalidPermission.Duplicate' in str(e):
            print("Ports already authorized.")
        else:
            raise e
            
    # 3. Create Key Pair
    try:
        print("Creating Key Pair...")
        key_pair = ec2.create_key_pair(KeyName='kafka-aws-key')
        with open('kafka-aws-key.pem', 'w') as f:
            f.write(key_pair['KeyMaterial'])
        print("Saved kafka-aws-key.pem")
    except Exception as e:
        if 'InvalidKeyPair.Duplicate' in str(e):
            print("Key Pair 'kafka-aws-key' already exists. We will reuse it, but hope you still have the .pem file!")
        else:
            raise e
            
    # 4. Find Ubuntu 22.04 AMI
    print("Finding Ubuntu 22.04 AMI...")
    response = ec2.describe_images(
        Filters=[
            {'Name': 'name', 'Values': ['ubuntu/images/hvm-ssd/ubuntu-jammy-22.04-amd64-server-*']},
            {'Name': 'state', 'Values': ['available']}
        ],
        Owners=['099720109477'],
    )
    images = sorted(response['Images'], key=lambda k: k['CreationDate'], reverse=True)
    ami_id = images[0]['ImageId']
    print(f"Using AMI: {ami_id}")
    
    # 5. Run Instance
    print("Launching Instance...")
    instances = ec2_resource.create_instances(
        ImageId=ami_id,
        MinCount=1,
        MaxCount=1,
        InstanceType='t3.micro',
        KeyName='kafka-aws-key',
        SecurityGroupIds=[sg_id],
        TagSpecifications=[
            {
                'ResourceType': 'instance',
                'Tags': [{'Key': 'Name', 'Value': 'kafka-broker'}]
            }
        ]
    )
    
    instance = instances[0]
    print(f"Waiting for instance {instance.id} to be running...")
    instance.wait_until_running()
    instance.reload()
    
    print(f"Instance is running! Public IP: {instance.public_ip_address}")

if __name__ == "__main__":
    deploy()
