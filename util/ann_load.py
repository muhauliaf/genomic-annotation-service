import json
import time
import boto3
from botocore.exceptions import ClientError
import os

"""
Annotator Load tester
This file is used to load test request messages to annotator.
Itu sends a dummy load which is then monitored by CloudWatch  
"""

# Get ini configuration
from configparser import ConfigParser
config = ConfigParser(os.environ)
config.read(os.path.join(os.path.abspath(os.path.dirname(__file__)), 'util_config.ini'))

# Load AWS clients and resources 
aws_sns = boto3.resource('sns', region_name=config['aws']['AwsRegionName'])

if __name__ == '__main__':
    # Create infinite loop with a specified delay time to send dummy request message to annotator
    loop_period = 15
    while True:
        aws_result_topic = aws_sns.create_topic(Name=config['gas']['SNSJobRequestTopic'])
        response = aws_result_topic.publish(Message=json.dumps({}))
        print(response)
        time.sleep(loop_period)
