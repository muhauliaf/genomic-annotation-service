# run.py
#
# Copyright (C) 2011-2019 Vas Vasiliadis
# University of Chicago
#
# Wrapper script for running AnnTools
#
##
__author__ = 'Vas Vasiliadis <vas@uchicago.edu>'

import boto3
from botocore.exceptions import ClientError
import os
import json
import sys
sys.path.append('../util/')
import time
import driver
import file_utils as fu
import logging

import helpers

# Get ini configuration
from configparser import ConfigParser
config = ConfigParser(os.environ)
config.read(os.path.join(os.path.abspath(os.path.dirname(__file__)), 'ann_config.ini'))

"""A rudimentary timer for coarse-grained profiling
"""
class Timer(object):
    def __init__(self, verbose=True):
        self.verbose = verbose

    def __enter__(self):
        self.start = time.time()
        return self

    def __exit__(self, *args):
        self.end = time.time()
        self.secs = self.end - self.start
        if self.verbose:
            logging.debug(f"Approximate runtime: {self.secs:.2f} seconds")

if __name__ == '__main__':
    # Call the AnnTools pipeline
    if len(sys.argv) > 1:
        input_path_name = sys.argv[1]
        input_file_name = input_path_name.split('/')[-1]
        with Timer():
            driver.run(input_path_name, 'vcf')
        complete_time = int(time.time())

        # Add code to save results and log files to S3 results bucket
        job_id = input_file_name.split('~')[0]

        # Load AWS clients and resources
        aws_s3_client = boto3.client('s3')
        aws_db = boto3.resource('dynamodb')
        aws_db_table = aws_db.Table(config['gas']['AnnotationsDatabase'])
        aws_sns = boto3.resource('sns', region_name=config['aws']['AwsRegionName'])

        try:
            # Query job information from DynamoDB
            db_response = aws_db_table.get_item(Key = {'job_id': job_id})
            data = db_response['Item']
            data['submit_time'] = int(data['submit_time'])

            # Upload the results file
            result_path_name = (f'{input_path_name}.annot').replace('.vcf.annot', '.annot.vcf')
            result_file_name = result_path_name.split('/')[-1]
            result_file_key = f"{config['gas']['CNetID']}/{data['user_id']}/{result_file_name}"
            response = aws_s3_client.upload_file(
                result_path_name,
                config['gas']['AwsResultBucketName'],
                result_file_key
            )

            # Upload the log file
            log_path_name = f'{input_path_name}.count.log'
            log_file_name = log_path_name.split('/')[-1]
            log_file_key = f"{config['gas']['CNetID']}/{data['user_id']}/{log_file_name}"
            response = aws_s3_client.upload_file(
                log_path_name,
                config['gas']['AwsResultBucketName'],
                log_file_key
            )

            # Update job data with completion information
            data.update({
                'job_status': 'COMPLETED',
                's3_results_bucket': config['gas']['AwsResultBucketName'],
                's3_key_result_file': result_file_key,
                's3_key_log_file': log_file_key,
                'complete_time': complete_time
            })
            aws_db_table.put_item(Item=data)

            # Update job completion information to DynamoDB
            response = aws_db_table.update_item(
                Key={'job_id': job_id},
                UpdateExpression=('set '+
                    'job_status=:js,'+
                    's3_results_bucket=:srb,'+
                    's3_key_result_file=:skrf,'+
                    's3_key_log_file=:sklf,'+
                    'complete_time=:ct'
                ),
                ExpressionAttributeValues={
                    ':js': 'COMPLETED',
                    ':srb': config['gas']['AwsResultBucketName'],
                    ':skrf': result_file_key,
                    ':sklf': log_file_key,
                    ':ct': complete_time
                },
                ReturnValues='ALL_NEW',
            )

            # Log data for future debug purpose
            logging.debug(json.dumps(data))

            # Check user status
            user_id = data.get('user_id')
            user_data = helpers.get_user_profile(user_id)

            # If user is a free user, publish archiving topic to SNS  
            if user_data.get('role') == 'free_user':
                archive_data = {
                    'job_id': job_id,
                    'user_id': user_id,
                    's3_results_bucket': config['gas']['AwsResultBucketName'],
                    's3_result_key_file': result_file_key,
                    'complete_time': complete_time,
                }
                aws_archive_topic = aws_sns.create_topic(Name=config['gas']['SNSJobArchiveTopic'])
                aws_archive_topic.publish(Message=json.dumps(archive_data))

            # Publish result topic to SNS, used for email handler
            email_data = {
                'job_id': job_id,
                'user_name': user_data.get('name'),
                'user_email': user_data.get('email'),
                'user_role': user_data.get('role'),
            }
            aws_result_topic = aws_sns.create_topic(Name=config['gas']['SNSJobCompleteTopic'])
            aws_result_topic.publish(Message=json.dumps(email_data))

            # Clean up (delete) local job files
            fu.delete(input_path_name)
            fu.delete(result_path_name)
            fu.delete(log_path_name)
        
        # If there are ClientError, the error might involve AWS settings
        # Otherwise, it's probably because of invalid data
        # In any case, the process should stop to prevent anomalies
        except ClientError as e:
            logging.error(e.response["Error"]["Message"])
        except Exception as e:
            logging.error(e)
    else:
        logging.error("A valid .vcf file must be provided as input to this program.")

### EOF