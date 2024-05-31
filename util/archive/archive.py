# archive.py
#
# NOTE: This file lives on the Utils instance
#
# Copyright (C) 2011-2019 Vas Vasiliadis
# University of Chicago
##
__author__ = 'Vas Vasiliadis <vas@uchicago.edu>'

import os
import sys
import io
import time
import logging
import json
import boto3
from botocore.exceptions import ClientError

# Import utility helpers
sys.path.insert(1, os.path.realpath(os.path.pardir))
import helpers

# Get configuration
from configparser import ConfigParser
config = ConfigParser(os.environ)
config.read('archive_config.ini')

# Setting up AWS client and resource
aws_s3 = boto3.resource('s3')
aws_s3_client = boto3.client('s3')
aws_db = boto3.resource('dynamodb')
aws_db_table = aws_db.Table(config['gas']['AnnotationsDatabase'])
aws_glacier = boto3.resource('glacier')
aws_glacier_vault = aws_glacier.Vault(
    str(config['gas']['AccountID']),
    str(config['gas']['GlacierVaultName']),
)
aws_sqs = boto3.resource('sqs')
aws_sqs_client = boto3.client('sqs')
aws_sqs_queue = aws_sqs.get_queue_by_name(
    QueueName=config['gas']['SQSJobArchive'],
)

while True:
    # Attempt to read a message from the queue
    # Use long polling - DO NOT use sleep() to wait between polls
    response = aws_sqs_client.receive_message(
        QueueUrl=aws_sqs_queue.url,
        WaitTimeSeconds=10
    )

    # If message read, extract job parameters from the message body as before
    if 'Messages' in response:
        for message in response['Messages']:
            try:
                # Extract data from SQS message
                message_body = json.loads(message['Body'])
                data = json.loads(message_body['Message'])

                # Log data for future debug purpose
                logging.debug(data)

                # If data is invalid because of missing body or attributes
                # Raise error for message deletion
                if not data:
                    raise Exception('No JSON data provided')
                for data_key in [
                    'job_id',
                    'user_id',
                    's3_results_bucket',
                    's3_result_key_file',
                    'complete_time',
                ]:
                    if data_key not in data:
                        raise Exception(f'{data_key} value is required')
                    
                # Compare job completion time and current time
                complete_time = int(data.get('complete_time'))
                current_time = int(time.time())

                # Check whether it has passed the free user grace period
                if current_time - complete_time > int(config['gas']['FreeGracePeriod']):
                    user_id = data.get('user_id')
                    user_data = helpers.get_user_profile(user_id)

                    # Check whether the user is still a free user after the grace period
                    # If the user is already a premium user, skip this process
                    # If the user is still a free user, begin archiving process
                    if user_data.get('role') == 'free_user':

                        # 'Move' file from S3 to Glacier by downloading the file from S
                        # and then uploading it to Glacier
                        # Reference: https://stackoverflow.com/questions/71439361/archiving-s3-data-to-glacier
                        file_stream = io.BytesIO()
                        aws_s3_client.download_fileobj(
                            data.get('s3_results_bucket'),
                            data.get('s3_result_key_file'),
                            file_stream
                        )
                        file_stream.seek(0)

                        # Set annotation job ID as archiveDescription
                        archive = aws_glacier_vault.upload_archive(
                            archiveDescription=data.get('job_id'),
                            body=file_stream
                        )

                        # Update DynamoDB table to add archive ID and archive status as ARCHIVED
                        aws_db_table.update_item(
                            Key={'job_id': data.get('job_id')},
                            UpdateExpression=('set '+
                                'results_file_archive_id=:rfaid,'+
                                'archive_status=:as'
                            ),
                            ExpressionAttributeValues={
                                ':rfaid': archive.id,
                                ':as': 'ARCHIVED'
                            },
                            ReturnValues='ALL_NEW',
                        )

                        # Remove file from S3 bucket
                        aws_s3_client.delete_object(
                            Bucket=data.get('s3_results_bucket'),
                            Key=data.get('s3_result_key_file')
                        )
                    # Delete the message from the queue, if job was successfully processed
                    aws_sqs_client.delete_message(
                        QueueUrl=aws_sqs_queue.url,
                        ReceiptHandle=message['ReceiptHandle']
                    )
            # If there are ClientError, the error might involve AWS settings, so don't remove the SQS message
            # Otherwise, it's because invalid data. Therefore, it can be removed
            except ClientError as e:
                logging.error(e)
            except Exception as e:
                aws_sqs_client.delete_message(
                    QueueUrl=aws_sqs_queue.url,
                    ReceiptHandle=message['ReceiptHandle']
                )
                logging.error(e)
