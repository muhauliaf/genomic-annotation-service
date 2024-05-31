# restore.py
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
from boto3.dynamodb.conditions import Key

# Import utility helpers
sys.path.insert(1, os.path.realpath(os.path.pardir))
import helpers

# Get configuration
from configparser import ConfigParser
config = ConfigParser(os.environ)
config.read('restore_config.ini')

# Setting up AWS client and resource
aws_s3 = boto3.resource('s3')
aws_s3_client = boto3.client('s3')
aws_db = boto3.resource('dynamodb')
aws_db_table = aws_db.Table(config['gas']['AnnotationsDatabase'])
aws_glacier_client = boto3.client('glacier')
aws_glacier = boto3.resource('glacier')
aws_glacier_vault = aws_glacier.Vault(
    str(config['gas']['AccountID']),
    str(config['gas']['GlacierVaultName']),
)
aws_sns = boto3.resource('sns')
aws_sqs = boto3.resource('sqs')
aws_sqs_client = boto3.client('sqs')
aws_sqs_queue = aws_sqs.get_queue_by_name(
    QueueName=config['gas']['SQSJobRestore'],
)
aws_thaw_topic = aws_sns.create_topic(Name=config['gas']['SNSJobThawTopic'])

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
                    'user_id',
                ]:
                    if data_key not in data:
                        raise Exception(f'{data_key} value is required')
                    
                # Query annotations from user
                user_id = data.get('user_id')
                response = aws_db_table.query(
                    IndexName = 'user_id_index',
                    KeyConditionExpression = Key('user_id').eq(user_id)
                )
                annotations = response.get('Items', [])
                for annotation in annotations:

                    # Check whether file is archived by 'results_file_archive_id' and 'archive_status'
                    if 'results_file_archive_id' in annotation and annotation.get('archive_status') == 'ARCHIVED':
                        
                        try:
                            # Attempt to initiate restore job from Glacier using Expedited tier
                            # Reference: https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/glacier/client/initiate_job.html
                            response = aws_glacier_client.initiate_job(
                                vaultName=config['gas']['GlacierVaultName'],
                                jobParameters={
                                    'Type': 'archive-retrieval',
                                    'ArchiveId': annotation.get('results_file_archive_id'),
                                    'SNSTopic': aws_thaw_topic.arn,
                                    'Tier': 'Expedited'
                                }
                            )
                        except Exception as e:
                            logging.error(e)
                            
                            # If failed, initiate restore job from Glacier using Standard tier
                            # Reference: https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/glacier/client/initiate_job.html
                            response = aws_glacier_client.initiate_job(
                                vaultName=config['gas']['GlacierVaultName'],
                                jobParameters={
                                    'Type': 'archive-retrieval',
                                    'ArchiveId': annotation.get('results_file_archive_id'),
                                    'SNSTopic': aws_thaw_topic.arn,
                                    'Tier': 'Standard'
                                }
                            )

                        # Log glacier response data for future debug purpose
                        logging.debug(response)

                        # Update annotation archive_status to RESTORING
                        aws_db_table.update_item(
                            Key={'job_id': annotation.get('job_id')},
                            UpdateExpression=('set '+
                                'archive_status=:as'
                            ),
                            ExpressionAttributeValues={
                                ':as': 'RESTORING'
                            },
                            ReturnValues='ALL_NEW',
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
