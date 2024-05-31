# thaw.py
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
config.read('thaw_config.ini')

# Setting up AWS client and resource
aws_s3 = boto3.resource('s3')
aws_s3_client = boto3.client('s3')
aws_db = boto3.resource('dynamodb')
aws_db_table = aws_db.Table(config['gas']['AnnotationsDatabase'])
aws_glacier_client = boto3.client('glacier')
aws_glacier = boto3.resource('glacier')
aws_glacier_vault = aws_glacier.Vault(
    str(config['gas']['AccountID']),
    config['gas']['GlacierVaultName'],
)
aws_sns = boto3.resource('sns')
aws_sqs = boto3.resource('sqs')
aws_sqs_client = boto3.client('sqs')
aws_sqs_queue = aws_sqs.get_queue_by_name(
    QueueName=config['gas']['SQSJobThaw'],
)
aws_thaw_topic = aws_sns.create_topic(Name=config['gas']['SNSJobThawTopic'])

while True:
    # Attempt to read a message from the queue
    # Use long polling - DO NOT use sleep() to wait between polls
    response = aws_sqs_client.receive_message(
        QueueUrl=aws_sqs_queue.url,
        WaitTimeSeconds=10
    )

    # Attempt to read a message from the queue
    # Use long polling - DO NOT use sleep() to wait between polls
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
                
                # Get job output from Glacier restore job
                # Reference: https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/glacier/client/get_job_output.html
                thaw_output = aws_glacier_client.get_job_output(
                    vaultName=config['gas']['GlacierVaultName'],
                    jobId=data.get('JobId'),
                )

                # Extract annotation job ID from Glacier archiveDescription
                job_id = thaw_output.get('archiveDescription')

                # Get annotation information based on annotation job ID
                response = aws_db_table.get_item(Key = {'job_id': job_id})
                annotation = response.get('Item', None)

                # Get file binaries from job output
                # Reference: https://github.com/boto/boto3/issues/564
                buffered_reader = io.BufferedReader(thaw_output.get('body')._raw_stream)
                file_stream = io.BytesIO(buffered_reader.read())
                file_stream.seek(0)

                # Upload file binaries to s3 with original file key
                # Reference: https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/s3/client/upload_fileobj.html
                aws_s3_client.upload_fileobj(
                    file_stream,
                    annotation.get('s3_results_bucket'),
                    annotation.get('s3_key_result_file')
                )

                # Remove file from Glacier
                # Reference: https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/glacier/client/delete_archive.html
                aws_glacier_client.delete_archive(
                    vaultName=config['gas']['GlacierVaultName'],
                    archiveId=data.get('ArchiveId')
                )

                # Remove results_file_archive_id and archive_status as the file has been restored to s3
                aws_db_table.update_item(
                    Key={'job_id': job_id},
                    UpdateExpression=('remove '+
                        'results_file_archive_id,'+
                        'archive_status'
                    ),
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
