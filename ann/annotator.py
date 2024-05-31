import base64
from http.client import HTTPException
import logging
import boto3
from boto3.dynamodb.conditions import Attr, Key
from botocore.exceptions import ClientError
from datetime import datetime, timedelta
from hashlib import sha256
import hashlib
import hmac
import json
import os
import shutil
import subprocess
import uuid
from flask import Flask, abort, redirect, render_template, request, jsonify, url_for

# Get ini configuration
from configparser import ConfigParser
config = ConfigParser(os.environ)
config.read(os.path.join(os.path.abspath(os.path.dirname(__file__)), 'ann_config.ini'))

# Setting up AWS clients and resources
aws_s3_client = boto3.client('s3')
aws_db = boto3.resource('dynamodb')
aws_db_table = aws_db.Table(config['gas']['AnnotationsDatabase'])
aws_sqs = boto3.resource('sqs')
aws_sqs_client = boto3.client('sqs')
aws_sqs_queue = aws_sqs.get_queue_by_name(
    QueueName=config['gas']['SQSJobRequest'],
)

# Setting up jobs folder
os.makedirs('jobs', exist_ok=True)

# Connect to SQS and get the message queue
# Poll the message queue in a loop
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
                    'input_file_name',
                    's3_inputs_bucket',
                    's3_key_input_file',
                    'submit_time',
                    'job_status'
                ]:
                    if data_key not in data:
                        raise Exception(f'{data_key} value is required')
                
                # Get the input file S3 object and copy it to a local file
                aws_s3_client.download_file(
                    data['s3_inputs_bucket'],
                    data['s3_key_input_file'],
                    f"{config['gas']['JobDirectory']}/{data['input_file_name']}"
                )

                # Launch annotation job as a background process
                ann_process = subprocess.Popen([
                    'python', config['gas']['RunnerFilename'],
                    f"{config['gas']['JobDirectory']}/{data['input_file_name']}"
                ])

                response = aws_db_table.update_item(
                    Key={'job_id': data.get('job_id')},
                    UpdateExpression=('set '+
                        'job_status=:js'
                    ),
                    ExpressionAttributeValues={
                        ':js': 'RUNNING'
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
                logging.error(e)
            except Exception as e:
                aws_sqs_client.delete_message(
                    QueueUrl=aws_sqs_queue.url,
                    ReceiptHandle=message['ReceiptHandle']
                )
                logging.error(e)
