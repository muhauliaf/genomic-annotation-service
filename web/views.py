# views.py
#
# Copyright (C) 2011-2020 Vas Vasiliadis
# University of Chicago
#
# Application logic for the GAS
#
##
__author__ = 'Vas Vasiliadis <vas@uchicago.edu>'

import uuid
import time
import json
from datetime import datetime

import boto3
from boto3.dynamodb.conditions import Key
from botocore.client import Config
from botocore.exceptions import ClientError

from flask import (abort, flash, redirect, render_template,
  request, session, url_for)

from gas import app, db
from decorators import authenticated, is_premium
from auth import get_profile, update_profile


"""Start annotation request
Create the required AWS S3 policy document and render a form for
uploading an annotation input file using the policy document.

Note: You are welcome to use this code instead of your own
but you can replace the code below with your own if you prefer.
"""
@app.route('/annotate', methods=['GET'])
@authenticated
def annotate():
  # Create a session client to the S3 service
  s3 = boto3.client('s3',
    region_name=app.config['AWS_REGION_NAME'],
    config=Config(signature_version='s3v4'))

  bucket_name = app.config['AWS_S3_INPUTS_BUCKET']
  user_id = session['primary_identity']

  # Check if user is a free tier user
  is_free_user = session['role'] == 'free_user'

  # Define upload file size limit foe free user
  free_upload_limit = app.config['FREE_USER_UPLOAD_SIZE_LIMIT']

  # Generate unique ID to be used as S3 key (name)
  key_name = app.config['AWS_S3_KEY_PREFIX'] + user_id + '/' + \
    str(uuid.uuid4()) + '~${filename}'

  # Create the redirect URL
  redirect_url = str(request.url) + '/job'

  # Define policy fields/conditions
  encryption = app.config['AWS_S3_ENCRYPTION']
  acl = app.config['AWS_S3_ACL']
  fields = {
    "success_action_redirect": redirect_url,
    "x-amz-server-side-encryption": encryption,
    "acl": acl
  }
  conditions = [
    ["starts-with", "$success_action_redirect", redirect_url],
    {"x-amz-server-side-encryption": encryption},
    {"acl": acl}
  ]

  # Generate the presigned POST call
  try:
    presigned_post = s3.generate_presigned_post(
      Bucket=bucket_name, 
      Key=key_name,
      Fields=fields,
      Conditions=conditions,
      ExpiresIn=app.config['AWS_SIGNED_REQUEST_EXPIRATION'])
  except ClientError as e:
    app.logger.error(f"Unable to generate presigned URL for upload: {e}")
    return abort(500)
    
  # Render the upload form which will parse/submit the presigned POST
  return render_template('annotate.html',
    s3_post=presigned_post,
    is_free_user=is_free_user,
    free_upload_limit=free_upload_limit,
  )


"""Fires off an annotation job
Accepts the S3 redirect GET request, parses it to extract 
required info, saves a job item to the database, and then
publishes a notification for the annotator service.

Note: Update/replace the code below with your own from previous
homework assignments
"""
@app.route('/annotate/job', methods=['GET'])
@authenticated
def create_annotation_job_request():

  # Get bucket name, key, and job ID from the S3 redirect URL
  bucket_name = str(request.args.get('bucket'))
  s3_key = str(request.args.get('key'))
  user_id = session['primary_identity']

  # Extract the job ID from the S3 key
  file_name = s3_key.split('/')[-1]
  job_id = file_name.split('~')[0]
  submit_time = int(time.time())

  # Persist job to database
  data = {
    'job_id': job_id,
    'user_id': user_id,
    'input_file_name': file_name,
    's3_inputs_bucket': bucket_name,
    's3_key_input_file': s3_key,
    'submit_time': submit_time,
    'job_status': 'PENDING'
  }
  dynamodb = boto3.resource('dynamodb',
    region_name=app.config['AWS_REGION_NAME'])
  dynamodb_table = dynamodb.Table(app.config['AWS_DYNAMODB_ANNOTATIONS_TABLE'])
  dynamodb_table.put_item(
    Item = data,
    ConditionExpression='attribute_not_exists(job_id) OR NOT job_status IN (:jsr, :jsc)',
    ExpressionAttributeValues={
      ':jsr': 'RUNNING',
      ':jsc': 'COMPLETED',
    },
  )

  # Send message to request queue
  sns = boto3.resource('sns',
    region_name=app.config['AWS_REGION_NAME'])
  topic = sns.create_topic(Name=app.config['AWS_SNS_JOB_REQUEST_TOPIC'])
  topic_arn = topic.arn
  topic.publish(Message=json.dumps(data))

  # Render the template based on parameters above
  return render_template('annotate_confirm.html', job_id=job_id)


"""List all annotations for the user
"""
@app.route('/annotations', methods=['GET'])
@authenticated
def annotations_list():
  # Get list of annotations to display
  user_id = session['primary_identity']
  dynamodb = boto3.resource('dynamodb',
    region_name=app.config['AWS_REGION_NAME'])
  dynamodb_table = dynamodb.Table(app.config['AWS_DYNAMODB_ANNOTATIONS_TABLE'])

  # Query the DynamoDB table based on user ID 
  response = dynamodb_table.query(
      IndexName = 'user_id_index',
      KeyConditionExpression = Key('user_id').eq(user_id)
  )
  annotations = response.get('Items', [])

  # Format the submit_time in a date and time format
  # Reference: https://stackoverflow.com/questions/60133202/how-to-format-time-time-using-strftime-in-python
  for annotation in annotations:
    annotation['submit_time'] = time.strftime("%Y-%m-%d %H:%M:%S", time.gmtime(annotation['submit_time']))
  
  # Render the template based on parameters above
  return render_template('annotations.html', annotations=annotations)


"""Display details of a specific annotation job
"""
@app.route('/annotations/<id>', methods=['GET'])
@authenticated
def annotation_details(id):
  # Load AWS clients and resources
  s3 = boto3.client('s3',
  region_name=app.config['AWS_REGION_NAME'],
  config=Config(signature_version='s3v4'))
  dynamodb = boto3.resource('dynamodb',
    region_name=app.config['AWS_REGION_NAME'])
  dynamodb_table = dynamodb.Table(app.config['AWS_DYNAMODB_ANNOTATIONS_TABLE'])

  # Query DynamoDB table to get an item based on Job ID 
  response = dynamodb_table.get_item(Key = {'job_id': id})
  annotation = response.get('Item', None)

  # Check if user in Job ID is same as user in session
  # If noed, redirect to 'Not authorized' page
  user_id = session['primary_identity']
  if user_id != annotation['user_id']:
    return render_template('error.html',
      title='Not authorized', alert_level='danger',
      message="You are not authorized to view this annotation. \
        Only the file uploader can view this annotation."
      ), 405

  # Format the submit_time in a date and time format
  # Reference: https://stackoverflow.com/questions/60133202/how-to-format-time-time-using-strftime-in-python
  annotation['submit_time'] = time.strftime("%Y-%m-%d %H:%M:%S", time.gmtime(annotation['submit_time']))

  # Format the input_file_name to just initial format 
  annotation['input_file_name'] = (annotation['input_file_name'].split('/')[-1]).split('~')[-1]

  # Generate the presigned URL for input file download
  # Reference: https://boto3.amazonaws.com/v1/documentation/api/latest/guide/s3-presigned-urls.html
  try:
    input_file_url = s3.generate_presigned_url(
      'get_object',
      Params={
        'Bucket': annotation['s3_inputs_bucket'],
        'Key': annotation['s3_key_input_file']},
      ExpiresIn=app.config['AWS_SIGNED_REQUEST_EXPIRATION'])
    annotation['input_file_url'] = input_file_url
  except ClientError as e:
    app.logger.error(f"Unable to generate presigned URL for result file: {e}")


  free_access_expired = False
  if annotation['job_status'] == "COMPLETED":
    # Format the complete_time in a date and time format
    annotation['complete_time'] = time.strftime("%Y-%m-%d %H:%M:%S", time.gmtime(annotation['complete_time']))
    # Check if file is not available by checking 'results_file_archive_id' attribute
    if 'results_file_archive_id' in annotation:
      # Check if file is on restoring process
      if 'archive_status' in annotation and annotation['archive_status'] == 'RESTORING':
        annotation['restore_message'] = 'Restoring file..'
      else:
        free_access_expired = True
    else:    
      # If file is available, generate the presigned URL for result file
      try:
        result_file_url = s3.generate_presigned_url(
          'get_object',
          Params={
            'Bucket': annotation['s3_results_bucket'],
            'Key': annotation['s3_key_result_file']},
          ExpiresIn=app.config['AWS_SIGNED_REQUEST_EXPIRATION'])
        annotation['result_file_url'] = result_file_url
      except ClientError as e:
        app.logger.error(f"Unable to generate presigned URL for result file: {e}")

  # Render the template based on parameters above
  return render_template('annotation_details.html',
    annotation=annotation,
    free_access_expired=free_access_expired
  )


"""Display the log file contents for an annotation job
"""
@app.route('/annotations/<id>/log', methods=['GET'])
@authenticated
def annotation_log(id):
  # Load AWS clients and resources
  dynamodb = boto3.resource('dynamodb',
    region_name=app.config['AWS_REGION_NAME'])
  dynamodb_table = dynamodb.Table(app.config['AWS_DYNAMODB_ANNOTATIONS_TABLE'])

  # Query DynamoDB table to get an item based on Job ID 
  response = dynamodb_table.get_item(Key = {'job_id': id})
  annotation = response.get('Item', None)

  # Check if user in Job ID is same as user in session
  # If noed, redirect to 'Not authorized' page
  user_id = session['primary_identity']
  if user_id != annotation['user_id']:
    return render_template('error.html',
      title='Not authorized', alert_level='danger',
      message="You are not authorized to view this annotation. \
        Only the file uploader can view this annotation."
      ), 405
  
  # Load AWS S3 client
  s3 = boto3.client('s3',
    region_name=app.config['AWS_REGION_NAME'],
    config=Config(signature_version='s3v4'))
  objects = s3.list_objects_v2(
    Bucket=app.config['AWS_S3_RESULTS_BUCKET'],
    Prefix=app.config['AWS_S3_KEY_PREFIX']
  )

  # Get Log file key
  # Reference: https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/s3/client/list_objects_v2.html
  log_s3_key = [
    obj['Key']
    for obj in objects['Contents']
    if id in obj['Key']
    and obj['Key'].endswith('.log')
  ][0]

  # Get Log file content
  # Reference: https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/s3/client/get_object.html
  log_file_contents = s3.get_object(
    Bucket=app.config['AWS_S3_RESULTS_BUCKET'],
    Key=log_s3_key
  ).get('Body').read().decode()

  # Render the template based on parameters above
  return render_template('view_log.html', job_id=id, log_file_contents=log_file_contents)


"""Subscription management handler
"""
@app.route('/subscribe', methods=['GET', 'POST'])
@authenticated
def subscribe():
  if (request.method == 'GET'):
    # Display form to get subscriber credit card info
    if (session.get('role') == "free_user"):
      return render_template('subscribe.html')
    else:
      return redirect(url_for('profile'))

  elif (request.method == 'POST'):
    # Update user role to allow access to paid features
    update_profile(
      identity_id=session['primary_identity'],
      role="premium_user"
    )

    # Update role in the session
    session['role'] = "premium_user"

    # Request restoration of the user's data from Glacier
    # Add code here to initiate restoration of archived user data
    # Make sure you handle files not yet archived!
    sns = boto3.resource('sns',
      region_name=app.config['AWS_REGION_NAME'])
    restore_data = {
      'user_id': session['primary_identity']
    }
    restore_topic = sns.create_topic(Name=app.config['AWS_SNS_JOB_RESTORE_TOPIC'])
    restore_topic.publish(Message=json.dumps(restore_data))

    # Display confirmation page
    return render_template('subscribe_confirm.html') 

"""Reset subscription
"""
@app.route('/unsubscribe', methods=['GET'])
@authenticated
def unsubscribe():
  # Hacky way to reset the user's role to a free user; simplifies testing
  update_profile(
    identity_id=session['primary_identity'],
    role="free_user"
  )
  return redirect(url_for('profile'))


"""DO NOT CHANGE CODE BELOW THIS LINE
*******************************************************************************
"""

"""Home page
"""
@app.route('/', methods=['GET'])
def home():
  return render_template('home.html')

"""Login page; send user to Globus Auth
"""
@app.route('/login', methods=['GET'])
def login():
  app.logger.info(f"Login attempted from IP {request.remote_addr}")
  # If user requested a specific page, save it session for redirect after auth
  if (request.args.get('next')):
    session['next'] = request.args.get('next')
  return redirect(url_for('authcallback'))

"""404 error handler
"""
@app.errorhandler(404)
def page_not_found(e):
  return render_template('error.html', 
    title='Page not found', alert_level='warning',
    message="The page you tried to reach does not exist. \
      Please check the URL and try again."
    ), 404

"""403 error handler
"""
@app.errorhandler(403)
def forbidden(e):
  return render_template('error.html',
    title='Not authorized', alert_level='danger',
    message="You are not authorized to access this page. \
      If you think you deserve to be granted access, please contact the \
      supreme leader of the mutating genome revolutionary party."
    ), 403

"""405 error handler
"""
@app.errorhandler(405)
def not_allowed(e):
  return render_template('error.html',
    title='Not allowed', alert_level='warning',
    message="You attempted an operation that's not allowed; \
      get your act together, hacker!"
    ), 405

"""500 error handler
"""
@app.errorhandler(500)
def internal_error(error):
  return render_template('error.html',
    title='Server error', alert_level='danger',
    message="The server encountered an error and could \
      not process your request."
      + f"Error: {error}"
    ), 500

### EOF
