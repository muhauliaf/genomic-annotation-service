# Final Project
Muhammad Aulia Firmansyah
mauliafirmansyah@uchicago.edu

# gas-framework
An enhanced web framework (based on [Flask](http://flask.pocoo.org/)) for use in the capstone project. Adds robust user authentication (via [Globus Auth](https://docs.globus.org/api/auth)), modular templates, and some simple styling based on [Bootstrap](http://getbootstrap.com/).

Directory contents are as follows:
* `/web` - The GAS web app files
* `/ann` - Annotator files
* `/util` - Utility scripts for notifications, archival, and restoration
* `/aws` - AWS user data files

# Glacier related process
Archive process:
* After the annotator run the annotation request, it checks the user role. If the user is a free user, publish an archive request to an archive SNS topic.
* The archive SQS receive the message, then check if it has reach past the grace period. If it has, check user role. If it is a premium user, skip the process and remove the message. If it is still a free user, begin archiving process
* After the process is done, save the archive ID and add ARCHIVED archive status to the database

Restore process:
* After the user upgrades to premium, publish a restore request to a restore SNS topic.
* The restore SQS receive the message, then initiate restoring process and attached thaw SNS to the process.
* Update archive status to RESTORING to the database

Thaw process:
* When restoring process is finished, an thaw SNS topic is published.
* The thaw SQS receive the message, then initiate thawing process.
* Remove archive ID and archive status from the database

# Optional: Free user file upload size check
When submitting the upload file POST, a javascript script is executed to check whether the user is premium or free. If the user is a free user, check the file size. If it's larger than the file limit, cancel the file upload and alert the user.

# Optional: Free user file upload size check
When submitting the upload file POST, a javascript script is executed to check whether the user is premium or free. If the user is a free user, check the file size. If it's larger than the file limit, cancel the file upload and alert the user.

# Optional: Test under load using Locust.
I run the locust test twice, because in the first run (23:30 to 00:00), the web downscaling alarm didn't trigger. In the second run (00:15 to 07.00), the locust was run using 100 users @ 5 users/second. These are several observations I found during the run:
* The web upscale alarm was triggered as the request count reached 3000/minute, which is expected. Although, there was fluctuation where the requests dropped to around 2700.
* Interestingly, the response time was only >= 0.01 for around 30 minutes (00:00 to 00:30). After that period, the response time remained < 0.01, thus trigger the web downscale alarm.
* Because web downscale alarm wasn't triggered from 00:00 to 00:30, the instances scaled up. After that, the web downscale alarm was triggered, prompting a small downscale. Although, after that, the instances keep scaling up to its maximum instances of 10 at 01:15.
* After that period, the number of instances keep fluctuating between 9 and 10 every 5 minutes. This is possibly because both upscale and downscale alarm was triggered at the same time.
* Finally, after the locust was stopped, the number of instances stated to go down to 2 in about 30-40 minutes. This is because the upscale alarm stopped while the downscale alarm continued.
* Another fascinating observation is the statistics from the locust itself. In the beginning of the run, the requests encountered 10 failures/second (2%). After the instances scaled up to 10, failures still fluctuated but at lower rate of 3-5 failures/second (<=1%). This means increasing number of instances actually improved the performance.
* Rather than the average time in CloudWatch, the response time graph in Locust used 50th and 95th percentile, which captured the occasional high response times of 10s. This is probably a timeout, though.

# Optional: Load test the annotator farm.
I created a file named 'ann_load.py' which was used to simulate load test request messages to annotator. It sends dummy loads at defined interval to the request SNS. The purpose is to trigger the upscale alarm from the CloudWatch monitoring.
These are my scenario for the test:
* I increased the messages rate to 0.5-1 message per second, rising the messages rate shown in the CloudWatch sharply, reaching 300 messages/10 minutes.
* After that, I tried to set my messages rate so that the number is lower than 50 but higher than 5.
* Finally, I stopped the load test, lowering the number to 0.

These are several observations I found during the run:
* The behavior of the instances reflected the scenario consistently, rising to 8, staying there, then going down to 2.
* Interestingly, the CloudWatch alarm didn't trigger until 10 minutes after the increase, possibly because the alarm collected data in 10 minutes time frame.