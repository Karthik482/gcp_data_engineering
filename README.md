# youtube_views_dataengineering_project

This repository contains two python scripts that demonstrate how to analyze YouTube data using the YouTube Data API and Apache Beam. The two scripts are:

youtubeapi.py: This script demonstrates how to retrieve YouTube data using the YouTube Data API. The script retrieves the data which include artist name, latest album title, latest album view count for each day for the 30 days from the day album was published in youtube for a particular channel and writes the results to a pubsub topic.

beam.py: This script demonstrates how to process and analyze the YouTube data using Apache Beam. The script reads the data from a Pub/Sub subscription, processes the data to extract, group, and format the data, and finally writes the processed data to a BigQuery table.

Note: youtube api only allows primary owner of chanel to get metrics no.of likes, no.of views. You need to replace chanel id with your youtube chanel id youtubeapi.py code. 

After publishing data to pubsub topic, you need create a pubsub subscrption to that topic. Pubsub subscrption acts as a bridge between pubsub topic and dataflow. Use Gcloud CMD to create subscription or pub/sub UI in GCP Console.

Requirements
The following are the prerequisites for running the scripts:

A Google Cloud project with the YouTube Data API and BigQuery enabled.
A Pub/Sub topic and subscription.
A Python environment with the following packages installed:
apache_beam
google-auth
google-auth-oauthlib
google-auth-httplib2
google-api-python-client
Running the Scripts
To run the youtubeapi.py script:

Replace the API_KEY and CHANNEL_ID placeholders in the script with your own YouTube API key and channel ID.
Run the script using the following command:
Copy code
python youtubeapi.py
To run the beam.py script:

Replace the placeholders in the script with your own Google Cloud project ID, Pub/Sub topic name, and BigQuery table name.
Run the script using the following command:
Copy code
python beam.py
Note: You may need to run the script with administrative permissions, for example, by using sudo on a Linux system.




