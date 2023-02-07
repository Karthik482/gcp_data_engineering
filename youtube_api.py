import json
import requests
from datetime import datetime, timedelta
import google.oauth2.credentials
import google_auth_oauthlib.flow
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
from google_auth_oauthlib.flow import InstalledAppFlow
import google.auth
from google.cloud import pubsub

# Define the OAuth 2.0 scopes
SCOPES = ['https://www.googleapis.com/auth/yt-analytics.readonly', 'https://www.googleapis.com/auth/pubsub']

# Define the API service name and version
API_SERVICE_NAME = 'youtubeAnalytics'
API_VERSION = 'v2'

# Define the path to the client secrets file
CLIENT_SECRETS_FILE = 'path/to/client_secret.json'

def get_service():
    """
    Build and return the YouTube Analytics and Pub/Sub service objects.
    """
    flow = InstalledAppFlow.from_client_secrets_file(CLIENT_SECRETS_FILE, SCOPES)
    credentials = flow.run_console()
    youtubeAnalytics = build(API_SERVICE_NAME, API_VERSION, credentials=credentials)
    pubsub_client = pubsub.PublisherClient(credentials=credentials)
    return youtubeAnalytics, pubsub_client

def execute_api_request(client_library_function, **kwargs):
    """
    Execute the specified API request and print the response.
    """
    response = client_library_function(**kwargs).execute()
    print(response)

# Define a list of channel IDs for different artists
channel_ids = ["UCMgFcY-KUZVIrR_mXn4B_dA", "UCi2KnNtGk-wBz7x-kLbD5tR", "UCZU9T1ceaOgwfLRqFQrrG3w"]


# Iterate through the list of channel IDs
for channel_id in channel_ids:
    # Define the endpoint URL to get the latest video
    url = f"https://www.googleapis.com/youtube/v3/search?key={api_key}&channelId={channel_id}&part=snippet,id&order=date&maxResults=1"

    # Make the API request
    response = requests.get(url)

    # Parse the response JSON
    data = json.loads(response.text)

    # Extract the video details
    video_id = data['items'][0]['id']['videoId']
    video_title = data['items'][0]['snippet']['title']
    video_artist = data['items'][0]['snippet']['channelTitle']
    video_duration = data['items'][0]['snippet']['duration']
    video_publish_date = data['items'][0]['snippet']['publishedAt']
  
    # Convert the video publish date to datetime object
    video_publish_datetime = datetime.strptime(video_publish_date, "%Y-%m-%dT%H:%M:%S.%fZ")
    end_date = video_publish_datetime + timedelta(days=30)
    # Define the start and end date for the views
    start_date = video_publish_datetime.strftime("%Y-%m-%d")
    end_date = end_date.strftime("%Y-%m-%d")

    youtubeAnalytics = get_service()
    result = execute_api_request(
      youtubeAnalytics.reports().query,
      ids=f'channel=={channel_id}',
      startDate=start_date,
      endDate=end_date,
      metrics='views',
      dimensions='day',
      filters=f'video=={video_id}'
    )
    rows = result['rows']

    # Convert the data to JSON
    data = {
        'artist': video_artist,
        'title': video_title,
        'duration': video_duration,
        'views': rows
    }
    data_json = json.dumps(data)

    # Publish the data to the Pub/Sub topic
    publisher = pubsub_v1.PublisherClient()
    topic_name = 'projects/{project_id}/topics/{topic}'.format(
        project_id=PROJECT_ID,
        topic=PUBSUB_TOPIC
    )
    future = publisher.publish(topic_name, data=data_json.encode('utf-8'))
    future.result()
