import base64
from google.cloud import bigquery,secretmanager,pubsub_v1
import ast
import requests
from datetime import datetime
import time
import pandas as pd
import pandas_gbq
import json
from dotenv import load_dotenv
import os

load_dotenv()
SECRET_NAME = os.getenv("SECRET_NAME") 
PROJECT_ID = os.getenv("PROJECT_ID")
SPOTIFY_TRACKS_TABLE = os.getenv("SPOTIFY_TRACKS_TABLE")
ARTIST_TRACKS_TABLE = os.getenv("ARTIST_TRACKS_TABLE")
ARTISTS_TABLE = os.getenv("ARTISTS_TABLE")
SPOTIFY_MARKET = "IN" # Enter your market
TOPIC_ID = os.getenv("TOPIC_ID")

def refresh_token_and_update_secret(spotify_secret_json: dict,secret_name: str,project_id: str):
    
    print("Refreshing access token ...")
    api_url = "https://accounts.spotify.com/api/token"

    client_credentials = f'''{spotify_secret_json['client_id']}:{spotify_secret_json['client_secret']}'''
    client_credentials_b64 = base64.b64encode(client_credentials.encode()).decode()

    print(f"base 64 value : {client_credentials_b64}")

    api_headers = {
    'Authorization': f'Basic {client_credentials_b64}',
    'Content-Type': 'application/x-www-form-urlencoded'
     }
    
    data = {
    'grant_type': 'refresh_token',
    'refresh_token': spotify_secret_json["refresh_token"]
    }

    response = requests.post(api_url, headers=api_headers, data=data)

    if response.status_code == 200:
    # Extract the access token from the response
        access_token = response.json().get('access_token')
        spotify_secret_json["access_token"] = access_token
        print(" Refresh token api successful , now updating the secret manager")
        update_secret(spotify_secret_json,secret_name,project_id)
        
    else:
        print("Failed to refresh the token. Status Code:", response.status_code)
        print("Response:", response.json())

def publish_message(project_id: str, topic_id: str, message: str):
    """Publishes single message to a Pub/Sub topic."""

    publisher = pubsub_v1.PublisherClient()
    topic_path = publisher.topic_path(project_id, topic_id)
    print(f"topic_path: {topic_path}")
    data_str = message
    # Data must be a bytestring
    data = data_str.encode("utf-8")
    # When you publish a message, the client returns a future.
    
    try:
        print("Publishing message ...")
        future = publisher.publish(topic_path, data)
    except Exception as e:
        print("Error publishing: " + str(e))
    print(f"Published messages to {topic_path}.")

def get_secret(secret_name: str,project_id: str):
    '''
    Accesss secret from google secret manager
    '''
    print("Accessing spotify api token from secret manager...")
    try:
        secret_client = secretmanager.SecretManagerServiceClient()
        name = f"projects/{project_id}/secrets/{secret_name}/versions/latest"
        encoded_secret = secret_client.access_secret_version(request={"name": name})
        decoded_secret = encoded_secret.payload.data.decode('UTF-8')
        secret = ast.literal_eval(decoded_secret) # convert to dictionary
        return secret

    except Exception as e:
        print(f"Error retrieving secret: {e}")
        return None

def update_secret(spotify_secret_json: dict,secret_name: str,project_id: str):
    
    print("Updating secret manager with new secret value ....")
    # Create the Secret Manager client.
    client = secretmanager.SecretManagerServiceClient()

    # Build the resource name of the parent secret.
    parent = client.secret_path(project_id, secret_name)

    payload = json.dumps(spotify_secret_json)
    payload = payload.encode("UTF-8")

    # Add the secret version.
    response = client.add_secret_version(
        request={
            "parent": parent,
            "payload": {"data": payload},
        }
    )

    # Print the new secret version name.
    print("Added secret version: {}".format(response.name))

def get_user_saved_tracks(offset: int,market: str,spotify_secret_json: dict,secret_name: str,project_id: str,tracks_count_bq: int):

    print("Getting user saved tracks from spotify ....")
    api_url = f'https://api.spotify.com/v1/me/tracks?market={market}'
    api_headers = {
    'Authorization': f'Bearer {spotify_secret_json["access_token"]}'
    }
    params = {"offset": offset}

    try:
        api_response = requests.get(api_url, headers=api_headers,params=params)
        status_code = api_response.status_code
        print(f"API status code for fetching saved tracks : {status_code}")
    except Exception as e:
        print(f"Exception {e} while hitting api to fetch saved tracks")
        return None,None

    if status_code == 401: #Unauthorized
        print("** API token expired **")
        refresh_token_and_update_secret(spotify_secret_json,secret_name,project_id)
        return offset,None
    
    if status_code == 413: # Too many api requests
        print("Too many api requests...")
        time.sleep(180) # sleep for 3 minutes
        return offset,None

    tracks = api_response.json()
    max_tracks = tracks["total"]

    if max_tracks == tracks_count_bq:
        print("No new tracks to process")
        return -1,[]
    
    if offset+20 > max_tracks:
        print(" Reached max limit")
        print(" All Songs have been added to DB")
        # new_offset = offset + 20
        return -1,tracks
    
    new_offset = offset + 20
    return new_offset,tracks

def write_to_BQ(data: list[dict],table_id: str,project_id: str):

    df = pd.DataFrame(data)
    table_name = f"{project_id}.{table_id}"
    try:
        df.head() 
        pandas_gbq.to_gbq(df,table_name, if_exists='append', project_id= project_id)
    except Exception as e:
        print(f"Exception while writing to  bq {e}")

def parse_tracks(tracks: list,bq_tracks: set,artists_in_bq: set,match: int,spotify_secret_json: dict,secret_name: str,project_id: str):
    
    print(f"Parsing tracks and artists data")
    track_data_list = []
    artist_track_data_list = []
    artist_data_list = []

    length_of_response = len(tracks["items"])
    count = 0
    new_artists_current_run = set()
    for track_detail in tracks["items"]:
        
        track = track_detail["track"]
        track_data = {}
        


        track_id = track["id"]

        if track_id in bq_tracks:
            count+=1
            continue

        track_data["spotify_track_id"] = track_id
        track_data["track_name"] = track["name"]
        track_data["track_duration"] = track["duration_ms"]/1000
        track_data["is_explicit"] = track["explicit"]
        track_data["spotify_url"] = track["external_urls"]["spotify"]
        track_data["spotify_album_id"] = track["id"]
        track_data["album_name"] = track["album"]["name"]
        track_data["album_release_date"] = track["album"]["release_date"]
        track_data["date_added_to_spotify"] = track_detail["added_at"]
        track_data["date_added_to_table"] = datetime.today().strftime('%Y-%m-%d')
        # track_data["offset"] = offset

        track_data_list.append(track_data)

        for artist in track["artists"]:
            artist_data = {}
            artist_track_data = {}
            artist_id = artist["id"]

            if artist_id not in artists_in_bq and artist_id not in new_artists_current_run:
                new_artists_current_run.add(artist_id)
                artist_data["spotify_artist_id"] = artist["id"]
                artist_data["artist_name"] = artist["name"]
                genres = get_artist_genres(artist["id"],spotify_secret_json,secret_name,project_id)
                if genres == -1:
                    print(f" Unable to fetch genres")
                    return match,[],[],[]
                
                print(f"Fetched artist {artist['name']} generes are: {genres} ")
                artist_data["genres"] = genres
                artist_data["type"] = artist["type"]

                artist_data_list.append(artist_data)
                
            else:
                print(f"Artist {artist['name']} already exists")

            artist_track_data["track_id"] = track_id
            artist_track_data["artist_id"] = artist_id

            artist_track_data_list.append(artist_track_data)


    if count == length_of_response:
        match+=1

    return match,track_data_list,artist_track_data_list,artist_data_list

def get_artist_from_BQ(artist_table: str,project_id: str):

    print("Getting saved Artists from big query ...") 
    query = f"select spotify_artist_id from {project_id}.{artist_table}"
    bq_client = bigquery.Client(project=project_id)

    try:
        query_job = bq_client.query(query)
        result = {row.spotify_artist_id for row in query_job.result()}
        print(f"Successfully fetched {len(result)} artists from BQ")
        return result

    except Exception as e:
         print(f"Exception caught while fetching artists from BQ {e}")
         return set()

def get_artist_genres(artist_id: str,spotify_secret_json: dict,secret_name: str,project_id: str):
    
    print("Getting artist genre")
    api_url = f"https://api.spotify.com/v1/artists/{artist_id}"
    api_headers = {
    'Authorization': f'Bearer {spotify_secret_json["access_token"]}'
    }
    
    try:
        api_response = requests.get(api_url, headers=api_headers)
        status_code = api_response.status_code
        # print(f"API status code {status_code}")
    except Exception as e:
        print(f"Exception {e} while hitting api ")
        return -1

    if status_code == 401: #Unauthorized
        print("** API token expired **")
        refresh_token_and_update_secret(spotify_secret_json,secret_name,project_id)
        return -1
    
    if status_code == 413: # Too many api requests
        print("Too many api requests...")
        time.sleep(180) # sleep for 3 minutes
        return -1
    
    artist_data = api_response.json()
    # print(artist_data)
    return ','.join(artist_data["genres"])

def get_tracks_from_BQ(tracks_table: str,project_id: str):

    print("Getting saved Tracks from big query ...")
    query = f"select spotify_track_id from {project_id}.{tracks_table}  order by date_added_to_spotify"
    bq_client = bigquery.Client(project=project_id)

    try:
        query_job = bq_client.query(query)
        result = {row.spotify_track_id for row in query_job.result()}
        print(f"Successfully fetched {len(result)} tracks from BQ")
        return result

    except Exception as e:
         print(f"Exception caught while fetching tracks from BQ : {e}")
         return set()

def hello_pubsub(event, context):
    """Triggered from a message on a Cloud Pub/Sub topic.
    Args:
         event (dict): Event payload.
         context (google.cloud.functions.Context): Metadata for the event.
    """
    pubsub_message = base64.b64decode(event['data']).decode('utf-8')
    print(pubsub_message)

    cloud_function_count = 1 # Stores the current cloud function count during self invocation
    offset = 0
    match = 0

    if "rebirth" in pubsub_message:
        cloud_function_count = int(pubsub_message.split("-")[1])+1
        print(f" {cloud_function_count} life of cloud function started")
        offset = int(pubsub_message.split("-")[2])
        match = int(pubsub_message.split("-")[3])

    if cloud_function_count > 1000:

        print(f"*** Maximum number of cloud function self invocations happened ***")
        print(f"*** WARNING: Exiting Cloud function to prevent infinite loop ***")
        return 0
    
    if match > 15:
        print(f"Last 10 sets of tracked matched")
        print("Ending cloud function")
        return 0

    spotify_secret_json = get_secret(SECRET_NAME,PROJECT_ID)

    if not spotify_secret_json:

        print("Error accessing secret, Terminating cloud function")
        return 0

    tracks_in_bq = get_tracks_from_BQ(SPOTIFY_TRACKS_TABLE,PROJECT_ID)
    artists_in_bq = get_artist_from_BQ(ARTISTS_TABLE,PROJECT_ID)

    new_offset,new_tracks = get_user_saved_tracks(offset,SPOTIFY_MARKET,spotify_secret_json,SECRET_NAME,PROJECT_ID,len(tracks_in_bq))
    
    if new_tracks:
        print("Found new tracks saved")
        
        match,tracks_data,artist_track_data,artist_data = parse_tracks(new_tracks,tracks_in_bq,artists_in_bq,match,spotify_secret_json,SECRET_NAME,PROJECT_ID)

        print(f"Length of new tracks {len(tracks_data)}")
        print(f"Length of new artists {len(artist_data)}")
        print(f"Length of new artists-tracks {len(artist_track_data)}")
        
        if tracks_data:
            print("Writing Tracks Data to BQ")
            write_to_BQ(tracks_data,SPOTIFY_TRACKS_TABLE,PROJECT_ID)
        
        if artist_track_data:
            print("Writing Artist Track Data to BQ")
            write_to_BQ(artist_track_data,ARTIST_TRACKS_TABLE,PROJECT_ID)

        if artist_data:
            print("Writing Artist Data to BQ")
            write_to_BQ(artist_data,ARTISTS_TABLE,PROJECT_ID)

    if new_offset != -1:
        print("Self Invoking Cloud function")
        print(f"Matching so far : {match}")
        message = f"rebirth-{cloud_function_count}-{new_offset}-{match}"
        publish_message(PROJECT_ID,TOPIC_ID,message)

    print("Cloud function Finished Successfully")


