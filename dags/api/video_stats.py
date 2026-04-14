
import requests
import json
from datetime import date

# import os
# from dotenv import load_dotenv
# load_dotenv(dotenv_path=".env")

from airflow.decorators import task
from airflow.models import Variable

API_KEY = Variable.get("API_KEY")
CHANNEL_HANDLE = Variable.get("CHANNEL_HANDLE", "MrBeast")
maxResults = 50

@task
def get_playlist_id():
    try:

        url = f"https://youtube.googleapis.com/youtube/v3/channels?part=contentDetails&forHandle={CHANNEL_HANDLE}&key={API_KEY}"

        response = requests.get(url)

        response.raise_for_status()  # Check if the request was successful


        data = response.json()
        #print(json.dumps(data, indent=4))

        channel_items = data['items'][0]
        channel_playlist_id = channel_items['contentDetails']['relatedPlaylists']['uploads']
        #print(channel_playlist_id)

        return channel_playlist_id
    
    except requests.exceptions.RequestException as e:
        print(f"An error occurred: {e}")

@task
def get_video_ids(playlist_id):

    video_ids = []

    pageToken = None

    base_url = f"https://youtube.googleapis.com/youtube/v3/playlistItems?part=contentDetails&playlistId={playlist_id}&maxResults={maxResults}&key={API_KEY}"
                                                                                                                                                   
    try:
        while True:
            url = base_url
            if pageToken:
                url += f"&pageToken={pageToken}"

            response = requests.get(url)
            response.raise_for_status()  # Check if the request was successful

            data = response.json()

            for item in data.get('items', []):
                video_id = item['contentDetails']['videoId']
                video_ids.append(video_id)

            pageToken = data.get('nextPageToken')
            if not pageToken:
                break

        return video_ids

    
    except requests.exceptions.RequestException as e:
        print(f"An error occurred: {e}")
    


@task
def extract_video_data(video_ids):
    video_data = []

    def batch_list(video_id_lst, batch_size):
        for i in range(0, len(video_id_lst), batch_size):
            yield video_id_lst[i:i + batch_size]

    try:
        for batch in batch_list(video_ids, maxResults):
            ids = ",".join(batch)
            url = f"https://youtube.googleapis.com/youtube/v3/videos?part=contentDetails&part=snippet&part=statistics&id={ids}&key={API_KEY}"
                
            response = requests.get(url)
            response.raise_for_status()  # Check if the request was successful
                
            data = response.json()
                
            for item in data.get('items', []):
                video_info = {
                    'video_id': item['id'],                        
                        'title': item['snippet']['title'],
                        'publishedAt': item['snippet']['publishedAt'],
                        "duration": item['contentDetails']['duration'],
                        'viewCount': item['statistics'].get('viewCount', None),
                        'likeCount': item['statistics'].get('likeCount', None),
                        'commentCount': item['statistics'].get('commentCount', None)
                    }
                video_data.append(video_info)

    except requests.exceptions.RequestException as e:
        print(f"An error occurred: {e}")
    
    return video_data

@task
def save_to_json(data):
    file_path = f"./data/YT_data_{date.today()}.json"
    with open(file_path, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=4, ensure_ascii=False)

if __name__ == "__main__":
    print("Fetching playlist ID...")
    playlist_id = get_playlist_id()
    print(f"Playlist ID: {playlist_id}")
    video_ids = get_video_ids(playlist_id)
    #print(f"Video IDs: {video_ids}")
    #print(extract_video_data(video_ids))
    video_data = extract_video_data(video_ids)
    save_to_json(video_data)
else:   
    print("This module is being imported, not run directly.")