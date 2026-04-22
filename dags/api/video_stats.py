
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
MAX_RESULTS = 50

# Get the playlist ID for the channel's uploads
def _get_playlist_id(api_key: str, channel_handle: str) -> str:
    url = f"https://youtube.googleapis.com/youtube/v3/channels?part=contentDetails&forHandle={channel_handle}&key={api_key}"
    response = requests.get(url)
    response.raise_for_status()
    data = response.json()
    return data['items'][0]['contentDetails']['relatedPlaylists']['uploads']

@task
def get_playlist_id() -> str:
    return _get_playlist_id(API_KEY, CHANNEL_HANDLE)


# Get video IDs from the playlist
def _get_video_ids(api_key: str, playlist_id: str) -> list:
    video_ids = []
    pageToken = None
    base_url = f"https://youtube.googleapis.com/youtube/v3/playlistItems?part=contentDetails&playlistId={playlist_id}&maxResults={MAX_RESULTS}&key={api_key}"

    while True:
        url = base_url
        if pageToken:
            url += f"&pageToken={pageToken}"
        response = requests.get(url, timeout=30)
        response.raise_for_status()
        data = response.json()
        for item in data.get('items', []):
            video_ids.append(item['contentDetails']['videoId'])
        pageToken = data.get('nextPageToken')
        if not pageToken:
            break

    return video_ids



@task
def get_video_ids() -> list:
    return _get_video_ids(API_KEY, CHANNEL_HANDLE)    
    


# Get video details for each video ID
def _extract_video_data(video_ids: list) -> list:
    video_data = []

    def batch_list(video_id_lst, batch_size):
        for i in range(0, len(video_id_lst), batch_size):
            yield video_id_lst[i:i + batch_size]

    for batch in batch_list(video_ids, MAX_RESULTS):
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

    return video_data

@task
def extract_video_data(video_ids: list) -> list:
    return _extract_video_data(video_ids)


def _save_to_json(data):
    file_path = f"./data/YT_data_{date.today()}.json"
    with open(file_path, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=4, ensure_ascii=False)

@task
def save_to_json(data: list):
    return _save_to_json(data) 

if __name__ == "__main__":
    playlist_id = _get_playlist_id(API_KEY, CHANNEL_HANDLE)
    print(f"Playlist ID: {playlist_id}")
    video_ids = _get_video_ids(API_KEY, playlist_id)
    video_data = _extract_video_data(video_ids)
    _save_to_json(video_data)
else:   
    print("This module is being imported, not run directly.")