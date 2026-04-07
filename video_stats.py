import requests
import json

import os
from dotenv import load_dotenv
from datetime import date

load_dotenv(dotenv_path=".env")

API_KEY = os.getenv("API_KEY")
CHANNEL_HANDLE = "MrBeast"
maxResults = 50

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
                        'published_at': item['snippet']['publishedAt'],
                        'view_count': item['statistics'].get('viewCount', None),
                        'like_count': item['statistics'].get('likeCount', None),
                        'comment_count': item['statistics'].get('commentCount', None)
                    }
                video_data.append(video_info)

    except requests.exceptions.RequestException as e:
        print(f"An error occurred: {e}")
    
    return video_data

def save_to_json(data):
    file_path = f"/home/yangzx_97/YouTube_ETL/data/data_{date.today()}.json"
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