import requests
import pandas as pd
import boto3
from io import StringIO

def fetch_youtube_videos():
    # --- AWS & S3 Configuration ---
    AWS_REGION = "eu-north-1"
    S3_BUCKET = "bhavika-etl-youtube"
    S3_KEY = "youtube_videos.csv"  # CSV filename in S3

    s3_client = boto3.client("s3", region_name=AWS_REGION)

    # --- YouTube API Configuration ---
    API_KEY = "<API-KEY>"
    CHANNEL_ID = "UCk2U-Oqn7RXf-ydPqfSxG5g"

    # Step 1: Get Uploads Playlist ID
    url = f"https://www.googleapis.com/youtube/v3/channels?part=contentDetails&id={CHANNEL_ID}&key={API_KEY}"
    channel_response = requests.get(url).json()

    if "items" not in channel_response or len(channel_response["items"]) == 0:
        raise Exception("No channel found. Check your CHANNEL_ID or API_KEY.")

    uploads_playlist_id = channel_response["items"][0]["contentDetails"]["relatedPlaylists"]["uploads"]

    # Step 2: Get Videos from Uploads Playlist
    videos = []
    next_page_token = None

    while True:
        playlist_url = (
            f"https://www.googleapis.com/youtube/v3/playlistItems"
            f"?part=snippet,contentDetails&maxResults=50&playlistId={uploads_playlist_id}"
            f"&key={API_KEY}"
        )
        if next_page_token:
            playlist_url += f"&pageToken={next_page_token}"

        resp = requests.get(playlist_url).json()

        for item in resp.get("items", []):
            videos.append({
                "videoId": item["contentDetails"]["videoId"],
                "title": item["snippet"]["title"],
                "publishedAt": item["contentDetails"]["videoPublishedAt"]
            })

        next_page_token = resp.get("nextPageToken")
        if not next_page_token:
            break

    # Step 3: Convert to pandas DataFrame
    df = pd.DataFrame(videos)

    # Step 4: Save CSV to S3
    csv_buffer = StringIO()
    df.to_csv(csv_buffer, index=False)
    s3_client.put_object(Bucket=S3_BUCKET, Key=S3_KEY, Body=csv_buffer.getvalue())
    
    print(f"✅ Data saved as CSV to s3://{S3_BUCKET}/{S3_KEY}")
    print(df.head())

    return df

# Run the functionfetch_youtube_videos()
