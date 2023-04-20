import json
import os
import psycopg2
from googleapiclient.discovery import build

def update_channel_stats():
    # 채널 ID 리스트 불러오기
    with open('data/channels_renewal_202304031340.csv', 'r') as f:
        channel_id_list = f.read().splitlines()

    # youtube API client 생성
    DEVELOPER_KEY = os.environ.get('DEVELOPER_KEY')
    YOUTUBE_API_SERVICE_NAME = 'youtube'
    YOUTUBE_API_VERSION = 'v3'
    youtube = build(YOUTUBE_API_SERVICE_NAME, YOUTUBE_API_VERSION, developerKey=DEVELOPER_KEY)

    # 빈 리스트 생성
    channel_data = []

    # 채널 ID 리스트를 순회하며 API 호출하여 데이터 추출
    for channel_id in channel_id_list:
        try:
            search_response = youtube.channels().list(
                # 'channel_id'를 대상으로 'snippet', 'statistics' 검색
                part='id,snippet,statistics',
                id=channel_id,
                maxResults=50
            ).execute()

            # search_response에서 statistics 및 snippet 데이터 추출
            stats = search_response['items'][0]['statistics']
            snippet = search_response['items'][0]['snippet']

            # stats 및 snippet dictionary에서 데이터 정리
            channel = {
                'channel_id': channel_id,
                'view_count': stats['viewCount'],
                'subscriber_count': stats['subscriberCount'],
                'video_count': stats['videoCount'],
                'title': snippet['title']
            }

            channel_data.append(channel)

        except:
            continue

    # postgresql에 연결
    DATABASE_URL = os.environ.get('DATABASE_URL')
    conn = psycopg2.connect(DATABASE_URL, sslmode='require')

    # 데이터를 PostgreSQL에 삽입
    cur = conn.cursor()
    for channel in channel_data:
        cur.execute(
            "INSERT INTO channel_stats (channel_id, channel_title, subscriber_count, view_count, video_count) VALUES (%s, %s, %s, %s, %s)",
            (channel['channel_id'], channel['title'], channel['subscriber_count'], channel['view_count'],
             channel['video_count']))
    conn.commit()
    cur.close()
    conn.close()

def handler(event, context):
    update_channel_stats()

    return {
        'statusCode': 200,
        'body': json.dumps('Hello from Vercel!')
    }
