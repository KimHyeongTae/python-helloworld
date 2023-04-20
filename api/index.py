import os
from http.server import BaseHTTPRequestHandler
from google-api-python-client.discovery import build
import psycopg2

DEVELOPER_KEY = os.environ.get('DEVELOPER_KEY')
host = os.environ.get('host')
port = os.environ.get('port')
database = os.environ.get('database')
user = os.environ.get('user')
password = os.environ.get('password')


class handler(BaseHTTPRequestHandler):

    def do_GET(self):
        self.send_response(200)
        self.send_header('Content-type', 'text/plain')
        self.end_headers()
        self.wfile.write('Hello, world!'.encode('utf-8'))

        # youtube data api 사용을 위한 인증키 로드
        DEVELOPER_KEY = DEVELOPER_KEY
        YOUTUBE_API_SERVICE_NAME = 'youtube'
        YOUTUBE_API_VERSION = 'v3'

        def update_channel_stats():
            # 채널 ID 리스트 불러오기
            with open('data/channels_renewal_202304031340.csv', 'r') as f:
                channel_id_list = f.read().splitlines()

            # youtube API client 생성
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
            conn = psycopg2.connect(
                host=host,
                port=port,
                database=database,
                user=user,
                password=password
            )

            # 데이터프레임을 postgresql에 삽입
            cur = conn.cursor()
            for channel in channel_data:
                cur.execute(
                    "INSERT INTO channel_stats (channel_id, channel_title, subscriber_count, view_count, video_count) VALUES (%s, %s, %s, %s, %s)",
                    (channel['channel_id'], channel['title'], channel['subscriber_count'], channel['view_count'],
                     channel['video_count']))
            conn.commit()
            cur.close()
            conn.close()

        update_channel_stats()

        return
