import os
from http.server import BaseHTTPRequestHandler
import pandas as pd
from googleapiclient.discovery import build
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
        self.send_header('Content-type','text/plain')
        self.end_headers()
        self.wfile.write('Hello, world!'.encode('utf-8'))

        # youtube data api 사용을 위한 인증키 로드
        DEVELOPER_KEY = DEVELOPER_KEY
        YOUTUBE_API_SERVICE_NAME = 'youtube'
        YOUTUBE_API_VERSION = 'v3'


        def update_channel_stats():
            # CSV 파일에서 채널 ID 리스트 불러오기
            channel_id_list = pd.read_csv("data/channels_renewal_202304031340.csv")

            # youtube API client 생성
            youtube = build(YOUTUBE_API_SERVICE_NAME, YOUTUBE_API_VERSION, developerKey=DEVELOPER_KEY)

            # 빈 데이터프레임 생성
            channel_data = pd.DataFrame()

            # 채널 ID 리스트를 순회하며 API 호출하여 데이터 추출
            for i in range(len(channel_id_list)):
                try:
                    channel_id = channel_id_list["channel_id"][i]
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
                    stats_series = pd.Series([channel_id, stats['viewCount'], stats['subscriberCount'], stats['videoCount'], snippet['title']],
                                          index=['channel_id', 'view_count', 'subscriber_count', 'video_count', 'title'])

                    channel_data = pd.concat([channel_data, stats_series.to_frame().transpose()], ignore_index=True)

                    if i < len(channel_id_list):
                      i = i+1
                    else: break
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
            for index, row in channel_data.iterrows():
                cur.execute("INSERT INTO channel_stats (channel_id, channel_title, subscriber_count, view_count, video_count) VALUES (%s, %s, %s, %s, %s)",
                            (row['channel_id'], row['title'], row['subscriber_count'], row['view_count'], row['video_count']))
            conn.commit()
            cur.close()
            conn.close()

        update_channel_stats()


        
        return
