import time

import requests
import config
from sqlalchemy import create_engine, text
from sqlalchemy import event
import sqlalchemy
import pandas as pd

def tel_send_message(text):
    url = f'https://api.telegram.org/bot{config.telegram_token}/sendMessage'
    payload = {
        'chat_id': config.chat_id,
        'text': text
    }
    r = requests.post(url, json=payload)
    return r

def tel_send_image(photo_url, caption):
    url = f'https://api.telegram.org/bot{config.telegram_token}/sendPhoto'
    payload = {
        'chat_id': config.chat_id,
        'photo': photo_url,
        'caption': caption
    }
    r = requests.post(url, json=payload)
    return r

def tel_send_audio(audio_url):
    url = f'https://api.telegram.org/bot{config.telegram_token}/sendAudio'
    payload = {
        'chat_id': config.chat_id,
        "audio": audio_url,
    }
    r = requests.post(url, json=payload)
    return r

def tel_send_video(video_url):
    url = f'https://api.telegram.org/bot{config.telegram_token}/sendVideo'
    payload = {
        'chat_id': config.chat_id,
        "video": video_url,
    }
    r = requests.post(url, json=payload)
    return r


def tel_send_document(document_url):
    url = f'https://api.telegram.org/bot{config.telegram_token}/sendDocument'
    payload = {
        'chat_id': config.chat_id,
        "document": document_url,
    }
    r = requests.post(url, json=payload)
    return r


def tel_send_button(text):
    url = f'https://api.telegram.org/bot{config.telegram_token}/sendMessage'
    payload = {
        'chat_id': config.chat_id,
        'text': text,
        'reply_markup': {'keyboard': [[{'text': 'supa'}, {'text': 'mario'}]]}
    }
    r = requests.post(url, json=payload)
    return r

if __name__ == '__main__':

    base_time = time.strftime('%H')
    time.sleep(60)

    while True:
        compare_time = time.strftime('%H')
        if base_time != compare_time:
            base_time = compare_time
            engine = create_engine(f'mariadb+pymysql://{config.user}:{config.pwd}@{config.host}:3306/{config.db}', echo=False)

            conn = engine.connect()

            database_count_sql = text('''
            SELECT  base.za,
                    A.gapi,
                    B.gapie,		
                    C.gbboi,
                    D.gbboie,
                    E.gbepai,
                    F.gbepaie,
                    G.gbfoi,
                    H.gbfoie,
                    I.gbrti,
                    J.gbrtie,
                    K.gbti,
                    L.gbtie,
                    M.gx,
                    N.gxe
            FROM (select count(management_key) as za from zz_all) base,
                 (select count(management_key) as gapi from getArchitecturePossessionInfo) A,
                 (select count(management_key) as gapie from getArchitecturePossessionInfo_empty) B,
                 (select count(management_key) as gbboi from getBrBasisOulnInfo) C,
                 (select count(management_key) as gbboie from getBrBasisOulnInfo_empty) D,
                 (select count(management_key) as gbepai from getBrExposPubuseAreaInfo) E,
                 (select count(management_key) as gbepaie from getBrExposPubuseAreaInfo_empty) F,
                 (select count(management_key) as gbfoi from getBrFlrOulnInfo) G,
                 (select count(management_key) as gbfoie from getBrFlrOulnInfo_empty) H,
                 (select count(management_key) as gbrti from getBrRecapTitleInfo) I,
                 (select count(management_key) as gbrtie from getBrRecapTitleInfo_empty) J,
                 (select count(management_key) as gbti from getBrTitleInfo) K,
                 (select count(management_key) as gbtie from getBrTitleInfo_empty) L,
                 (select count(management_key) as gx from getXYcoordinate) M,
                 (select count(management_key) as gxe from getXYcoordinate_empty) N
            ''')

            database_count_sql_df = pd.read_sql_query(database_count_sql, conn)

            tel_send_message('현재시간: ' + time.strftime('%y년 %m월 %d일 %H시 %M분')
                             + '\n전체 데이터: ' + str(database_count_sql_df['za'][0])
                             + '\n건축물대장소유자정보: ' + str(database_count_sql_df['gapi'][0]) + ' 누락: ' + str(database_count_sql_df['gapie'][0])
                             + '\n건축물대장 기본 개요: ' + str(database_count_sql_df['gbboi'][0]) + ' 누락: ' + str(database_count_sql_df['gbboie'][0])
                             + '\n건축물대장 전유공용면적: ' + str(database_count_sql_df['gbepai'][0]) + ' 누락: ' + str(database_count_sql_df['gbepaie'][0])
                             + '\n건축물대장 층별 개요: ' + str(database_count_sql_df['gbfoi'][0]) + ' 누락: ' + str(database_count_sql_df['gbfoie'][0])
                             + '\n건축물대장 총괄표제부: ' + str(database_count_sql_df['gbrti'][0]) + ' 누락: ' + str(database_count_sql_df['gbrti'][0])
                             + '\n건축물대장 표제부: ' + str(database_count_sql_df['gbti'][0]) + ' 누락: ' + str(database_count_sql_df['gbtie'][0])
                             + '\n주소좌표변환: ' + str(database_count_sql_df['gx'][0]) + ' 누락: ' + str(database_count_sql_df['gxe'][0])
                             )
