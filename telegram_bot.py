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

    before_gapi    = 0
    before_gapie   = 0
    before_gbboi   = 0
    before_gbboie  = 0
    before_gbepai  = 0
    before_gbepaie = 0
    before_gbfoi   = 0
    before_gbfoie  = 0
    before_gbrti   = 0
    before_gbrtie  = 0
    before_gbti    = 0
    before_gbtie   = 0
    before_gx      = 0
    before_gxe     = 0

    while True:
        compare_time = time.strftime('%H')
        if base_time != compare_time:
            base_time = compare_time
            engine = create_engine(f'mariadb+pymysql://{config.user}:{config.pwd}@{config.host}:3306/{config.db}', echo=False)

            conn = engine.connect()

            before_gapi = database_count_sql_df['gapi'][0]
            before_gapie = database_count_sql_df['gapie'][0]
            before_gbboi = database_count_sql_df['gbboi'][0]
            before_gbboie = database_count_sql_df['gbboie'][0]
            before_gbepai = database_count_sql_df['gbepai'][0]
            before_gbepaie = database_count_sql_df['gbepaie'][0]
            before_gbfoi = database_count_sql_df['gbfoi'][0]
            before_gbfoie = database_count_sql_df['gbfoie'][0]
            before_gbrti = database_count_sql_df['gbrti'][0]
            before_gbrtie = database_count_sql_df['gbrtie'][0]
            before_gbti = database_count_sql_df['gbti'][0]
            before_gbtie = database_count_sql_df['gbtie'][0]
            before_gx = database_count_sql_df['gx'][0]
            before_gxe = database_count_sql_df['gxe'][0]

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

            gapi    = database_count_sql_df['gapi'][0]
            gapie   = database_count_sql_df['gapie'][0]
            gbboi   = database_count_sql_df['gbboi'][0]
            gbboie  = database_count_sql_df['gbboie'][0]
            gbepai  = database_count_sql_df['gbepai'][0]
            gbepaie = database_count_sql_df['gbepaie'][0]
            gbfoi   = database_count_sql_df['gbfoi'][0]
            gbfoie  = database_count_sql_df['gbfoie'][0]
            gbrti   = database_count_sql_df['gbrti'][0]
            gbrtie  = database_count_sql_df['gbrtie'][0]
            gbti    = database_count_sql_df['gbti'][0]
            gbtie   = database_count_sql_df['gbtie'][0]
            gx      = database_count_sql_df['gx'][0]
            gxe     = database_count_sql_df['gxe'][0]

            calculate_gapi    = gapi - before_gapi
            calculate_gapie   = gapie - before_gapie
            calculate_gbboi   = gbboi - before_gbboi
            calculate_gbboie  = gbboie - before_gbboie
            calculate_gbepai  = gbepai - before_gbepai
            calculate_gbepaie = gbepaie - before_gbepaie
            calculate_gbfoi   = gbfoi - before_gbfoi
            calculate_gbfoie  = gbfoie - before_gbfoie
            calculate_gbrti   = gbrti - before_gbrti
            calculate_gbrtie  = gbrtie - before_gbrtie
            calculate_gbti    = gbti - before_gbti
            calculate_gbtie   = gbtie - before_gbtie
            calculate_gx      = gx - before_gx
            calculate_gxe     = gxe - before_gxe

            tel_send_message('현재시간: ' + time.strftime('%y년 %m월 %d일 %H시 %M분')
                             + '\n전체 데이터: ' + str(database_count_sql_df['za'][0])
                             + '\n건축물대장소유자정보: ' + str(database_count_sql_df['gapi'][0]) + ' 누락: ' + str(database_count_sql_df['gapie'][0])
                             + '\n건축물대장 기본 개요: ' + str(database_count_sql_df['gbboi'][0]) + ' 누락: ' + str(database_count_sql_df['gbboie'][0])
                             + '\n건축물대장 전유공용면적: ' + str(database_count_sql_df['gbepai'][0]) + ' 누락: ' + str(database_count_sql_df['gbepaie'][0])
                             + '\n건축물대장 층별 개요: ' + str(database_count_sql_df['gbfoi'][0]) + ' 누락: ' + str(database_count_sql_df['gbfoie'][0])
                             + '\n건축물대장 총괄표제부: ' + str(database_count_sql_df['gbrti'][0]) + ' 누락: ' + str(database_count_sql_df['gbrtie'][0])
                             + '\n건축물대장 표제부: ' + str(database_count_sql_df['gbti'][0]) + ' 누락: ' + str(database_count_sql_df['gbtie'][0])
                             + '\n주소좌표변환: ' + str(database_count_sql_df['gx'][0]) + ' 누락: ' + str(database_count_sql_df['gxe'][0])
                             + '\n\n이전데이터 비교 정보'
                             + '\n건축물대장소유자정보: ' + str(calculate_gapi) + ' 누락: ' + str(calculate_gapie)
                             + '\n건축물대장 기본 개요: ' + str(calculate_gbboi) + ' 누락: ' + str(calculate_gbboie)
                             + '\n건축물대장 전유공용면적: ' + str(calculate_gbepai) + ' 누락: ' + str(calculate_gbepaie)
                             + '\n건축물대장 층별 개요: ' + str(calculate_gbfoi) + ' 누락: ' + str(calculate_gbfoie)
                             + '\n건축물대장 총괄표제부: ' + str(calculate_gbrti) + ' 누락: ' + str(calculate_gbrtie)
                             + '\n건축물대장 표제부: ' + str(calculate_gbti) + ' 누락: ' + str(calculate_gbtie)
                             + '\n주소좌표변환: ' + str(calculate_gx) + ' 누락: ' + str(calculate_gxe)
                             )
