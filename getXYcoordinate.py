from tqdm import tqdm
import pandas as pd
import openpyxl

from sqlalchemy import create_engine, text
from sqlalchemy import event
import sqlalchemy

import requests as re
import config   #API Key 및 Server 정보

import time
from bs4 import BeautifulSoup as bs
from pyproj import Proj, transform  # 좌표계 변환 라이브러리

# FutureWarning 제거(사용 모듈의 업데이트 시 의도대로 작동하지 않을 가능성이 있음을 알리는 경고)
import warnings
warnings.simplefilter(action='ignore', category=FutureWarning)

delay = 0.5

opertaion_list = ['getBrBasisOulnInfo', # 건축물대장 기본개요 조회
                'getBrRecapTitleInfo', # 건축물대장 총괄표제부 조회
                'getBrTitleInfo', # 건축물대장 표제부 조회
                'getBrFlrOulnInfo', # 건축물대장 층별개요 조회
                'getBrExposPubuseAreaInfo', # 건축물대장 전유공용면적 조회
                 ]


def getXYcoordinate(management_key, management_number, sido_name, load_address, admCd, rnMgtSn, udrtYn, buldMnnm,
                    buldSlno):  # 주소좌표변환
    params = {'admCd': admCd, 'rnMgtSn': rnMgtSn, 'udrtYn': udrtYn, 'buldMnnm': buldMnnm,
              'buldSlno': buldSlno, 'confmKey': config.geocode_key}
    url = 'https://business.juso.go.kr/addrlink/addrCoordApi.do'
    res = re.get(url, params=params)
    soup = bs(res.text, features='xml')

    totalCount = soup.find('totalCount').get_text()
    errorCode = soup.find('errorCode').get_text()
    errorMessage = soup.find('errorMessage').get_text()

    if totalCount == '0':
        getXYcoordinate_empty.append({
            'management_key': management_key,
            'management_number': management_number,
            'sido_name': sido_name,
            'full_load_address': load_address,
            'admCd': admCd,
            'rnMgtSn': rnMgtSn,
            'udrtYn': udrtYn,
            'buldMnnm': buldMnnm,
            'buldSlno': buldSlno,
            'errorCode': errorCode,
            'errorMessage': errorMessage,
            'update_month': update_month
        })

    elif totalCount != '0' and errorCode == '0':
        bdMgtSn = soup.find('bdMgtSn').get_text()  # 건물관리번호
        entX = soup.find('entX').get_text()  # X좌표(UTM-K/GRS80[EPSG:5179])
        entY = soup.find('entY').get_text()  # Y좌표(UTM-K/GRS80[EPSG:5179])
        bdNm = soup.find('bdNm').get_text()  # 건물명

        # 좌표계 정의
        entUTMK = Proj(init='epsg:5179')  # UTM-K(GRS80) 주소별 좌표
        entWGS84 = Proj(init='epsg:4326')  # WGS84 경도/위도, GPS 사용 전지구 좌표

        WGS84_X, WGS84_Y = transform(entUTMK, entWGS84, entX, entY)

        getXYcoordinate_data.append({
            'management_key': management_key,
            'management_number': management_number,
            'sido_name': sido_name,
            'full_load_address': load_address,
            'admCd': admCd,
            'rnMgtSn': rnMgtSn,
            'udrtYn': udrtYn,
            'buldMnnm': buldMnnm,
            'buldSlno': buldSlno,
            'bdMgtSn': bdMgtSn,
            'bdNm': bdNm,
            'entX': entX,
            'entY': entY,
            'longitude': WGS84_X,
            'latitude': WGS84_Y,
            'update_month': update_month,
        })

if __name__ == '__main__':

    engine = create_engine(f'mariadb+pymysql://{config.user}:{config.pwd}@{config.host}:3306/{config.db}', echo=False)

    limit_value = 100
    address_table = 'zz_all'

    while True:
        update_month = time.strftime('%y%m')
        conn = engine.connect()

        getXYcoordinate_sql = text('''
            select  distinct management_key, 
                    management_number, 
                    sido_name,
                    full_load_address, 
                    beopdong_code as admCd, 
                    load_code as rnMgtSn, 
                    underground_flag as udrtYn, 
                    build_1st_number as buldMnnm, 
                    build_2nd_number as buldSlno 
            from {address_table}
            where management_number not in (select management_number from getXYcoordinate)
              and management_number not in (select management_number from getXYcoordinate_empty)
            limit {limit_value}
            '''.format(address_table=address_table, limit_value=limit_value))

        getXYcoordinate_sql_df = pd.read_sql_query(getXYcoordinate_sql, conn)
        global getXYcoordinate_data, getXYcoordinate_empty
        getXYcoordinate_data = []
        getXYcoordinate_empty = []

        for process_cnt in tqdm(range(0, limit_value), total=limit_value, desc='프로세스 진행률', ncols=100, ascii=' =', leave=True):
            try:
                getXYcoordinate(
                    getXYcoordinate_sql_df['management_key'][process_cnt],
                    getXYcoordinate_sql_df['management_number'][process_cnt],
                    getXYcoordinate_sql_df['sido_name'][process_cnt],
                    getXYcoordinate_sql_df['full_load_address'][process_cnt],
                    getXYcoordinate_sql_df['admCd'][process_cnt],
                    getXYcoordinate_sql_df['rnMgtSn'][process_cnt],
                    getXYcoordinate_sql_df['udrtYn'][process_cnt],
                    getXYcoordinate_sql_df['buldMnnm'][process_cnt],
                    getXYcoordinate_sql_df['buldSlno'][process_cnt],
                )
            except Exception as e:
                pass

            time.sleep(delay)

        getXYcoordinate_data_df = pd.DataFrame(getXYcoordinate_data)
        getXYcoordinate_empty_df = pd.DataFrame(getXYcoordinate_empty)

        # Bulk Data DB 업로드 속도 개선 기능
        @event.listens_for(engine, "before_cursor_execute")
        def receive_before_cursor_execute(
                conn, cursor, statement, params, context, executemany
        ):
            if executemany:
                cursor.fast_executemany = True

        getXYcoordinate_data_df.to_sql('getXYcoordinate', engine, index=False, if_exists="append", chunksize=100000)
        getXYcoordinate_empty_df.to_sql('getXYcoordinate_empty', engine, index=False, if_exists="append", chunksize=100000)