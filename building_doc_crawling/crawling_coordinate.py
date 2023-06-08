from tqdm import tqdm
import pandas as pd
import openpyxl
from sqlalchemy import create_engine, text
from sqlalchemy import event
import sqlalchemy
import requests as re
import config
import time
from bs4 import BeautifulSoup as bs
from pyproj import Proj, transform  # 좌표계 변환 라이브러리

# FutureWarning 제거(사용 모듈의 업데이트 시 의도대로 작동하지 않을 가능성이 있음을 알리는 경고)
import warnings

warnings.simplefilter(action='ignore', category=FutureWarning)


def geocode_parse(df, process_cnt):
    management_number = df['management_number'][process_cnt]
    sido_name = df['sido_name'][process_cnt]
    load_address = df['full_load_address'][process_cnt]
    admCd = df['admCd'][process_cnt]
    rnMgtSn = df['rnMgtSn'][process_cnt]
    udrtYn = df['udrtYn'][process_cnt]
    buldMnnm = df['buldMnnm'][process_cnt]
    buldSlno = df['buldSlno'][process_cnt]

    params = {'admCd': admCd, 'rnMgtSn': rnMgtSn, 'udrtYn': udrtYn, 'buldMnnm': buldMnnm,
              'buldSlno': buldSlno, 'confmKey': config.geocode_key}
    url = 'https://business.juso.go.kr/addrlink/addrCoordApi.do'
    res = re.get(url, params=params)
    soup = bs(res.text, features='xml')

    totalCount = soup.find('totalCount').get_text()
    errorCode = soup.find('errorCode').get_text()
    errorMessage = soup.find('errorMessage').get_text()

    if totalCount != '0' and errorCode == '0':
        bdMgtSn = soup.find('bdMgtSn').get_text()  # 건물관리번호
        entX = soup.find('entX').get_text()  # X좌표(UTM-K/GRS80[EPSG:5179])
        entY = soup.find('entY').get_text()  # Y좌표(UTM-K/GRS80[EPSG:5179])
        bdNm = soup.find('bdNm').get_text()  # 건물명

        # 좌표계 정의
        entUTMK = Proj(init='epsg:5179')  # UTM-K(GRS80) 주소별 좌표
        entWGS84 = Proj(init='epsg:4326')  # WGS84 경도/위도, GPS 사용 전지구 좌표

        WGS84_X, WGS84_Y = transform(entUTMK, entWGS84, entX, entY)

    elif errorCode == '-999':
        print(load_address, '-', errorMessage)
    elif errorCode == 'E0001':
        print(load_address, '-', errorMessage)
    elif errorCode == 'E0002':
        print(load_address, '-', errorMessage)
    elif errorCode == 'E0003':
        print(load_address, '-', errorMessage)
    elif errorCode == 'E0004':
        print(load_address, '-', errorMessage)
    elif errorCode == 'E0005':
        print(load_address, '-', errorMessage)
    elif errorCode == 'E0006':
        print(load_address, '-', errorMessage)
    elif errorCode == 'E0007':
        print(load_address, '-', errorMessage)
    else:
        print(load_address, '-', errorCode, ": ", errorMessage)

    return {
        'management_number': management_number,
        'sido_name': sido_name,
        'full_load_address': load_address,
        'bdMgtSn': bdMgtSn,
        'bdNm': bdNm,
        'entX': entX,
        'entY': entY,
        'longitude': WGS84_X,
        'latitude': WGS84_Y
    }

if __name__ == '__main__':

engine = create_engine(f'mariadb+pymysql://{config.user}:{config.pwd}@{config.host}:3306/{config.db}', echo=False)
conn = engine.connect()
while True:
    try:
        sql = text('''
                select  distinct management_number, 
                        sido_name,
                        apartment_flag, 
                        delivery_name, 
                        document_build_name, 
                        sigungu_build_name, 
                        full_load_address, 
                        beopdong_code as admCd, 
                        load_code as rnMgtSn, 
                        underground_flag as udrtYn, 
                        build_1st_number as buldMnnm, 
                        build_2nd_number as buldSlno 
                from zz_all
                where management_number not in (select management_number from zz_coordinate) 
                
                limit 10000
                ''')

        df = pd.read_sql_query(sql, conn)
        coordinate = []  # 좌표

        for process_cnt in tqdm(range(0, len(df['full_load_address'])), total=len(df['full_load_address']), desc='좌표획득 진행율', ncols=100, ascii=' =', leave=True):
            coordinate.append(geocode_parse(df, process_cnt))

        coordinate_df = pd.DataFrame(coordinate)

        # Bulk Data DB 업로드 속도 개선 기능
        @event.listens_for(engine, "before_cursor_execute")
        def receive_before_cursor_execute(
                conn, cursor, statement, params, context, executemany
        ):
            if executemany:
                cursor.fast_executemany = True

        coordinate_df.to_sql('zz_coordinate', engine, index=False, if_exists="append", chunksize=100000)

    except Exception as e:
        print('좌표획득 프로세스 오류: ', e)
        pass