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

opertaion_list = ['getBrBasisOulnInfo', # 건축물대장 기본개요 조회
                'getBrRecapTitleInfo', # 건축물대장 총괄표제부 조회
                'getBrTitleInfo', # 건축물대장 표제부 조회
                'getBrFlrOulnInfo', # 건축물대장 층별개요 조회
                'getBrExposPubuseAreaInfo', # 건축물대장 전유공용면적 조회
                 ]

def getBrExposPubuseAreaInfo(management_key, management_number, sigungu_cd, bjdong_cd, bun, ji):  # 건축물대장 전유공용면적 조회
    params = {'sigunguCd': sigungu_cd, 'bjdongCd': bjdong_cd, 'bun': bun,
              'ji': ji, 'numOfRows': '1', 'pageNo': '1', 'ServiceKey': config.molit_API_key}
    url = 'http://apis.data.go.kr/1613000/BldRgstService_v2/' + opertaion_list[4]
    res = re.get(url, params=params)
    soup = bs(res.text, features='xml')

    totalCount = soup.find('totalCount').get_text().strip()  # 전체갯수

    if totalCount == '0':
        getBrExposPubuseAreaInfo_empty.append({
            'management_key': management_key,
            'management_number': management_number,
            'sigungu_cd': sigungu_cd,
            'bjdong_cd': bjdong_cd,
            'bun': bun,
            'ji': ji,
            'update_month': update_month
        })
    elif totalCount != '0':
        rnum = soup.find('rnum').get_text().strip()	# 순번
        platPlc = soup.find('platPlc').get_text().strip()	# 대지위치
        sigunguCd = soup.find('sigunguCd').get_text().strip()	# 시군구코드
        bjdongCd = soup.find('bjdongCd').get_text().strip()	# 법정동코드
        platGbCd = soup.find('platGbCd').get_text().strip()	# 대지구분코드
        bun = soup.find('bun').get_text().strip()	# 번
        ji = soup.find('ji').get_text().strip()	# 지
        mgmBldrgstPk = soup.find('mgmBldrgstPk').get_text().strip()	# 관리건축물대장PK
        regstrGbCd = soup.find('regstrGbCd').get_text().strip()	# 대장구분코드
        regstrGbCdNm = soup.find('regstrGbCdNm').get_text().strip()	# 대장구분코드명
        regstrKindCd = soup.find('regstrKindCd').get_text().strip()	# 대장종류코드
        regstrKindCdNm = soup.find('regstrKindCdNm').get_text().strip()	# 대장종류코드명
        newPlatPlc = soup.find('newPlatPlc').get_text().strip()	# 도로명대지위치
        bldNm = soup.find('bldNm').get_text().strip()	# 건물명
        splotNm = soup.find('splotNm').get_text().strip()	# 특수지명
        block = soup.find('block').get_text().strip()	# 블록
        lot = soup.find('lot').get_text().strip()	# 로트
        naRoadCd = soup.find('naRoadCd').get_text().strip()	# 새주소도로코드
        naBjdongCd = soup.find('naBjdongCd').get_text().strip()	# 새주소법정동코드
        naUgrndCd = soup.find('naUgrndCd').get_text().strip()	# 새주소지상지하코드
        naMainBun = soup.find('naMainBun').get_text().strip()	# 새주소본번
        naSubBun = soup.find('naSubBun').get_text().strip()	# 새주소부번
        dongNm = soup.find('dongNm').get_text().strip()	# 동명칭
        hoNm = soup.find('hoNm').get_text().strip()	# 호명칭
        flrGbCd = soup.find('flrGbCd').get_text().strip()	# 층구분코드
        flrGbCdNm = soup.find('flrGbCdNm').get_text().strip()	# 층구분코드명
        flrNo = soup.find('flrNo').get_text().strip()	# 층번호
        flrNoNm = soup.find('flrNoNm').get_text().strip()	# 층번호명
        exposPubuseGbCd = soup.find('exposPubuseGbCd').get_text().strip()	# 전유공용구분코드
        exposPubuseGbCdNm = soup.find('exposPubuseGbCdNm').get_text().strip()	# 전유공용구분코드명
        mainAtchGbCd = soup.find('mainAtchGbCd').get_text().strip()	# 주부속구분코드
        mainAtchGbCdNm = soup.find('mainAtchGbCdNm').get_text().strip()	# 주부속구분코드명
        strctCd = soup.find('strctCd').get_text().strip()	# 구조코드
        strctCdNm = soup.find('strctCdNm').get_text().strip()	# 구조코드명
        etcStrct = soup.find('etcStrct').get_text().strip()	# 기타구조
        mainPurpsCd = soup.find('mainPurpsCd').get_text().strip()	# 주용도코드
        mainPurpsCdNm = soup.find('mainPurpsCdNm').get_text().strip()	# 주용도코드명
        etcPurps = soup.find('etcPurps').get_text().strip()	# 기타용도
        area = soup.find('area').get_text().strip()	# 면적(㎡)
        crtnDay = soup.find('crtnDay').get_text().strip()	# 생성일자
        totalCount = soup.find('totalCount').get_text().strip()	# 전체갯수

        getBrExposPubuseAreaInfo_data.append({
            'management_key': management_key,
            'management_number': management_number,
            'rnum': rnum,
            'platPlc': platPlc,
            'sigunguCd': sigunguCd,
            'bjdongCd': bjdongCd,
            'platGbCd': platGbCd,
            'bun': bun,
            'ji': ji,
            'mgmBldrgstPk': mgmBldrgstPk,
            'regstrGbCd': regstrGbCd,
            'regstrGbCdNm': regstrGbCdNm,
            'regstrKindCd': regstrKindCd,
            'regstrKindCdNm': regstrKindCdNm,
            'newPlatPlc': newPlatPlc,
            'bldNm': bldNm,
            'splotNm': splotNm,
            'block': block,
            'lot': lot,
            'naRoadCd': naRoadCd,
            'naBjdongCd': naBjdongCd,
            'naUgrndCd': naUgrndCd,
            'naMainBun': naMainBun,
            'naSubBun': naSubBun,
            'dongNm': dongNm,
            'hoNm': hoNm,
            'flrGbCd': flrGbCd,
            'flrGbCdNm': flrGbCdNm,
            'flrNo': flrNo,
            'flrNoNm': flrNoNm,
            'exposPubuseGbCd': exposPubuseGbCd,
            'exposPubuseGbCdNm': exposPubuseGbCdNm,
            'mainAtchGbCd': mainAtchGbCd,
            'mainAtchGbCdNm': mainAtchGbCdNm,
            'strctCd': strctCd,
            'strctCdNm': strctCdNm,
            'etcStrct': etcStrct,
            'mainPurpsCd': mainPurpsCd,
            'mainPurpsCdNm': mainPurpsCdNm,
            'etcPurps': etcPurps,
            'area': area,
            'crtnDay': crtnDay,
            'totalCount': totalCount,
            'update_month': update_month,
        })

if __name__ == '__main__':

    engine = create_engine(f'mariadb+pymysql://{config.user}:{config.pwd}@{config.host}:3306/{config.db}', echo=False)

    limit_value = 100
    address_table = 'zz_all'

    while True:
        update_month = time.strftime('%y%m')
        conn = engine.connect()

        getBrExposPubuseAreaInfo_sql = text('''
                    select  distinct management_key, 
                            management_number, 
                            left(beopdong_code,5) as sigungu_cd, 
                            right(beopdong_code,5) as bjdong_cd, 
                            lpad(jibun_1st_number, 4, '0') as bun, 
                            lpad(jibun_2nd_number, 4, '0') as ji 
                    from {address_table}
                    where management_number not in (select management_number from getBrExposPubuseAreaInfo)
                      and management_number not in (select management_number from getBrExposPubuseAreaInfo_empty)
                    limit {limit_value}
                    '''.format(address_table=address_table, limit_value=limit_value))

        getBrExposPubuseAreaInfo_sql_df = pd.read_sql_query(getBrExposPubuseAreaInfo_sql, conn)
        global getBrExposPubuseAreaInfo_data, getBrExposPubuseAreaInfo_empty
        getBrExposPubuseAreaInfo_data = []
        getBrExposPubuseAreaInfo_empty = []

        for process_cnt in tqdm(range(0, limit_value), total=limit_value, desc='프로세스 진행률', leave=True):
            try:
                getBrExposPubuseAreaInfo(
                    getBrExposPubuseAreaInfo_sql_df['management_key'][process_cnt],
                    getBrExposPubuseAreaInfo_sql_df['management_number'][process_cnt],
                    getBrExposPubuseAreaInfo_sql_df['sigungu_cd'][process_cnt],
                    getBrExposPubuseAreaInfo_sql_df['bjdong_cd'][process_cnt],
                    getBrExposPubuseAreaInfo_sql_df['bun'][process_cnt],
                    getBrExposPubuseAreaInfo_sql_df['ji'][process_cnt],
                )
            except Exception as e:
                pass

        getBrExposPubuseAreaInfo_data_df = pd.DataFrame(getBrExposPubuseAreaInfo_data)
        getBrExposPubuseAreaInfo_empty_df = pd.DataFrame(getBrExposPubuseAreaInfo_empty)


        # Bulk Data DB 업로드 속도 개선 기능
        @event.listens_for(engine, "before_cursor_execute")
        def receive_before_cursor_execute(
                conn, cursor, statement, params, context, executemany
        ):
            if executemany:
                cursor.fast_executemany = True


        getBrExposPubuseAreaInfo_data_df.to_sql('getBrExposPubuseAreaInfo', engine, index=False, if_exists="append", chunksize=100000)
        getBrExposPubuseAreaInfo_empty_df.to_sql('getBrExposPubuseAreaInfo_empty', engine, index=False, if_exists="append", chunksize=100000)