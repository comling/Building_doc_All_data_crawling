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


import warnings
warnings.simplefilter(action='ignore', category=FutureWarning)

def getArchitecturePossessionInfo(management_key, management_number, sigungu_cd, bjdong_cd, bun, ji):   # 건축물대장소유자정보조회
    params = {'sigungu_cd': sigungu_cd, 'bjdong_cd': bjdong_cd, 'bun': bun,
              'ji': ji, 'numOfRows': '1', 'pageNo': '1', 'ServiceKey': config.molit_API_key}
    url = 'http://apis.data.go.kr/1611000/OwnerInfoService/getArchitecturePossessionInfo'
    res = re.get(url, params=params)
    soup = bs(res.text, features='xml')

    totalCount = soup.find('totalCount').get_text().strip()  # 전체갯수

    if totalCount == '0':
        getArchitecturePossessionInfo_empty.append({
            'management_key': management_key,
            'management_number': management_number,
            'sigungu_cd': sigungu_cd,
            'bjdong_cd': bjdong_cd,
            'bun': bun,
            'ji': ji,
            'update_month': update_month
        })
    elif totalCount != '0':
        mgm_bldrgst_pk = soup.find('mgm_bldrgst_pk').get_text().strip()	# 관리건축물대장PK
        sigungu_cd = soup.find('sigungu_cd').get_text().strip()	# 시군구코드
        sigungu_nm = soup.find('sigungu_nm').get_text().strip()	# 시군구명
        bjdong_cd = soup.find('bjdong_cd').get_text().strip()	# 법정동코드
        bjdong_nm = soup.find('bjdong_nm').get_text().strip()	# 법정동명
        plat_gb_cd = soup.find('plat_gb_cd').get_text().strip()	# 대지구분코드
        plat_gb_nm = soup.find('plat_gb_nm').get_text().strip()	# 대지구분명
        bun = soup.find('bun').get_text().strip()	# 번
        ji = soup.find('ji').get_text().strip()	# 지
        splot_nm = soup.find('splot_nm').get_text().strip()	# 특수지명
        block = soup.find('block').get_text().strip()	# 블록
        lot = soup.find('lot').get_text().strip()	# 로트
        na_plat_plc = soup.find('na_plat_plc').get_text().strip()	# 새주소대지위치
        na_road_cd = soup.find('na_road_cd').get_text().strip()	# 새주소도로코드
        na_bjdong_cd = soup.find('na_bjdong_cd').get_text().strip()	# 새주소법정동코드
        na_ugrnd_cd = soup.find('na_ugrnd_cd').get_text().strip()	# 새주소지상지하코드
        na_ugrnd_nm = soup.find('na_ugrnd_nm').get_text().strip()	# 새주소지상지하명
        na_main_bun = soup.find('na_main_bun').get_text().strip()	# 새주소본번
        na_sub_bun = soup.find('na_sub_bun').get_text().strip()	# 새주소부번
        regstr_gb_cd = soup.find('regstr_gb_cd').get_text().strip()	# 대장구분코드
        regstr_gb_nm = soup.find('regstr_gb_nm').get_text().strip()	# 대장구분명
        regstr_kind_cd = soup.find('regstr_kind_cd').get_text().strip()	# 대장종류코드
        regstr_kind_nm = soup.find('regstr_kind_nm').get_text().strip()	# 대장종류명
        bld_nm = soup.find('bld_nm').get_text().strip()	# 건물명
        dong_nm = soup.find('dong_nm').get_text().strip()	# 동명칭
        ho_nm = soup.find('ho_nm').get_text().strip()	# 호명칭
        area = soup.find('area').get_text().strip()	# 면적
        own_gb_cd = soup.find('own_gb_cd').get_text().strip()	# 소유구분코드
        own_gb_nm = soup.find('own_gb_nm').get_text().strip()	# 소유구분명
        jm_gb_cd = soup.find('jm_gb_cd').get_text().strip()	# 주민구분코드
        jm_gb_nm = soup.find('jm_gb_nm').get_text().strip()	# 주민구분명
        nm = soup.find('nm').get_text().strip()	# 성명
        jmno = soup.find('jmno').get_text().strip()	# 주민번호
        quota1 = soup.find('quota1').get_text().strip()	# 지분1
        quota2 = soup.find('quota2').get_text().strip()	# 지분2
        ownsh_quota = soup.find('ownsh_quota').get_text().strip()	# 소유권지분
        chang_caus_day = soup.find('chang_caus_day').get_text().strip()	# 변동원인일
        loc_sigungu_cd = soup.find('loc_sigungu_cd').get_text().strip()	# 소재지시군구코드
        loc_sigungu_nm = soup.find('loc_sigungu_nm').get_text().strip()	# 소재지시군구명
        loc_bjdong_cd = soup.find('loc_bjdong_cd').get_text().strip()	# 소재지법정동코드
        loc_bjdong_nm = soup.find('loc_bjdong_nm').get_text().strip()	# 소재지법정동명
        loc_detl_addr = soup.find('loc_detl_addr').get_text().strip()	# 소재지상세주소
        na_loc_plat_plc = soup.find('na_loc_plat_plc').get_text().strip()	# 새주소소재지대지위치
        na_loc_road_cd = soup.find('na_loc_road_cd').get_text().strip()	# 새주소소재지도로코드
        na_loc_bjdong_cd = soup.find('na_loc_bjdong_cd').get_text().strip()	# 새주소소재지법정동코드
        na_loc_detl_addr = soup.find('na_loc_detl_addr').get_text().strip()	# 새주소소재지상세주소
        na_loc_ugrnd_cd = soup.find('na_loc_ugrnd_cd').get_text().strip()	# 새주소소재지지상지하코드
        na_loc_ugrnd_nm = soup.find('na_loc_ugrnd_nm').get_text().strip()	# 새주소소재지지상지하명
        na_loc_main_bun = soup.find('na_loc_main_bun').get_text().strip()	# 새주소소재지본번
        na_loc_sub_bun = soup.find('na_loc_sub_bun').get_text().strip()	# 새주소소재지부번
        numOfRows = soup.find('numOfRows').get_text().strip()	# 새주소소재지부번
        pageNo = soup.find('pageNo').get_text().strip()	# 페이지 번호
        totalCount = soup.find('totalCount').get_text().strip()	# 총 갯수

        getArchitecturePossessionInfo_data.append({
            'management_key': management_key,
            'management_number': management_number,
            'mgm_bldrgst_pk': mgm_bldrgst_pk,
            'sigungu_cd': sigungu_cd,
            'sigungu_nm': sigungu_nm,
            'bjdong_cd': bjdong_cd,
            'bjdong_nm': bjdong_nm,
            'plat_gb_cd': plat_gb_cd,
            'plat_gb_nm': plat_gb_nm,
            'bun': bun,
            'ji': ji,
            'splot_nm': splot_nm,
            'block': block,
            'lot': lot,
            'na_plat_plc': na_plat_plc,
            'na_road_cd': na_road_cd,
            'na_bjdong_cd': na_bjdong_cd,
            'na_ugrnd_cd': na_ugrnd_cd,
            'na_ugrnd_nm': na_ugrnd_nm,
            'na_main_bun': na_main_bun,
            'na_sub_bun': na_sub_bun,
            'regstr_gb_cd': regstr_gb_cd,
            'regstr_gb_nm': regstr_gb_nm,
            'regstr_kind_cd': regstr_kind_cd,
            'regstr_kind_nm': regstr_kind_nm,
            'bld_nm': bld_nm,
            'dong_nm': dong_nm,
            'ho_nm': ho_nm,
            'area': area,
            'own_gb_cd': own_gb_cd,
            'own_gb_nm': own_gb_nm,
            'jm_gb_cd': jm_gb_cd,
            'jm_gb_nm': jm_gb_nm,
            'nm': nm,
            'jmno': jmno,
            'quota1': quota1,
            'quota2': quota2,
            'ownsh_quota': ownsh_quota,
            'chang_caus_day': chang_caus_day,
            'loc_sigungu_cd': loc_sigungu_cd,
            'loc_sigungu_nm': loc_sigungu_nm,
            'loc_bjdong_cd': loc_bjdong_cd,
            'loc_bjdong_nm': loc_bjdong_nm,
            'loc_detl_addr': loc_detl_addr,
            'na_loc_plat_plc': na_loc_plat_plc,
            'na_loc_road_cd': na_loc_road_cd,
            'na_loc_bjdong_cd': na_loc_bjdong_cd,
            'na_loc_detl_addr': na_loc_detl_addr,
            'na_loc_ugrnd_cd': na_loc_ugrnd_cd,
            'na_loc_ugrnd_nm': na_loc_ugrnd_nm,
            'na_loc_main_bun': na_loc_main_bun,
            'na_loc_sub_bun': na_loc_sub_bun,
            'numOfRows': numOfRows,
            'pageNo': pageNo,
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

        getArchitecturePossessionInfo_sql = text('''
                    select  distinct management_key, 
                            management_number, 
                            left(beopdong_code,5) as sigungu_cd, 
                            right(beopdong_code,5) as bjdong_cd, 
                            lpad(jibun_1st_number, 4, '0') as bun, 
                            lpad(jibun_2nd_number, 4, '0') as ji 
                    from {address_table}
                    where management_number not in (select management_number from getArchitecturePossessionInfo)
                      and management_number not in (select management_number from getArchitecturePossessionInfo_empty)
                    limit {limit_value}
                    '''.format(address_table=address_table, limit_value=limit_value))

        getArchitecturePossessionInfo_sql_df = pd.read_sql_query(getArchitecturePossessionInfo_sql, conn)
        global getArchitecturePossessionInfo_data, getArchitecturePossessionInfo_empty
        getArchitecturePossessionInfo_data = []
        getArchitecturePossessionInfo_empty = []

        for process_cnt in tqdm(range(0, limit_value), total=limit_value, desc='프로세스 진행률', leave=True):
            try:
                getArchitecturePossessionInfo(
                    getArchitecturePossessionInfo_sql_df['management_key'][process_cnt],
                    getArchitecturePossessionInfo_sql_df['management_number'][process_cnt],
                    getArchitecturePossessionInfo_sql_df['sigungu_cd'][process_cnt],
                    getArchitecturePossessionInfo_sql_df['bjdong_cd'][process_cnt],
                    getArchitecturePossessionInfo_sql_df['bun'][process_cnt],
                    getArchitecturePossessionInfo_sql_df['ji'][process_cnt],
                )
            except Exception as e:
                print('오류: ', process_cnt, e)
                pass

        getArchitecturePossessionInfo_data_df = pd.DataFrame(getArchitecturePossessionInfo_data)
        getArchitecturePossessionInfo_empty_df = pd.DataFrame(getArchitecturePossessionInfo_empty)


        # Bulk Data DB 업로드 속도 개선 기능
        @event.listens_for(engine, "before_cursor_execute")
        def receive_before_cursor_execute(
                conn, cursor, statement, params, context, executemany
        ):
            if executemany:
                cursor.fast_executemany = True


        getArchitecturePossessionInfo_data_df.to_sql('getArchitecturePossessionInfo', engine, index=False, if_exists="append", chunksize=100000)
        getArchitecturePossessionInfo_empty_df.to_sql('getArchitecturePossessionInfo_empty', engine, index=False, if_exists="append", chunksize=100000)
