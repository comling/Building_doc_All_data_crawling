from tqdm import tqdm
import pandas as pd
import openpyxl
from sqlalchemy import create_engine, text
from sqlalchemy import event
import sqlalchemy
from PyKakao import KakaoLocal
import requests as re
import config
import time
from bs4 import BeautifulSoup as bs
from pyproj import Proj, transform  # 좌표계 변환 라이브러리

csv_flag = 0


def kakao_address(address, management_number):
    # 카카오로컬API 라이브러리를 사용한 주소 검색 조회
    KL = KakaoLocal(config.KAKAO_REST_API_KEY)

    # 카카오 API 로컬주소 확인
    address_get = KL.search_address(address)
    # 주소 정보 저장
    result = []

    try:
        result.append(address_get['documents'][0]['address']['address_name'])
        result.append(address_get['documents'][0]['road_address']['address_name'])
        result.append(address_get['documents'][0]['address']['region_3depth_h_name'])  # 읍면동 단위 parsing
        result.append(address_get['documents'][0]['address']['region_3depth_name'])  # 읍면동 + 리 단위 parsing
        result.append(address_get['documents'][0]['address']['b_code'][0:5])
        result.append(address_get['documents'][0]['address']['b_code'][5:10])
        if address_get['documents'][0]['address']['mountain_yn'] == 'N':
            result.append('0')
        elif address_get['documents'][0]['address']['mountain_yn'] == 'Y':
            result.append('1')
        result.append(address_get['documents'][0]['address']['main_address_no'].zfill(4))
        result.append(address_get['documents'][0]['address']['sub_address_no'].zfill(4))
        result.append(address_get['documents'][0]['address']['x'])
        result.append(address_get['documents'][0]['address']['y'])

        return result

    except Exception as e:
        print(e, '카카오맵 주소 정보 오류: ', address)
        KakaoError_data.append({'management_number': management_number,
            'full_load_address': address})


def OwnerAllInfoParse(soup, management_number, params):
    try:
        last_month = time.strftime('%y%m')
        mgm_bldrgst_pk = soup.find('mgm_bldrgst_pk').get_text().strip()  # 관리건축물대장PK
        sigungu_cd = soup.find('sigungu_cd').get_text().strip()  # 시군구코드
        sigungu_nm = soup.find('sigungu_nm').get_text().strip()  # 시군구명
        bjdong_cd = soup.find('bjdong_cd').get_text().strip()  # 법정동코드
        bjdong_nm = soup.find('bjdong_nm').get_text().strip()  # 법정동명
        plat_gb_cd = soup.find('plat_gb_cd').get_text().strip()  # 대지구분코드
        plat_gb_nm = soup.find('plat_gb_nm').get_text().strip()  # 대지구분명
        bun = soup.find('bun').get_text().strip()  # 번
        ji = soup.find('ji').get_text().strip()  # 지
        splot_nm = soup.find('splot_nm').get_text().strip()  # 특수지명
        block = soup.find('block').get_text().strip()  # 블록
        lot = soup.find('lot').get_text().strip()  # 로트
        na_plat_plc = soup.find('na_plat_plc').get_text().strip()  # 새주소대지위치
        na_road_cd = soup.find('na_road_cd').get_text().strip()  # 새주소도로코드
        na_bjdong_cd = soup.find('na_bjdong_cd').get_text().strip()  # 새주소법정동코드
        na_ugrnd_cd = soup.find('na_ugrnd_cd').get_text().strip()  # 새주소지상지하코드
        na_ugrnd_nm = soup.find('na_ugrnd_nm').get_text().strip()  # 새주소지상지하명
        na_main_bun = soup.find('na_main_bun').get_text().strip()  # 새주소본번
        na_sub_bun = soup.find('na_sub_bun').get_text().strip()  # 새주소부번
        regstr_gb_cd = soup.find('regstr_gb_cd').get_text().strip()  # 대장구분코드
        regstr_gb_nm = soup.find('regstr_gb_nm').get_text().strip()  # 대장구분명
        regstr_kind_cd = soup.find('regstr_kind_cd').get_text().strip()  # 대장종류코드
        regstr_kind_nm = soup.find('regstr_kind_nm').get_text().strip()  # 대장종류명
        bld_nm = soup.find('bld_nm').get_text().strip()  # 건물명
        dong_nm = soup.find('dong_nm').get_text().strip()  # 동명칭
        ho_nm = soup.find('ho_nm').get_text().strip()  # 호명칭
        area = soup.find('area').get_text().strip()  # 면적
        own_gb_cd = soup.find('own_gb_cd').get_text().strip()  # 소유구분코드
        own_gb_nm = soup.find('own_gb_nm').get_text().strip()  # 소유구분명
        jm_gb_cd = soup.find('jm_gb_cd').get_text().strip()  # 주민구분코드
        jm_gb_nm = soup.find('jm_gb_nm').get_text().strip()  # 주민구분명
        nm = soup.find('nm').get_text().strip()  # 성명
        jmno = soup.find('jmno').get_text().strip()  # 주민번호
        quota1 = soup.find('quota1').get_text().strip()  # 지분1
        quota2 = soup.find('quota2').get_text().strip()  # 지분2
        ownsh_quota = soup.find('ownsh_quota').get_text().strip()  # 소유권지분
        chang_caus_day = soup.find('chang_caus_day').get_text().strip()  # 변동원인일
        loc_sigungu_cd = soup.find('loc_sigungu_cd').get_text().strip()  # 소재지시군구코드
        loc_sigungu_nm = soup.find('loc_sigungu_nm').get_text().strip()  # 소재지시군구명
        loc_bjdong_cd = soup.find('loc_bjdong_cd').get_text().strip()  # 소재지법정동코드
        loc_bjdong_nm = soup.find('loc_bjdong_nm').get_text().strip()  # 소재지법정동명
        loc_detl_addr = soup.find('loc_detl_addr').get_text().strip()  # 소재지상세주소
        na_loc_plat_plc = soup.find('na_loc_plat_plc').get_text().strip()  # 새주소소재지대지위치
        na_loc_road_cd = soup.find('na_loc_road_cd').get_text().strip()  # 새주소소재지도로코드
        na_loc_bjdong_cd = soup.find('na_loc_bjdong_cd').get_text().strip()  # 새주소소재지법정동코드
        na_loc_detl_addr = soup.find('na_loc_detl_addr').get_text().strip()  # 새주소소재지상세주소
        na_loc_ugrnd_cd = soup.find('na_loc_ugrnd_cd').get_text().strip()  # 새주소소재지지상지하코드
        na_loc_ugrnd_nm = soup.find('na_loc_ugrnd_nm').get_text().strip()  # 새주소소재지지상지하명
        na_loc_main_bun = soup.find('na_loc_main_bun').get_text().strip()  # 새주소소재지본번
        na_loc_sub_bun = soup.find('na_loc_sub_bun').get_text().strip()  # 새주소소재지부번
        numOfRows = soup.find('numOfRows').get_text().strip()  # 새주소소재지부번
        pageNo = soup.find('pageNo').get_text().strip()  # 페이지 번호
        totalCount = soup.find('totalCount').get_text().strip()  # 총 갯수

        return {
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
            'lastUpdate': last_month
        }
    except Exception as e:
        print('소유자정보 전체 조회에서 에러발생: ', e)
        pass

def OwnerInfoParse(management_number, sido_name, apartment_flag, delivery_name, document_build_name, sigungu_build_name,
                   target, parameter, road_address, soup, totalCount, region_3depth_h, region_3depth, longitude,
                   latitude, sigungu_cd, bjdong_cd, plat_gb_cd, bun, ji):
    try:
        mgm_bldrgst_pk = soup.find('mgm_bldrgst_pk').get_text().strip()  # 고유번호
        regstr_gb_cd = soup.find('regstr_gb_cd').get_text().strip()  # 대장구분코드
        regstr_gb_nm = soup.find('regstr_gb_nm').get_text().strip()  # 대장구분명
        regstr_kind_cd = soup.find('regstr_kind_cd').get_text().strip()  # 대장종류코드
        regstr_kind_nm = soup.find('regstr_kind_nm').get_text().strip()  # 대장종류명
        bld_nm = soup.find('bld_nm').get_text().strip()  # 건물명
        dong_nm = soup.find('dong_nm').get_text().strip()  # 동명칭
        ho_nm = soup.find('ho_nm').get_text().strip()  # 호명칭
        area = soup.find('area').get_text().strip()  # 면적
        own_gb_cd = soup.find('own_gb_cd').get_text().strip()  # 소유구분코드
        own_gb_nm = soup.find('own_gb_nm').get_text().strip()  # 소유구분명
        jm_gb_cd = soup.find('jm_gb_cd').get_text().strip()  # 주민구분코드(1, 2)
        jm_gb_nm = soup.find('jm_gb_nm').get_text().strip()  # 주민구분명(내국인, 외국인)
        nm = soup.find('nm').get_text().strip()  # 소유자명 (최**)
        ownsh_quota = soup.find('ownsh_quota').get_text().strip()  # 소유권지분
        chang_caus_day = soup.find('chang_caus_day').get_text().strip()  # 변동원인일

        last_month = time.strftime('%y%m')

        return {
            'management_number': management_number,
            'sido_name': sido_name,
            'full_load_address': target,
            '지역_읍면동': region_3depth_h,
            '지역_리': region_3depth,
            '등기갯수': totalCount,
            '지번주소': parameter,
            '도로명주소': road_address,
            '고유번호': mgm_bldrgst_pk,
            '대장구분코드': regstr_gb_cd,
            '대장구분명': regstr_gb_nm,
            '대장종류코드': regstr_kind_cd,
            '대장종류명': regstr_kind_nm,
            '배달처명': delivery_name,
            '건축물대장건물명': document_build_name,
            '시군구건물명': sigungu_build_name,
            '건물명': bld_nm,
            '동명칭': dong_nm,
            '호명칭': ho_nm,
            '면적': area,
            '소유구분코드': own_gb_cd,
            '소유구분명': own_gb_nm,
            '주민구분코드': jm_gb_cd,
            '주민구분명': jm_gb_nm,
            '소유자명': nm,
            '소유권지분': ownsh_quota,
            '변동원인일': chang_caus_day,
            '위도': latitude,
            '경도': longitude,
            'PNU': sigungu_cd + bjdong_cd + plat_gb_cd + bun + ji,
            '주소': target,
            '좌표': latitude + ',' + longitude,
            '공동주택여부': apartment_flag,
            '업데이트월': last_month,
        }

    except Exception as e:
        print('검색된 주소에서 오류가 발생하였습니다.\n', '주소 결과: ', target)
        print('에러코드: ', e)
        pass

def search_process(management_number, sido_name, apartment_flag, delivery_name, document_build_name, sigungu_build_name,
                   address_process_list):
    try:

        address_temp = address_process_list
        parameter = kakao_address(address_process_list, management_number)

        address_name = parameter[0]
        road_address_name = parameter[1]
        region_3depth_h = parameter[2]
        region_3depth = parameter[3]
        sigungu_cd = parameter[4]
        bjdong_cd = parameter[5]
        plat_gb_cd = parameter[6]
        bun = parameter[7]
        ji = parameter[8]
        longitude = parameter[9]
        latitude = parameter[10]

        if parameter[1] != "":

            params = {'sigungu_cd': sigungu_cd, 'bjdong_cd': bjdong_cd, 'plat_gb_cd': plat_gb_cd, 'bun': bun,
                      'ji': ji, 'numOfRows': '1', 'pageNo': '1', 'ServiceKey': config.molit_API_key}
            url = 'http://apis.data.go.kr/1611000/OwnerInfoService/getArchitecturePossessionInfo'
            res = re.get(url, params=params)
            soup = bs(res.text, features='xml')

            totalCount = soup.find('totalCount').get_text()

            if totalCount != '0':
                # 건축물대장 첫번째만 저장
                building_doc_data.append(
                    OwnerInfoParse(management_number, sido_name, apartment_flag, delivery_name, document_build_name,
                                   sigungu_build_name, address_temp, address_name, road_address_name, soup, totalCount,
                                   region_3depth_h, region_3depth, longitude,
                                   latitude, sigungu_cd, bjdong_cd, plat_gb_cd, bun, ji))

                # # 건축물대장 전체를 pageNo 순서대로 저장
                # for i in range(1, int(totalCount) + 1):
                #     params2 = {'sigungu_cd': sigungu_cd, 'bjdong_cd': bjdong_cd, 'bun': bun,
                #                'ji': ji, 'numOfRows': '1', 'pageNo': i, 'ServiceKey': config.molit_API_key}
                #     url2 = 'http://apis.data.go.kr/1611000/OwnerInfoService/getArchitecturePossessionInfo'
                #     res2 = re.get(url2, params=params2)
                #     soup2 = bs(res2.text, features='xml')
                #     try:
                #         OwnerAllInfo.append(OwnerAllInfoParse(soup2, management_number, params2))
                #         time.sleep(0.05)  # API 조회 시 Ddos 공격 오인 방지용 딜레이 추가
                #
                #     except Exception as e:
                #         print('건축물대장 전체 조회 오류: ', e)
                #         pass

            elif totalCount == '0':
                building_doc_empty.append({'management_number': management_number,
            'full_load_address': address_process_list})


    except TimeoutError as e:
        time.sleep(0.1)
        print('\n원격 호스트 연결이 끊겼습니다. 30초 후 재접속을 시도합니다.', e, (address_process_list))
        time.sleep(30)
        pass

    except ConnectionError as e:
        time.sleep(0.1)
        print('\n호스트로부터 응답이 없어 연결이 끊어졌습니다. 30초 후 재접속을 시도합니다.', e, (address_process_list))
        time.sleep(30)
        pass
    except Exception as e:
        time.sleep(0.1)
        print('\n오류: 주소 조회에 실패하였습니다. API 연결 오류, 조회 대상 주소 오류 등의 문제가 있습니다.', e, (address_process_list))
        pass


if __name__ == '__main__':
    sql_data = ''
    host = 'sundakorea.synology.me'
    user = 'sundakorea'
    pwd = '#Jhyun08027'
    db = 'Address'
    engine = create_engine(f'mariadb+pymysql://{user}:{pwd}@{host}:3306/{db}', echo=False)

    while True:
        global building_doc_data
        building_doc_data = []
        global OwnerAllInfo
        OwnerAllInfo = []
        global KakaoError_data
        KakaoError_data = []
        global building_doc_empty
        building_doc_empty = []
        search_data = []
        conn = engine.connect()

        sql = text(
            'select management_number, sido_name, apartment_flag, delivery_name, document_build_name, sigungu_build_name, full_load_address from zz_building_doc_final where management_number not in (select management_number from zz_OwnerInfo) and management_number not in (select management_number from zz_KakaoError) and management_number not in (select management_number from zz_building_doc_empty)  limit 100')

        df = pd.read_sql_query(sql, conn)

        for add_cnt in tqdm(range(0, len(df['full_load_address'])), total=len(df['full_load_address']), desc='진행율', ncols=100, ascii=' =', leave=True):
            try:
                search_process(df['management_number'][add_cnt], df['sido_name'][add_cnt], df['apartment_flag'][add_cnt],
                               df['delivery_name'][add_cnt], df['document_build_name'][add_cnt],
                               df['sigungu_build_name'][add_cnt], df['full_load_address'][add_cnt])
            except Exception as e:
                print('오류발생: ', e)
                pass
        OwnerInfo_df = pd.DataFrame(building_doc_data)
        # OwnerAllInfo_df = pd.DataFrame(OwnerAllInfo)
        OwnerAllInfo_df = '0'
        KakaoError_df = pd.DataFrame(KakaoError_data)
        building_doc_empty_df = pd.DataFrame(building_doc_empty)
        print('OwnerInfo_df: ', str(len(OwnerInfo_df)), 'OwnerAllInfo:', str(len(OwnerAllInfo_df)), 'KakaoError: ', str(len(KakaoError_df)), 'building_doc_empty: ', str(len(building_doc_empty_df)))


        @event.listens_for(engine, "before_cursor_execute")
        def receive_before_cursor_execute(
                conn, cursor, statement, params, context, executemany
        ):
            if executemany:
                cursor.fast_executemany = True


        OwnerInfo_df.to_sql('zz_OwnerInfo', engine, index=False, if_exists="append", chunksize=100000)
        # OwnerAllInfo_df.to_sql('zz_OwnerInfoAll', engine, index=False, if_exists="append", chunksize=100000)
        KakaoError_df.to_sql('zz_KakaoError', engine, index=False, if_exists="append", chunksize=100000)
        building_doc_empty_df.to_sql('zz_building_doc_empty', engine, index=False, if_exists="append", chunksize=100000)