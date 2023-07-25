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

delay = 0.05

opertaion_list = ['getBrBasisOulnInfo', # 건축물대장 기본개요 조회
                'getBrRecapTitleInfo', # 건축물대장 총괄표제부 조회
                'getBrTitleInfo', # 건축물대장 표제부 조회
                'getBrFlrOulnInfo', # 건축물대장 층별개요 조회
                'getBrExposPubuseAreaInfo', # 건축물대장 전유공용면적 조회
                 ]

def getXYcoordinate(management_key, management_number, sido_name, load_address, admCd, rnMgtSn, udrtYn, buldMnnm, buldSlno): #주소좌표변환
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
        
def getBrBasisOulnInfo(management_key, management_number, sigungu_cd, bjdong_cd, bun, ji):  # 건축물대장 기본개요 조회
    params = {'sigunguCd': sigungu_cd, 'bjdongCd': bjdong_cd, 'bun': bun,
              'ji': ji, 'numOfRows': '1', 'pageNo': '1', 'ServiceKey': config.molit_API_key}
    url = 'http://apis.data.go.kr/1613000/BldRgstService_v2/' + opertaion_list[0]
    res = re.get(url, params=params)
    soup = bs(res.text, features='xml')

    totalCount = soup.find('totalCount').get_text().strip()  # 전체갯수

    if totalCount == '0':
        getBrBasisOulnInfo_empty.append({
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
        mgmUpBldrgstPk = soup.find('mgmUpBldrgstPk').get_text().strip()	# 관리상위건축물대장PK
        regstrGbCd = soup.find('regstrGbCd').get_text().strip()	# 대장구분코드
        regstrGbCdNm = soup.find('regstrGbCdNm').get_text().strip()	# 대장구분코드명
        regstrKindCd = soup.find('regstrKindCd').get_text().strip()	# 대장종류코드
        regstrKindCdNm = soup.find('regstrKindCdNm').get_text().strip()	# 대장종류코드명
        newPlatPlc = soup.find('newPlatPlc').get_text().strip()	# 도로명대지위치
        bldNm = soup.find('bldNm').get_text().strip()	# 건물명
        splotNm = soup.find('splotNm').get_text().strip()	# 특수지명
        block = soup.find('block').get_text().strip()	# 블록
        lot = soup.find('lot').get_text().strip()	# 로트
        bylotCnt = soup.find('bylotCnt').get_text().strip()	# 외필지수
        naRoadCd = soup.find('naRoadCd').get_text().strip()	# 새주소도로코드
        naBjdongCd = soup.find('naBjdongCd').get_text().strip()	# 새주소법정동코드
        naUgrndCd = soup.find('naUgrndCd').get_text().strip()	# 새주소지상지하코드
        naMainBun = soup.find('naMainBun').get_text().strip()	# 새주소본번
        naSubBun = soup.find('naSubBun').get_text().strip()	# 새주소부번
        jiyukCd = soup.find('jiyukCd').get_text().strip()	# 지역코드
        jiguCd = soup.find('jiguCd').get_text().strip()	# 지구코드
        guyukCd = soup.find('guyukCd').get_text().strip()	# 구역코드
        jiyukCdNm = soup.find('jiyukCdNm').get_text().strip()	# 지역코드명
        jiguCdNm = soup.find('jiguCdNm').get_text().strip()	# 지구코드명
        guyukCdNm = soup.find('guyukCdNm').get_text().strip()	# 구역코드명
        crtnDay = soup.find('crtnDay').get_text().strip()	# 생성일자
        totalCount = soup.find('totalCount').get_text().strip()	# 전체갯수

        getBrBasisOulnInfo_data.append({
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
            'mgmUpBldrgstPk': mgmUpBldrgstPk,
            'regstrGbCd': regstrGbCd,
            'regstrGbCdNm': regstrGbCdNm,
            'regstrKindCd': regstrKindCd,
            'regstrKindCdNm': regstrKindCdNm,
            'newPlatPlc': newPlatPlc,
            'bldNm': bldNm,
            'splotNm': splotNm,
            'block': block,
            'lot': lot,
            'bylotCnt': bylotCnt,
            'naRoadCd': naRoadCd,
            'naBjdongCd': naBjdongCd,
            'naUgrndCd': naUgrndCd,
            'naMainBun': naMainBun,
            'naSubBun': naSubBun,
            'jiyukCd': jiyukCd,
            'jiguCd': jiguCd,
            'guyukCd': guyukCd,
            'jiyukCdNm': jiyukCdNm,
            'jiguCdNm': jiguCdNm,
            'guyukCdNm': guyukCdNm,
            'crtnDay': crtnDay,
            'totalCount': totalCount,
            'update_month': update_month,
        })
def getBrRecapTitleInfo(management_key, management_number, sigungu_cd, bjdong_cd, bun, ji):  # 건축물대장 총괄표제부 조회
    params = {'sigunguCd': sigungu_cd, 'bjdongCd': bjdong_cd, 'bun': bun,
              'ji': ji, 'numOfRows': '1', 'pageNo': '1', 'ServiceKey': config.molit_API_key}
    url = 'http://apis.data.go.kr/1613000/BldRgstService_v2/' + opertaion_list[1]
    res = re.get(url, params=params)
    soup = bs(res.text, features='xml')

    totalCount = soup.find('totalCount').get_text().strip()  # 전체갯수

    if totalCount == '0':
        getBrRecapTitleInfo_empty.append({
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
        newOldRegstrGbCd = soup.find('newOldRegstrGbCd').get_text().strip()	# 신구대장구분코드
        newOldRegstrGbCdNm = soup.find('newOldRegstrGbCdNm').get_text().strip()	# 신구대장구분코드명
        newPlatPlc = soup.find('newPlatPlc').get_text().strip()	# 도로명대지위치
        bldNm = soup.find('bldNm').get_text().strip()	# 건물명
        splotNm = soup.find('splotNm').get_text().strip()	# 특수지명
        block = soup.find('block').get_text().strip()	# 블록
        lot = soup.find('lot').get_text().strip()	# 로트
        bylotCnt = soup.find('bylotCnt').get_text().strip()	# 외필지수
        naRoadCd = soup.find('naRoadCd').get_text().strip()	# 새주소도로코드
        naBjdongCd = soup.find('naBjdongCd').get_text().strip()	# 새주소법정동코드
        naUgrndCd = soup.find('naUgrndCd').get_text().strip()	# 새주소지상지하코드
        naMainBun = soup.find('naMainBun').get_text().strip()	# 새주소본번
        naSubBun = soup.find('naSubBun').get_text().strip()	# 새주소부번
        platArea = soup.find('platArea').get_text().strip()	# 대지면적(㎡)
        archArea = soup.find('archArea').get_text().strip()	# 건축면적(㎡)
        bcRat = soup.find('bcRat').get_text().strip()	# 건폐율(%)
        totArea = soup.find('totArea').get_text().strip()	# 연면적(㎡)
        vlRatEstmTotArea = soup.find('vlRatEstmTotArea').get_text().strip()	# 용적률산정연면적(㎡)
        vlRat = soup.find('vlRat').get_text().strip()	# 용적률(%)
        mainPurpsCd = soup.find('mainPurpsCd').get_text().strip()	# 주용도코드
        mainPurpsCdNm = soup.find('mainPurpsCdNm').get_text().strip()	# 주용도코드명
        etcPurps = soup.find('etcPurps').get_text().strip()	# 기타용도
        hhldCnt = soup.find('hhldCnt').get_text().strip()	# 세대수(세대)
        fmlyCnt = soup.find('fmlyCnt').get_text().strip()	# 가구수(가구)
        mainBldCnt = soup.find('mainBldCnt').get_text().strip()	# 주건축물수
        atchBldCnt = soup.find('atchBldCnt').get_text().strip()	# 부속건축물수
        atchBldArea = soup.find('atchBldArea').get_text().strip()	# 부속건축물면적(㎡)
        totPkngCnt = soup.find('totPkngCnt').get_text().strip()	# 총주차수
        indrMechUtcnt = soup.find('indrMechUtcnt').get_text().strip()	# 옥내기계식대수(대)
        indrMechArea = soup.find('indrMechArea').get_text().strip()	# 옥내기계식면적(㎡)
        oudrMechUtcnt = soup.find('oudrMechUtcnt').get_text().strip()	# 옥외기계식대수(대)
        oudrMechArea = soup.find('oudrMechArea').get_text().strip()	# 옥외기계식면적(㎡)
        indrAutoUtcnt = soup.find('indrAutoUtcnt').get_text().strip()	# 옥내자주식대수(대)
        indrAutoArea = soup.find('indrAutoArea').get_text().strip()	# 옥내자주식면적(㎡)
        oudrAutoUtcnt = soup.find('oudrAutoUtcnt').get_text().strip()	# 옥외자주식대수(대)
        oudrAutoArea = soup.find('oudrAutoArea').get_text().strip()	# 옥외자주식면적(㎡)
        pmsDay = soup.find('pmsDay').get_text().strip()	# 허가일
        stcnsDay = soup.find('stcnsDay').get_text().strip()	# 착공일
        useAprDay = soup.find('useAprDay').get_text().strip()	# 사용승인일
        pmsnoYear = soup.find('pmsnoYear').get_text().strip()	# 허가번호년
        pmsnoKikCd = soup.find('pmsnoKikCd').get_text().strip()	# 허가번호기관코드
        pmsnoKikCdNm = soup.find('pmsnoKikCdNm').get_text().strip()	# 허가번호기관코드명
        pmsnoGbCd = soup.find('pmsnoGbCd').get_text().strip()	# 허가번호구분코드
        pmsnoGbCdNm = soup.find('pmsnoGbCdNm').get_text().strip()	# 허가번호구분코드명
        hoCnt = soup.find('hoCnt').get_text().strip()	# 호수(호)
        engrGrade = soup.find('engrGrade').get_text().strip()	# 에너지효율등급
        engrRat = soup.find('engrRat').get_text().strip()	# 에너지절감율
        engrEpi = soup.find('engrEpi').get_text().strip()	# EPI점수
        gnBldGrade = soup.find('gnBldGrade').get_text().strip()	# 친환경건축물등급
        gnBldCert = soup.find('gnBldCert').get_text().strip()	# 친환경건축물인증점수
        itgBldGrade = soup.find('itgBldGrade').get_text().strip()	# 지능형건축물등급
        itgBldCert = soup.find('itgBldCert').get_text().strip()	# 지능형건축물인증점수
        crtnDay = soup.find('crtnDay').get_text().strip()	# 생성일자
        totalCount = soup.find('totalCount').get_text().strip()	# 전체갯수

        getBrRecapTitleInfo_data.append({
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
            'newOldRegstrGbCd': newOldRegstrGbCd,
            'newOldRegstrGbCdNm': newOldRegstrGbCdNm,
            'newPlatPlc': newPlatPlc,
            'bldNm': bldNm,
            'splotNm': splotNm,
            'block': block,
            'lot': lot,
            'bylotCnt': bylotCnt,
            'naRoadCd': naRoadCd,
            'naBjdongCd': naBjdongCd,
            'naUgrndCd': naUgrndCd,
            'naMainBun': naMainBun,
            'naSubBun': naSubBun,
            'platArea': platArea,
            'archArea': archArea,
            'bcRat': bcRat,
            'totArea': totArea,
            'vlRatEstmTotArea': vlRatEstmTotArea,
            'vlRat': vlRat,
            'mainPurpsCd': mainPurpsCd,
            'mainPurpsCdNm': mainPurpsCdNm,
            'etcPurps': etcPurps,
            'hhldCnt': hhldCnt,
            'fmlyCnt': fmlyCnt,
            'mainBldCnt': mainBldCnt,
            'atchBldCnt': atchBldCnt,
            'atchBldArea': atchBldArea,
            'totPkngCnt': totPkngCnt,
            'indrMechUtcnt': indrMechUtcnt,
            'indrMechArea': indrMechArea,
            'oudrMechUtcnt': oudrMechUtcnt,
            'oudrMechArea': oudrMechArea,
            'indrAutoUtcnt': indrAutoUtcnt,
            'indrAutoArea': indrAutoArea,
            'oudrAutoUtcnt': oudrAutoUtcnt,
            'oudrAutoArea': oudrAutoArea,
            'pmsDay': pmsDay,
            'stcnsDay': stcnsDay,
            'useAprDay': useAprDay,
            'pmsnoYear': pmsnoYear,
            'pmsnoKikCd': pmsnoKikCd,
            'pmsnoKikCdNm': pmsnoKikCdNm,
            'pmsnoGbCd': pmsnoGbCd,
            'pmsnoGbCdNm': pmsnoGbCdNm,
            'hoCnt': hoCnt,
            'engrGrade': engrGrade,
            'engrRat': engrRat,
            'engrEpi': engrEpi,
            'gnBldGrade': gnBldGrade,
            'gnBldCert': gnBldCert,
            'itgBldGrade': itgBldGrade,
            'itgBldCert': itgBldCert,
            'crtnDay': crtnDay,
            'totalCount': totalCount,
            'update_month': update_month,
        })

def getBrTitleInfo(management_key, management_number, sigungu_cd, bjdong_cd, bun, ji):  # 건축물대장 표제부 조회
    params = {'sigunguCd': sigungu_cd, 'bjdongCd': bjdong_cd, 'bun': bun,
              'ji': ji, 'numOfRows': '1', 'pageNo': '1', 'ServiceKey': config.molit_API_key}
    url = 'http://apis.data.go.kr/1613000/BldRgstService_v2/' + opertaion_list[2]
    res = re.get(url, params=params)
    soup = bs(res.text, features='xml')

    totalCount = soup.find('totalCount').get_text().strip()  # 전체갯수
    
    if totalCount == '0':
        getBrTitleInfo_empty.append({
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
        bylotCnt = soup.find('bylotCnt').get_text().strip()	# 외필지수
        naRoadCd = soup.find('naRoadCd').get_text().strip()	# 새주소도로코드
        naBjdongCd = soup.find('naBjdongCd').get_text().strip()	# 새주소법정동코드
        naUgrndCd = soup.find('naUgrndCd').get_text().strip()	# 새주소지상지하코드
        naMainBun = soup.find('naMainBun').get_text().strip()	# 새주소본번
        naSubBun = soup.find('naSubBun').get_text().strip()	# 새주소부번
        dongNm = soup.find('dongNm').get_text().strip()	# 동명칭
        mainAtchGbCd = soup.find('mainAtchGbCd').get_text().strip()	# 주부속구분코드
        mainAtchGbCdNm = soup.find('mainAtchGbCdNm').get_text().strip()	# 주부속구분코드명
        platArea = soup.find('platArea').get_text().strip()	# 대지면적(㎡)
        archArea = soup.find('archArea').get_text().strip()	# 건축면적(㎡)
        bcRat = soup.find('bcRat').get_text().strip()	# 건폐율(%)
        totArea = soup.find('totArea').get_text().strip()	# 연면적(㎡)
        vlRatEstmTotArea = soup.find('vlRatEstmTotArea').get_text().strip()	# 용적률산정연면적(㎡)
        vlRat = soup.find('vlRat').get_text().strip()	# 용적률(%)
        strctCd = soup.find('strctCd').get_text().strip()	# 구조코드
        strctCdNm = soup.find('strctCdNm').get_text().strip()	# 구조코드명
        etcStrct = soup.find('etcStrct').get_text().strip()	# 기타구조
        mainPurpsCd = soup.find('mainPurpsCd').get_text().strip()	# 주용도코드
        mainPurpsCdNm = soup.find('mainPurpsCdNm').get_text().strip()	# 주용도코드명
        etcPurps = soup.find('etcPurps').get_text().strip()	# 기타용도
        roofCd = soup.find('roofCd').get_text().strip()	# 지붕코드
        roofCdNm = soup.find('roofCdNm').get_text().strip()	# 지붕코드명
        etcRoof = soup.find('etcRoof').get_text().strip()	# 기타지붕
        hhldCnt = soup.find('hhldCnt').get_text().strip()	# 세대수(세대)
        fmlyCnt = soup.find('fmlyCnt').get_text().strip()	# 가구수(가구)
        heit = soup.find('heit').get_text().strip()	# 높이(m)
        grndFlrCnt = soup.find('grndFlrCnt').get_text().strip()	# 지상층수
        ugrndFlrCnt = soup.find('ugrndFlrCnt').get_text().strip()	# 지하층수
        rideUseElvtCnt = soup.find('rideUseElvtCnt').get_text().strip()	# 승용승강기수
        emgenUseElvtCnt = soup.find('emgenUseElvtCnt').get_text().strip()	# 비상용승강기수
        atchBldCnt = soup.find('atchBldCnt').get_text().strip()	# 부속건축물수
        atchBldArea = soup.find('atchBldArea').get_text().strip()	# 부속건축물면적(㎡)
        totDongTotArea = soup.find('totDongTotArea').get_text().strip()	# 총동연면적(㎡)
        indrMechUtcnt = soup.find('indrMechUtcnt').get_text().strip()	# 옥내기계식대수(대)
        indrMechArea = soup.find('indrMechArea').get_text().strip()	# 옥내기계식면적(㎡)
        oudrMechUtcnt = soup.find('oudrMechUtcnt').get_text().strip()	# 옥외기계식대수(대)
        oudrMechArea = soup.find('oudrMechArea').get_text().strip()	# 옥외기계식면적(㎡)
        indrAutoUtcnt = soup.find('indrAutoUtcnt').get_text().strip()	# 옥내자주식대수(대)
        indrAutoArea = soup.find('indrAutoArea').get_text().strip()	# 옥내자주식면적(㎡)
        oudrAutoUtcnt = soup.find('oudrAutoUtcnt').get_text().strip()	# 옥외자주식대수(대)
        oudrAutoArea = soup.find('oudrAutoArea').get_text().strip()	# 옥외자주식면적(㎡)
        pmsDay = soup.find('pmsDay').get_text().strip()	# 허가일
        stcnsDay = soup.find('stcnsDay').get_text().strip()	# 착공일
        useAprDay = soup.find('useAprDay').get_text().strip()	# 사용승인일
        pmsnoYear = soup.find('pmsnoYear').get_text().strip()	# 허가번호년
        pmsnoKikCd = soup.find('pmsnoKikCd').get_text().strip()	# 허가번호기관코드
        pmsnoKikCdNm = soup.find('pmsnoKikCdNm').get_text().strip()	# 허가번호기관코드명
        pmsnoGbCd = soup.find('pmsnoGbCd').get_text().strip()	# 허가번호구분코드
        pmsnoGbCdNm = soup.find('pmsnoGbCdNm').get_text().strip()	# 허가번호구분코드명
        hoCnt = soup.find('hoCnt').get_text().strip()	# 호수(호)
        engrGrade = soup.find('engrGrade').get_text().strip()	# 에너지효율등급
        engrRat = soup.find('engrRat').get_text().strip()	# 에너지절감율
        engrEpi = soup.find('engrEpi').get_text().strip()	# EPI점수
        gnBldGrade = soup.find('gnBldGrade').get_text().strip()	# 친환경건축물등급
        gnBldCert = soup.find('gnBldCert').get_text().strip()	# 친환경건축물인증점수
        itgBldGrade = soup.find('itgBldGrade').get_text().strip()	# 지능형건축물등급
        itgBldCert = soup.find('itgBldCert').get_text().strip()	# 지능형건축물인증점수
        crtnDay = soup.find('crtnDay').get_text().strip()	# 생성일자
        rserthqkDsgnApplyYn = soup.find('rserthqkDsgnApplyYn').get_text().strip()	# 내진 설계 적용 여부
        rserthqkAblty = soup.find('rserthqkAblty').get_text().strip()	# 내진 능력
        totalCount = soup.find('totalCount').get_text().strip()	# 전체갯수

        getBrTitleInfo_data.append({
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
            'bylotCnt': bylotCnt,
            'naRoadCd': naRoadCd,
            'naBjdongCd': naBjdongCd,
            'naUgrndCd': naUgrndCd,
            'naMainBun': naMainBun,
            'naSubBun': naSubBun,
            'dongNm': dongNm,
            'mainAtchGbCd': mainAtchGbCd,
            'mainAtchGbCdNm': mainAtchGbCdNm,
            'platArea': platArea,
            'archArea': archArea,
            'bcRat': bcRat,
            'totArea': totArea,
            'vlRatEstmTotArea': vlRatEstmTotArea,
            'vlRat': vlRat,
            'strctCd': strctCd,
            'strctCdNm': strctCdNm,
            'etcStrct': etcStrct,
            'mainPurpsCd': mainPurpsCd,
            'mainPurpsCdNm': mainPurpsCdNm,
            'etcPurps': etcPurps,
            'roofCd': roofCd,
            'roofCdNm': roofCdNm,
            'etcRoof': etcRoof,
            'hhldCnt': hhldCnt,
            'fmlyCnt': fmlyCnt,
            'heit': heit,
            'grndFlrCnt': grndFlrCnt,
            'ugrndFlrCnt': ugrndFlrCnt,
            'rideUseElvtCnt': rideUseElvtCnt,
            'emgenUseElvtCnt': emgenUseElvtCnt,
            'atchBldCnt': atchBldCnt,
            'atchBldArea': atchBldArea,
            'totDongTotArea': totDongTotArea,
            'indrMechUtcnt': indrMechUtcnt,
            'indrMechArea': indrMechArea,
            'oudrMechUtcnt': oudrMechUtcnt,
            'oudrMechArea': oudrMechArea,
            'indrAutoUtcnt': indrAutoUtcnt,
            'indrAutoArea': indrAutoArea,
            'oudrAutoUtcnt': oudrAutoUtcnt,
            'oudrAutoArea': oudrAutoArea,
            'pmsDay': pmsDay,
            'stcnsDay': stcnsDay,
            'useAprDay': useAprDay,
            'pmsnoYear': pmsnoYear,
            'pmsnoKikCd': pmsnoKikCd,
            'pmsnoKikCdNm': pmsnoKikCdNm,
            'pmsnoGbCd': pmsnoGbCd,
            'pmsnoGbCdNm': pmsnoGbCdNm,
            'hoCnt': hoCnt,
            'engrGrade': engrGrade,
            'engrRat': engrRat,
            'engrEpi': engrEpi,
            'gnBldGrade': gnBldGrade,
            'gnBldCert': gnBldCert,
            'itgBldGrade': itgBldGrade,
            'itgBldCert': itgBldCert,
            'crtnDay': crtnDay,
            'rserthqkDsgnApplyYn': rserthqkDsgnApplyYn,
            'rserthqkAblty': rserthqkAblty,
            'totalCount': totalCount,
            'update_month': update_month,
        })

def getBrFlrOulnInfo(management_key, management_number, sigungu_cd, bjdong_cd, bun, ji):    # 건축물대장 층별개요 조회
    params = {'sigunguCd': sigungu_cd, 'bjdongCd': bjdong_cd, 'bun': bun,
              'ji': ji, 'numOfRows': '1', 'pageNo': '1', 'ServiceKey': config.molit_API_key}
    url = 'http://apis.data.go.kr/1613000/BldRgstService_v2/' + opertaion_list[3]
    res = re.get(url, params=params)
    soup = bs(res.text, features='xml')

    totalCount = soup.find('totalCount').get_text().strip()  # 전체갯수
    
    if totalCount == '0':
        getBrFlrOulnInfo_empty.append({
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
        flrGbCd = soup.find('flrGbCd').get_text().strip()	# 층구분코드
        flrGbCdNm = soup.find('flrGbCdNm').get_text().strip()	# 층구분코드명
        flrNo = soup.find('flrNo').get_text().strip()	# 층번호
        flrNoNm = soup.find('flrNoNm').get_text().strip()	# 층번호명
        strctCd = soup.find('strctCd').get_text().strip()	# 구조코드
        strctCdNm = soup.find('strctCdNm').get_text().strip()	# 구조코드명
        etcStrct = soup.find('etcStrct').get_text().strip()	# 기타구조
        mainPurpsCd = soup.find('mainPurpsCd').get_text().strip()	# 주용도코드
        mainPurpsCdNm = soup.find('mainPurpsCdNm').get_text().strip()	# 주용도코드명
        etcPurps = soup.find('etcPurps').get_text().strip()	# 기타용도
        mainAtchGbCd = soup.find('mainAtchGbCd').get_text().strip()	# 주부속구분코드
        mainAtchGbCdNm = soup.find('mainAtchGbCdNm').get_text().strip()	# 주부속구분코드명
        area = soup.find('area').get_text().strip()	# 면적(㎡)
        areaExctYn = soup.find('areaExctYn').get_text().strip()	# 면적제외여부
        crtnDay = soup.find('crtnDay').get_text().strip()	# 생성일자
        totalCount = soup.find('totalCount').get_text().strip()	# 전체갯수

        getBrFlrOulnInfo_data.append({
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
            'flrGbCd': flrGbCd,
            'flrGbCdNm': flrGbCdNm,
            'flrNo': flrNo,
            'flrNoNm': flrNoNm,
            'strctCd': strctCd,
            'strctCdNm': strctCdNm,
            'etcStrct': etcStrct,
            'mainPurpsCd': mainPurpsCd,
            'mainPurpsCdNm': mainPurpsCdNm,
            'etcPurps': etcPurps,
            'mainAtchGbCd': mainAtchGbCd,
            'mainAtchGbCdNm': mainAtchGbCdNm,
            'area': area,
            'areaExctYn': areaExctYn,
            'crtnDay': crtnDay,
            'totalCount': totalCount,
            'update_month': update_month,
        })

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

        getBrBasisOulnInfo_sql = text('''
            select  distinct management_key, 
                    management_number, 
                    left(beopdong_code,5) as sigungu_cd, 
                    right(beopdong_code,5) as bjdong_cd, 
                    lpad(jibun_1st_number, 4, '0') as bun, 
                    lpad(jibun_2nd_number, 4, '0') as ji 
            from {address_table}
            where management_number not in (select management_number from getBrBasisOulnInfo)
              and management_number not in (select management_number from getBrBasisOulnInfo_empty)
            limit {limit_value}
            '''.format(address_table=address_table, limit_value=limit_value))

        getBrBasisOulnInfo_sql_df = pd.read_sql_query(getBrBasisOulnInfo_sql, conn)
        global getBrBasisOulnInfo_data, getBrBasisOulnInfo_empty
        getBrBasisOulnInfo_data = []
        getBrBasisOulnInfo_empty = []

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

        getBrFlrOulnInfo_sql = text('''
            select  distinct management_key, 
                    management_number, 
                    left(beopdong_code,5) as sigungu_cd, 
                    right(beopdong_code,5) as bjdong_cd, 
                    lpad(jibun_1st_number, 4, '0') as bun, 
                    lpad(jibun_2nd_number, 4, '0') as ji 
            from {address_table}
            where management_number not in (select management_number from getBrFlrOulnInfo)
              and management_number not in (select management_number from getBrFlrOulnInfo_empty)
            limit {limit_value}
            '''.format(address_table=address_table, limit_value=limit_value))

        getBrFlrOulnInfo_sql_df = pd.read_sql_query(getBrFlrOulnInfo_sql, conn)
        global getBrFlrOulnInfo_data, getBrFlrOulnInfo_empty
        getBrFlrOulnInfo_data = []
        getBrFlrOulnInfo_empty = []

        getBrRecapTitleInfo_sql = text('''
            select  distinct management_key, 
                    management_number, 
                    left(beopdong_code,5) as sigungu_cd, 
                    right(beopdong_code,5) as bjdong_cd, 
                    lpad(jibun_1st_number, 4, '0') as bun, 
                    lpad(jibun_2nd_number, 4, '0') as ji
            from {address_table}
            where management_number not in (select management_number from getBrRecapTitleInfo)
              and management_number not in (select management_number from getBrRecapTitleInfo_empty)
            limit {limit_value}
            '''.format(address_table=address_table, limit_value=limit_value))

        getBrRecapTitleInfo_sql_df = pd.read_sql_query(getBrRecapTitleInfo_sql, conn)
        global getBrRecapTitleInfo_data, getBrRecapTitleInfo_empty
        getBrRecapTitleInfo_data = []
        getBrRecapTitleInfo_empty = []

        getBrTitleInfo_sql = text('''
            select  distinct management_key, 
                    management_number, 
                    left(beopdong_code,5) as sigungu_cd, 
                    right(beopdong_code,5) as bjdong_cd, 
                    lpad(jibun_1st_number, 4, '0') as bun, 
                    lpad(jibun_2nd_number, 4, '0') as ji
            from {address_table}
            where management_number not in (select management_number from getBrTitleInfo)
              and management_number not in (select management_number from getBrTitleInfo_empty)
            limit {limit_value}
            '''.format(address_table=address_table, limit_value=limit_value))

        getBrTitleInfo_sql_df = pd.read_sql_query(getBrTitleInfo_sql, conn)
        global getBrTitleInfo_data, getBrTitleInfo_empty
        getBrTitleInfo_data = []
        getBrTitleInfo_empty = []

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
                pass

            try:
                getBrBasisOulnInfo(
                    getBrBasisOulnInfo_sql_df['management_key'][process_cnt],
                    getBrBasisOulnInfo_sql_df['management_number'][process_cnt],
                    getBrBasisOulnInfo_sql_df['sigungu_cd'][process_cnt],
                    getBrBasisOulnInfo_sql_df['bjdong_cd'][process_cnt],
                    getBrBasisOulnInfo_sql_df['bun'][process_cnt],
                    getBrBasisOulnInfo_sql_df['ji'][process_cnt],
                )
            except Exception as e:
                pass

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

            try:
                getBrFlrOulnInfo(
                    getBrFlrOulnInfo_sql_df['management_key'][process_cnt],
                    getBrFlrOulnInfo_sql_df['management_number'][process_cnt],
                    getBrFlrOulnInfo_sql_df['sigungu_cd'][process_cnt],
                    getBrFlrOulnInfo_sql_df['bjdong_cd'][process_cnt],
                    getBrFlrOulnInfo_sql_df['bun'][process_cnt],
                    getBrFlrOulnInfo_sql_df['ji'][process_cnt],
                )
            except Exception as e:
                pass

            try:
                getBrRecapTitleInfo(
                    getBrRecapTitleInfo_sql_df['management_key'][process_cnt],
                    getBrRecapTitleInfo_sql_df['management_number'][process_cnt],
                    getBrRecapTitleInfo_sql_df['sigungu_cd'][process_cnt],
                    getBrRecapTitleInfo_sql_df['bjdong_cd'][process_cnt],
                    getBrRecapTitleInfo_sql_df['bun'][process_cnt],
                    getBrRecapTitleInfo_sql_df['ji'][process_cnt],
                )
            except Exception as e:
                pass

            try:
                getBrTitleInfo(
                    getBrTitleInfo_sql_df['management_key'][process_cnt],
                    getBrTitleInfo_sql_df['management_number'][process_cnt],
                    getBrTitleInfo_sql_df['sigungu_cd'][process_cnt],
                    getBrTitleInfo_sql_df['bjdong_cd'][process_cnt],
                    getBrTitleInfo_sql_df['bun'][process_cnt],
                    getBrTitleInfo_sql_df['ji'][process_cnt],
                )
            except Exception as e:
                pass


        getXYcoordinate_data_df = pd.DataFrame(getXYcoordinate_data)
        getXYcoordinate_empty_df = pd.DataFrame(getXYcoordinate_empty)
        getArchitecturePossessionInfo_data_df = pd.DataFrame(getArchitecturePossessionInfo_data)
        getArchitecturePossessionInfo_empty_df = pd.DataFrame(getArchitecturePossessionInfo_empty)
        getBrBasisOulnInfo_data_df = pd.DataFrame(getBrBasisOulnInfo_data)
        getBrBasisOulnInfo_empty_df = pd.DataFrame(getBrBasisOulnInfo_empty)
        getBrExposPubuseAreaInfo_data_df = pd.DataFrame(getBrExposPubuseAreaInfo_data)
        getBrExposPubuseAreaInfo_empty_df = pd.DataFrame(getBrExposPubuseAreaInfo_empty)
        getBrFlrOulnInfo_data_df = pd.DataFrame(getBrFlrOulnInfo_data)
        getBrFlrOulnInfo_empty_df = pd.DataFrame(getBrFlrOulnInfo_empty)
        getBrRecapTitleInfo_data_df = pd.DataFrame(getBrRecapTitleInfo_data)
        getBrRecapTitleInfo_empty_df = pd.DataFrame(getBrRecapTitleInfo_empty)
        getBrTitleInfo_data_df = pd.DataFrame(getBrTitleInfo_data)
        getBrTitleInfo_empty_df = pd.DataFrame(getBrTitleInfo_empty)


        # Bulk Data DB 업로드 속도 개선 기능
        @event.listens_for(engine, "before_cursor_execute")
        def receive_before_cursor_execute(
                conn, cursor, statement, params, context, executemany
        ):
            if executemany:
                cursor.fast_executemany = True

        getXYcoordinate_data_df.to_sql('getXYcoordinate', engine, index=False, if_exists="append", chunksize=100000)
        getXYcoordinate_empty_df.to_sql('getXYcoordinate_empty', engine, index=False, if_exists="append", chunksize=100000)
        getArchitecturePossessionInfo_data_df.to_sql('getArchitecturePossessionInfo', engine, index=False, if_exists="append", chunksize=100000)
        getArchitecturePossessionInfo_empty_df.to_sql('getArchitecturePossessionInfo_empty', engine, index=False, if_exists="append", chunksize=100000)
        getBrBasisOulnInfo_data_df.to_sql('getBrBasisOulnInfo', engine, index=False, if_exists="append", chunksize=100000)
        getBrBasisOulnInfo_empty_df.to_sql('getBrBasisOulnInfo_empty', engine, index=False, if_exists="append", chunksize=100000)
        getBrExposPubuseAreaInfo_data_df.to_sql('getBrExposPubuseAreaInfo', engine, index=False, if_exists="append", chunksize=100000)
        getBrExposPubuseAreaInfo_empty_df.to_sql('getBrExposPubuseAreaInfo_empty', engine, index=False, if_exists="append", chunksize=100000)
        getBrFlrOulnInfo_data_df.to_sql('getBrFlrOulnInfo', engine, index=False, if_exists="append", chunksize=100000)
        getBrFlrOulnInfo_empty_df.to_sql('getBrFlrOulnInfo_empty', engine, index=False, if_exists="append", chunksize=100000)
        getBrRecapTitleInfo_data_df.to_sql('getBrRecapTitleInfo', engine, index=False, if_exists="append", chunksize=100000)
        getBrRecapTitleInfo_empty_df.to_sql('getBrRecapTitleInfo_empty', engine, index=False, if_exists="append", chunksize=100000)
        getBrTitleInfo_data_df.to_sql('getBrTitleInfo', engine, index=False, if_exists="append", chunksize=100000)
        getBrTitleInfo_empty_df.to_sql('getBrTitleInfo_empty', engine, index=False, if_exists="append", chunksize=100000)