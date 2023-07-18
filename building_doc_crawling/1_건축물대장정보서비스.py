from tqdm import tqdm
import pandas as pd
import openpyxl

import requests as re
import config
import time
from bs4 import BeautifulSoup as bs

opertaion_list = ['getBrBasisOulnInfo', # 건축물대장 기본개요 조회
                'getBrRecapTitleInfo', # 건축물대장 총괄표제부 조회
                'getBrTitleInfo', # 건축물대장 표제부 조회
                'getBrFlrOulnInfo', # 건축물대장 층별개요 조회
                'getBrExposPubuseAreaInfo', # 건축물대장 전유공용면적 조회
                 ]

sigungu_cd = '26110'
bjdong_cd = '10100'
bun = '0002'
ji = '0000'

params = {'sigunguCd': sigungu_cd, 'bjdongCd': bjdong_cd, 'bun': bun,
          'ji': ji, 'numOfRows': '1', 'pageNo': '1', 'ServiceKey': config.molit_API_key}
url = 'http://apis.data.go.kr/1613000/BldRgstService_v2/' + opertaion_list[9]
res = re.get(url, params=params)
soup = bs(res.text, features='xml')

totalCount = soup.find('totalCount').get_text().strip()  # 전체갯수

def getBrBasisOulnInfo(management_number, sigungu_cd, bjdong_cd, bun, ji):  # 건축물대장 기본개요 조회
    params = {'sigunguCd': sigungu_cd, 'bjdongCd': bjdong_cd, 'bun': bun,
                          'ji': ji, 'numOfRows': '1', 'pageNo': '1', 'ServiceKey': config.molit_API_key}
    url = 'http://apis.data.go.kr/1613000/BldRgstService_v2/' + opertaion_list[0]
    res = re.get(url, params=params)
    soup = bs(res.text, features='xml')

    totalCount = soup.find('totalCount').get_text().strip()  # 전체갯수

    if totalCount == '0':
        return {
            'management_number': management_number,
            'rnum': None,
            'platPlc': None,
            'sigunguCd': None,
            'bjdongCd': None,
            'platGbCd': None,
            'bun': None,
            'ji': None,
            'mgmBldrgstPk': None,
            'mgmUpBldrgstPk': None,
            'regstrGbCd': None,
            'regstrGbCdNm': None,
            'regstrKindCd': None,
            'regstrKindCdNm': None,
            'newPlatPlc': None,
            'bldNm': None,
            'splotNm': None,
            'block': None,
            'lot': None,
            'bylotCnt': None,
            'naRoadCd': None,
            'naBjdongCd': None,
            'naUgrndCd': None,
            'naMainBun': None,
            'naSubBun': None,
            'jiyukCd': None,
            'jiguCd': None,
            'guyukCd': None,
            'jiyukCdNm': None,
            'jiguCdNm': None,
            'guyukCdNm': None,
            'crtnDay': None,
            'totalCount': totalCount
        }


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

        return {
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
            'totalCount': totalCount
        }

def getBrRecapTitleInfo(management_number, sigungu_cd, bjdong_cd, bun, ji):  # 건축물대장 총괄표제부 조회
    params = {'sigunguCd': sigungu_cd, 'bjdongCd': bjdong_cd, 'bun': bun,
              'ji': ji, 'numOfRows': '1', 'pageNo': '1', 'ServiceKey': config.molit_API_key}
    url = 'http://apis.data.go.kr/1613000/BldRgstService_v2/' + opertaion_list[1]
    res = re.get(url, params=params)
    soup = bs(res.text, features='xml')

    totalCount = soup.find('totalCount').get_text().strip()  # 전체갯수

    if totalCount == '0':
        return {
            'management_number': management_number,
            'rnum': None,
            'platPlc': None,
            'sigunguCd': None,
            'bjdongCd': None,
            'platGbCd': None,
            'bun': None,
            'ji': None,
            'mgmBldrgstPk': None,
            'regstrGbCd': None,
            'regstrGbCdNm': None,
            'regstrKindCd': None,
            'regstrKindCdNm': None,
            'newOldRegstrGbCd': None,
            'newOldRegstrGbCdNm': None,
            'newPlatPlc': None,
            'bldNm': None,
            'splotNm': None,
            'block': None,
            'lot': None,
            'bylotCnt': None,
            'naRoadCd': None,
            'naBjdongCd': None,
            'naUgrndCd': None,
            'naMainBun': None,
            'naSubBun': None,
            'platArea': None,
            'archArea': None,
            'bcRat': None,
            'totArea': None,
            'vlRatEstmTotArea': None,
            'vlRat': None,
            'mainPurpsCd': None,
            'mainPurpsCdNm': None,
            'etcPurps': None,
            'hhldCnt': None,
            'fmlyCnt': None,
            'mainBldCnt': None,
            'atchBldCnt': None,
            'atchBldArea': None,
            'totPkngCnt': None,
            'indrMechUtcnt': None,
            'indrMechArea': None,
            'oudrMechUtcnt': None,
            'oudrMechArea': None,
            'indrAutoUtcnt': None,
            'indrAutoArea': None,
            'oudrAutoUtcnt': None,
            'oudrAutoArea': None,
            'pmsDay': None,
            'stcnsDay': None,
            'useAprDay': None,
            'pmsnoYear': None,
            'pmsnoKikCd': None,
            'pmsnoKikCdNm': None,
            'pmsnoGbCd': None,
            'pmsnoGbCdNm': None,
            'hoCnt': None,
            'engrGrade': None,
            'engrRat': None,
            'engrEpi': None,
            'gnBldGrade': None,
            'gnBldCert': None,
            'itgBldGrade': None,
            'itgBldCert': None,
            'crtnDay': None,
            'totalCount': totalCount,
        }

    elif totalCount != '0':
        management_number = soup.find('management_number').get_text().strip()	# 관리번호
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
        
        return {
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
        }

def getBrTitleInfo(management_number, sigungu_cd, bjdong_cd, bun, ji):  # 건축물대장 표제부 조회
    params = {'sigunguCd': sigungu_cd, 'bjdongCd': bjdong_cd, 'bun': bun,
              'ji': ji, 'numOfRows': '1', 'pageNo': '1', 'ServiceKey': config.molit_API_key}
    url = 'http://apis.data.go.kr/1613000/BldRgstService_v2/' + opertaion_list[2]
    res = re.get(url, params=params)
    soup = bs(res.text, features='xml')

    totalCount = soup.find('totalCount').get_text().strip()  # 전체갯수
    
    if totalCount == '0':
        return {
            'management_number': management_number,
            'rnum': None,
            'platPlc': None,
            'sigunguCd': None,
            'bjdongCd': None,
            'platGbCd': None,
            'bun': None,
            'ji': None,
            'mgmBldrgstPk': None,
            'regstrGbCd': None,
            'regstrGbCdNm': None,
            'regstrKindCd': None,
            'regstrKindCdNm': None,
            'newPlatPlc': None,
            'bldNm': None,
            'splotNm': None,
            'block': None,
            'lot': None,
            'bylotCnt': None,
            'naRoadCd': None,
            'naBjdongCd': None,
            'naUgrndCd': None,
            'naMainBun': None,
            'naSubBun': None,
            'dongNm': None,
            'mainAtchGbCd': None,
            'mainAtchGbCdNm': None,
            'platArea': None,
            'archArea': None,
            'bcRat': None,
            'totArea': None,
            'vlRatEstmTotArea': None,
            'vlRat': None,
            'strctCd': None,
            'strctCdNm': None,
            'etcStrct': None,
            'mainPurpsCd': None,
            'mainPurpsCdNm': None,
            'etcPurps': None,
            'roofCd': None,
            'roofCdNm': None,
            'etcRoof': None,
            'hhldCnt': None,
            'fmlyCnt': None,
            'heit': None,
            'grndFlrCnt': None,
            'ugrndFlrCnt': None,
            'rideUseElvtCnt': None,
            'emgenUseElvtCnt': None,
            'atchBldCnt': None,
            'atchBldArea': None,
            'totDongTotArea': None,
            'indrMechUtcnt': None,
            'indrMechArea': None,
            'oudrMechUtcnt': None,
            'oudrMechArea': None,
            'indrAutoUtcnt': None,
            'indrAutoArea': None,
            'oudrAutoUtcnt': None,
            'oudrAutoArea': None,
            'pmsDay': None,
            'stcnsDay': None,
            'useAprDay': None,
            'pmsnoYear': None,
            'pmsnoKikCd': None,
            'pmsnoKikCdNm': None,
            'pmsnoGbCd': None,
            'pmsnoGbCdNm': None,
            'hoCnt': None,
            'engrGrade': None,
            'engrRat': None,
            'engrEpi': None,
            'gnBldGrade': None,
            'gnBldCert': None,
            'itgBldGrade': None,
            'itgBldCert': None,
            'crtnDay': None,
            'rserthqkDsgnApplyYn': None,
            'rserthqkAblty': None,
            'totalCount': totalCount,
        }
    
    elif totalCount != '0':
        management_number = soup.find('management_number').get_text().strip()	# 관리번호
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
        
        return {
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
        }
    
def getBrFlrOulnInfo(management_number, sigungu_cd, bjdong_cd, bun, ji):    # 건축물대장 층별개요 조회
    params = {'sigunguCd': sigungu_cd, 'bjdongCd': bjdong_cd, 'bun': bun,
              'ji': ji, 'numOfRows': '1', 'pageNo': '1', 'ServiceKey': config.molit_API_key}
    url = 'http://apis.data.go.kr/1613000/BldRgstService_v2/' + opertaion_list[3]
    res = re.get(url, params=params)
    soup = bs(res.text, features='xml')

    totalCount = soup.find('totalCount').get_text().strip()  # 전체갯수
    
    if totalCount == '0':
        return {
            'management_number': management_number,
            'rnum': None,
            'platPlc': None,
            'sigunguCd': None,
            'bjdongCd': None,
            'platGbCd': None,
            'bun': None,
            'ji': None,
            'mgmBldrgstPk': None,
            'newPlatPlc': None,
            'bldNm': None,
            'splotNm': None,
            'block': None,
            'lot': None,
            'naRoadCd': None,
            'naBjdongCd': None,
            'naUgrndCd': None,
            'naMainBun': None,
            'naSubBun': None,
            'dongNm': None,
            'flrGbCd': None,
            'flrGbCdNm': None,
            'flrNo': None,
            'flrNoNm': None,
            'strctCd': None,
            'strctCdNm': None,
            'etcStrct': None,
            'mainPurpsCd': None,
            'mainPurpsCdNm': None,
            'etcPurps': None,
            'mainAtchGbCd': None,
            'mainAtchGbCdNm': None,
            'area': None,
            'areaExctYn': None,
            'crtnDay': None,
            'totalCount': totalCount,
        }
    
    elif totalCount != '0':
        management_number = soup.find('management_number').get_text().strip()	# 관리번호
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

        return {
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
        }

def getBrExposPubuseAreaInfo(management_number, sigungu_cd, bjdong_cd, bun, ji):  # 건축물대장 전유공용면적 조회
    params = {'sigunguCd': sigungu_cd, 'bjdongCd': bjdong_cd, 'bun': bun,
              'ji': ji, 'numOfRows': '1', 'pageNo': '1', 'ServiceKey': config.molit_API_key}
    url = 'http://apis.data.go.kr/1613000/BldRgstService_v2/' + opertaion_list[4]
    res = re.get(url, params=params)
    soup = bs(res.text, features='xml')

    totalCount = soup.find('totalCount').get_text().strip()  # 전체갯수

    if totalCount == '0':
        return {
            'management_number': management_number,
            'rnum': None,
            'platPlc': None,
            'sigunguCd': None,
            'bjdongCd': None,
            'platGbCd': None,
            'bun': None,
            'ji': None,
            'mgmBldrgstPk': None,
            'regstrGbCd': None,
            'regstrGbCdNm': None,
            'regstrKindCd': None,
            'regstrKindCdNm': None,
            'newPlatPlc': None,
            'bldNm': None,
            'splotNm': None,
            'block': None,
            'lot': None,
            'naRoadCd': None,
            'naBjdongCd': None,
            'naUgrndCd': None,
            'naMainBun': None,
            'naSubBun': None,
            'dongNm': None,
            'hoNm': None,
            'flrGbCd': None,
            'flrGbCdNm': None,
            'flrNo': None,
            'flrNoNm': None,
            'exposPubuseGbCd': None,
            'exposPubuseGbCdNm': None,
            'mainAtchGbCd': None,
            'mainAtchGbCdNm': None,
            'strctCd': None,
            'strctCdNm': None,
            'etcStrct': None,
            'mainPurpsCd': None,
            'mainPurpsCdNm': None,
            'etcPurps': None,
            'area': None,
            'crtnDay': None,
            'totalCount': totalCount,
        }
    
    elif totalCount != '0':
        management_number = soup.find('management_number').get_text().strip()	# 관리번호
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

        return {
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
        }

 