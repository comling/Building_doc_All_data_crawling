# Revision
## Ver 1.0
* 프로그램 및 DB 세팅 완료

# 건축물대장정보 및 주소 좌표정보 수집 프로그램
본 프로그램은 행안부 주소 정보(https://juso.go.kr/)를 기반으로 행안부 및 국토부에서 제공하는 OpenAPI를 사용하여 각종 정보를 수집하는 프로그램임

## 활용 OpenAPI 정보
* 건축물소유자정보 제공서비스(NIA)
* 건축물대장정보 서비스(NIA)
* 건축인허가 정보 서비스(NIA)
* 주소DB 제공 Bulk Data(행안부)
* Geocoder API 2.0 주소-좌표 변환(Vworld)

## Built With:
* Python 3.11.2

## file manual:
1. [crawling_All_autostart.py](crawling_All_autostart.py)
* 수집 프로그램을 자동으로 실행해주는 코드
* 종료 시 grep을 통해 프로세스를 확인하고 재실행
2. [getXYcoordinate.py](getXYcoordinate.py)
* DB의 주소 정보를 Vworld에서 제공하는 좌표변환 OpenAPI를 사용하여 좌표로 변환하여 DB에 저장
3. [getArchitecturePossessionInfo.py](getArchitecturePossessionInfo.py)
* NIA의 건축물대장소유자정보조회 OpenAPI를 통해 건축물대장 소유자정보 수집
4. [getBrBasisOulnInfo.py](getBrBasisOulnInfo.py)
* NIA의 건축물대장 기본개요 조회 OpenAPI를 통해 건축물대장 기본개요 정보 수집
5. [getBrExposPubuseAreaInfo.py](getBrExposPubuseAreaInfo.py)
* NIA의 건축물대장 전유공용면적 조회 OpenAPI를 통해 건축물대장 전유공용면적 정보 수집
6. [getBrFlrOulnInfo.py](getBrFlrOulnInfo.py)
* NIA의 건축물대장 층별개요 조회 OpenAPI를 통해 건축물대장 층별개요 정보 수집
7. [getBrRecapTitleInfo.py](getBrRecapTitleInfo.py)
* NIA의 건축물대장 총괄표제부 조회 OpenAPI를 통해 건축물대장 총괄표제부 정보 수집
8. [getBrTitleInfo.py](getBrTitleInfo.py)
* NIA의 건축물대장 표제부 조회 OpenAPI를 통해 건축물대장 표제부 정보 수집
9. [config.py](config.py)
* OpenAPI Key 정보 및 DB 접속 정보

## How to USE:
* crawling_All_autostart.py을 cmd 창에서 실행
\'python crawling_All_autostart.py'\
