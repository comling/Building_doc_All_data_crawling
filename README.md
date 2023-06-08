# building_doc_crawling
본 시스템은 정부에서 제공하는 OpenAPI 서비스를 활용하여 건축물대장 소유자 및 대장정보를 조회하여 DB에 저장하는 프로그램임

## 활용 OpenAPI 정보
* 건축물소유자정보 제공서비스(NIA)
* 건축물대장정보 서비스(NIA)
* 건축인허가 정보 서비스(NIA)
* 주소DB 제공[Bulk Data](행안부)
* Geocoder API 2.0 주소-좌표 변환(Vworld)

## Built With:
* Python 3.11.2

## Use Library
* pandas
* sqlalchemy
* requests
* beautifulsoup4
* pyproj
* openpyxl
* tqdm
