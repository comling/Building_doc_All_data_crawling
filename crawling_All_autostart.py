import os
import sys

def getXYcoordinate():
    getXYcoordinate_process_read = os.popen("ps -ef | grep getXYcoordinate.py | grep -v 'grep'").readlines()
    # ps -ef 명령어를 이용해서 현재 프로세스를 출력한 후, 그 중 getXYcoordinate.py 문자열이 포함된 줄만 모은다.
    # grep 명령어 자체도 프로세스에 나타나므로 grep -v를 이용해서 제외한다.
    getXYcoordinate_check_process = str(getXYcoordinate_process_read)
    # 문자열로 변환한다.
    getXYcoordinate_text_location = getXYcoordinate_check_process.find("getXYcoordinate.py")
    # run24h.py가 몇번째 문자열인지 찾아낸다. 만약 문자열이 없으면, 즉 프로세스가 존재하지 않을 경우에는 -1을 반환한다.
    if ( getXYcoordinate_text_location == -1 ):
        print("Process not found!")
        os.system("python getXYcoordinate.py &")
        # 해당 프로그램을 다시 실행한다. 백그라운드에서 실행할 경우 &기호를 붙인다.
    else:
        print("Process exists. Location is", getXYcoordinate_text_location)

def getArchitecturePossessionInfo():
    getArchitecturePossessionInfo_process_read = os.popen("ps -ef | grep getArchitecturePossessionInfo.py | grep -v 'grep'").readlines()
    # ps -ef 명령어를 이용해서 현재 프로세스를 출력한 후, 그 중 getArchitecturePossessionInfo.py 문자열이 포함된 줄만 모은다.
    # grep 명령어 자체도 프로세스에 나타나므로 grep -v를 이용해서 제외한다.
    getArchitecturePossessionInfo_check_process = str(getArchitecturePossessionInfo_process_read)
    # 문자열로 변환한다.
    getArchitecturePossessionInfo_text_location = getArchitecturePossessionInfo_check_process.find("getArchitecturePossessionInfo.py")
    # run24h.py가 몇번째 문자열인지 찾아낸다. 만약 문자열이 없으면, 즉 프로세스가 존재하지 않을 경우에는 -1을 반환한다.
    if ( getArchitecturePossessionInfo_text_location == -1 ):
        print("Process not found!")
        os.system("python getArchitecturePossessionInfo.py &")
        # 해당 프로그램을 다시 실행한다. 백그라운드에서 실행할 경우 &기호를 붙인다.
    else:
        print("Process exists. Location is", getArchitecturePossessionInfo_text_location)

def getBrBasisOulnInfo():
    getBrBasisOulnInfo_process_read = os.popen("ps -ef | grep getBrBasisOulnInfo.py | grep -v 'grep'").readlines()
    # ps -ef 명령어를 이용해서 현재 프로세스를 출력한 후, 그 중 getBrBasisOulnInfo.py 문자열이 포함된 줄만 모은다.
    # grep 명령어 자체도 프로세스에 나타나므로 grep -v를 이용해서 제외한다.
    getBrBasisOulnInfo_check_process = str(getBrBasisOulnInfo_process_read)
    # 문자열로 변환한다.
    getBrBasisOulnInfo_text_location = getBrBasisOulnInfo_check_process.find("getBrBasisOulnInfo.py")
    # run24h.py가 몇번째 문자열인지 찾아낸다. 만약 문자열이 없으면, 즉 프로세스가 존재하지 않을 경우에는 -1을 반환한다.
    if ( getBrBasisOulnInfo_text_location == -1 ):
        print("Process not found!")
        os.system("python getBrBasisOulnInfo.py &")
        # 해당 프로그램을 다시 실행한다. 백그라운드에서 실행할 경우 &기호를 붙인다.
    else:
        print("Process exists. Location is", getBrBasisOulnInfo_text_location)

def getBrExposPubuseAreaInfo():
    getBrExposPubuseAreaInfo_process_read = os.popen("ps -ef | grep getBrExposPubuseAreaInfo.py | grep -v 'grep'").readlines()
    # ps -ef 명령어를 이용해서 현재 프로세스를 출력한 후, 그 중 getBrExposPubuseAreaInfo.py 문자열이 포함된 줄만 모은다.
    # grep 명령어 자체도 프로세스에 나타나므로 grep -v를 이용해서 제외한다.
    getBrExposPubuseAreaInfo_check_process = str(getBrExposPubuseAreaInfo_process_read)
    # 문자열로 변환한다.
    getBrExposPubuseAreaInfo_text_location = getBrExposPubuseAreaInfo_check_process.find("getBrExposPubuseAreaInfo.py")
    # run24h.py가 몇번째 문자열인지 찾아낸다. 만약 문자열이 없으면, 즉 프로세스가 존재하지 않을 경우에는 -1을 반환한다.
    if ( getBrExposPubuseAreaInfo_text_location == -1 ):
        print("Process not found!")
        os.system("python getBrExposPubuseAreaInfo.py &")
        # 해당 프로그램을 다시 실행한다. 백그라운드에서 실행할 경우 &기호를 붙인다.
    else:
        print("Process exists. Location is", getBrExposPubuseAreaInfo_text_location)

def getBrFlrOulnInfo():
    getBrFlrOulnInfo_process_read = os.popen("ps -ef | grep getBrFlrOulnInfo.py | grep -v 'grep'").readlines()
    # ps -ef 명령어를 이용해서 현재 프로세스를 출력한 후, 그 중 getBrFlrOulnInfo.py 문자열이 포함된 줄만 모은다.
    # grep 명령어 자체도 프로세스에 나타나므로 grep -v를 이용해서 제외한다.
    getBrFlrOulnInfo_check_process = str(getBrFlrOulnInfo_process_read)
    # 문자열로 변환한다.
    getBrFlrOulnInfo_text_location = getBrFlrOulnInfo_check_process.find("getBrFlrOulnInfo.py")
    # run24h.py가 몇번째 문자열인지 찾아낸다. 만약 문자열이 없으면, 즉 프로세스가 존재하지 않을 경우에는 -1을 반환한다.
    if (getBrFlrOulnInfo_text_location == -1):
        print("Process not found!")
        os.system("python getBrFlrOulnInfo.py &")
        # 해당 프로그램을 다시 실행한다. 백그라운드에서 실행할 경우 &기호를 붙인다.
    else:
        print("Process exists. Location is", getBrFlrOulnInfo_text_location)

def getBrRecapTitleInfo():
    getBrRecapTitleInfo_process_read = os.popen("ps -ef | grep getBrRecapTitleInfo.py | grep -v 'grep'").readlines()
    # ps -ef 명령어를 이용해서 현재 프로세스를 출력한 후, 그 중 getBrRecapTitleInfo.py 문자열이 포함된 줄만 모은다.
    # grep 명령어 자체도 프로세스에 나타나므로 grep -v를 이용해서 제외한다.
    getBrRecapTitleInfo_check_process = str(getBrRecapTitleInfo_process_read)
    # 문자열로 변환한다.
    getBrRecapTitleInfo_text_location = getBrRecapTitleInfo_check_process.find("getBrRecapTitleInfo.py")
    # run24h.py가 몇번째 문자열인지 찾아낸다. 만약 문자열이 없으면, 즉 프로세스가 존재하지 않을 경우에는 -1을 반환한다.
    if (getBrRecapTitleInfo_text_location == -1):
        print("Process not found!")
        os.system("python getBrRecapTitleInfo.py &")
        # 해당 프로그램을 다시 실행한다. 백그라운드에서 실행할 경우 &기호를 붙인다.
    else:
        print("Process exists. Location is", getBrRecapTitleInfo_text_location)

def getBrTitleInfo():
    getBrTitleInfo_process_read = os.popen("ps -ef | grep getBrTitleInfo.py | grep -v 'grep'").readlines()
    # ps -ef 명령어를 이용해서 현재 프로세스를 출력한 후, 그 중 getBrTitleInfo.py 문자열이 포함된 줄만 모은다.
    # grep 명령어 자체도 프로세스에 나타나므로 grep -v를 이용해서 제외한다.
    getBrTitleInfo_check_process = str(getBrTitleInfo_process_read)
    # 문자열로 변환한다.
    getBrTitleInfo_text_location = getBrTitleInfo_check_process.find("getBrTitleInfo.py")
    # run24h.py가 몇번째 문자열인지 찾아낸다. 만약 문자열이 없으면, 즉 프로세스가 존재하지 않을 경우에는 -1을 반환한다.
    if (getBrTitleInfo_text_location == -1):
        print("Process not found!")
        os.system("python getBrTitleInfo.py &")
        # 해당 프로그램을 다시 실행한다. 백그라운드에서 실행할 경우 &기호를 붙인다.
    else:
        print("Process exists. Location is", getBrTitleInfo_text_location)
