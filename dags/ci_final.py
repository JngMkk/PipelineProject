#-*- coding: utf-8 -*-
import os
import requests
import pandas as pd
from bs4 import BeautifulSoup as bs
import math
import re
import pendulum
from mod.slackbot import Slack
from mod.csvtojson import csv_to_json
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.operators.bash import BashOperator

kst = pendulum.timezone('Asia/Seoul')
key = ''
sb = Slack('#pipeline')

default_args = {
    "owner" : "admin",
    "depends_on_past" : False,
    "wait_for_downstream" : False,
    "retries" : 1,
    "retry_delay" : timedelta(minutes=1),
    "on_failure_callback" : sb.fail,
}

dag = DAG(
    dag_id='ciAirflow',
    default_args=default_args,
    schedule_interval="50 8 * * *",
    start_date=datetime(2022, 3, 30, tzinfo=kst),
    end_date=datetime(2022, 4, 2, tzinfo=kst),
    catchup=False
)

def ci_all():
    try:
        """ 전국 놀이시설 정보 """
        ciSeq = []              #ciSeq 놀이시설 일련번호
        ciCode1 = []            #ciCode1 놀이시설코드 1
        ciCode2 = []            #ciCode2 놀이시설코드 2
        ciName = []             #ciName 놀이시설명
        ciMinGong = []          #name4 민간/공공
        ciInOut = []            #name21 실/내외
        ciPlaceCode = []        #code1 설치장소코드
        ciDuty = []             #name15 의무시설여부코드
        ciOperCode = []         #code2 시설운영구분코드

        cnt = 1
        while True:
            url = f"http://openapi.cpf.go.kr/openapi/service/rest/ChildPlyFcltInfoService/getFcltInfo?ServiceKey={key}&numOfRows=10000&pageNo={cnt}"
            res = requests.get(url)
            if res.status_code != 200:
                sb.dbgout(f"ci_all API Response Error!(Code : {res.status_code})")
                break
            res.encoding='UTF-8'
            html = res.text
            soup = bs(html, 'html.parser')
            items = soup.select_one("items")
            total = int(soup.select_one("totalcount").text)
            for item in items:
                ciSeq.append(item.select_one("ciSeq").text if item.select_one("ciSeq") else None)
                ciCode1.append(item.select_one("ciCode1").text)
                ciCode2.append(item.select_one("ciCode2").text)
                ciName.append(item.select_one("ciName").text)
                ciMinGong.append(item.select_one("name4").text)
                ciInOut.append(item.select_one("name21").text)
                ciPlaceCode.append(item.select_one("code1").text)
                ciDuty.append(item.select_one("name15").text)
                ciOperCode.append(item.select_one("code2").text)
            if cnt == math.ceil(total/10000):
                break
            cnt += 1

        ci_df = pd.DataFrame([ciSeq, ciCode1, ciCode2, ciName, ciMinGong, ciInOut, ciPlaceCode, ciDuty, ciOperCode]).T
        ci_df.columns = ['ciSeq', 'ciCode1', 'ciCode2', 'ciName', 'ciMinGong', 'ciInOut', 'ciPlaceCode', 'ciDuty', 'ciOperCode']
        ci_df = ci_df.drop(ci_df[ci_df.duplicated(['ciSeq'])].index)
        ci_df['ciSeq'] = ci_df['ciSeq'].astype('int')
        ci_df.sort_values(by='ciSeq', inplace=True)
        ci_df.reset_index(drop=True, inplace=True)

        a = ['-'] * len(ci_df)
        ci_df['ciCode'] = ci_df['ciCode1'].astype('str') + a + ci_df['ciCode2'].astype('str')
        ci_df = ci_df.drop(ci_df[ci_df.duplicated(['ciCode'])].index)
        ci_df.reset_index(drop=True, inplace=True)
        ci_df.drop(['ciCode1', 'ciCode2'], axis=1, inplace=True)
        ci_df = ci_df[['ciSeq', 'ciCode', 'ciName', 'ciMinGong', 'ciInOut', 'ciPlaceCode', 'ciDuty', 'ciOperCode']]

        df = pd.read_csv('/home/ubuntu/Pipeline_project/raw_data/loc.csv', index_col=0)
        df['ciSeq'] = df['ciSeq'].astype('int')
        new = pd.merge(ci_df, df, how='left', on='ciSeq')
        new.dropna(subset=['lat'], inplace=True)
        new.sort_values(by='ciSeq', inplace= True)
        new.reset_index(drop=True, inplace=True)
        new.rename(columns = {'addr':'ciAddr'},inplace=True)
        new = new[['ciSeq', 'ciCode', 'ciName', 'ciMinGong', 'ciInOut', 'ciPlaceCode', 'ciDuty', 'ciOperCode', 'ciAddr', 'lat', 'lng']]
        new.set_index('ciSeq', drop=True, inplace=True)
        new.to_csv("/home/ubuntu/Pipeline_project/data/ci_all.csv")

        file = '/home/ubuntu/Pipeline_project/data/ci_all.csv'
        js_file = '/home/ubuntu/Pipeline_project/data/ci_all.json'
        csv_to_json(file, js_file)
        sb.dbgout("ci_all DATA LOCAL STORAGE SUCCESS")
    except Exception as ex:
        sb.dbgout(f"ci_all FAIL -> {str(ex)}")

def ci_GMap():
    # """구글 API 사용 !!!! 절대 돌리지 말 것"""
    # df = pd.read_csv('./raw_data/ci_df.csv', index_col=0)
    # addr = df['ciAddrCity'] + df['ciAddr']
    # lat = []
    # lng = []
    # for ad in addr:
    #     url = f'https://maps.googleapis.com/maps/api/geocode/json?address={ad}&key={google_key}'
    #     res = requests.get(url).json()
    #     try:
    #         lat.append(res["results"][0]["geometry"]["location"]["lat"])
    #         lng.append(res["results"][0]["geometry"]["location"]["lng"])
    #     except:
    #         lat.append("")
    #         lng.append("")

    # loc = pd.DataFrame([lat, lng]).T
    # loc.columns = ['lat', 'lng']
    # loc.to_csv('./raw_data/loc.csv')
    # new_df = pd.DataFrame([df, loc], axis=1)
    # df = new_df.drop(new_df[new_df['lat'].isnull()].index)
    # df.reset_index(drop=True, inplace=True)
    # df.sort_values(by = 'ciSeq', inplace=True)

    # a = ['-'] * len(df)
    # df['ciCode'] = df['ciCode1'].astype('str') + a + df['ciCode2'].astype('str')
    # df = df.drop(df[df.duplicated(['ciCode'])].index)
    # df.reset_index(drop=True, inplace=True)
    # df.drop(['ciCode1', 'ciCode2', 'ciAddrCity', 'ciAddr'], axis=1, inplace=True)
    # df = df[['ciSeq', 'ciRegion', 'ciCode', 'ciName', 'ciMinGong', 'ciInOut', 'ciPlaceCode', 'ciDuty', 'ciOperCode', 'lat', 'lng']]
    # df.set_index('ciSeq', drop=True, inplace=True)
    # df.to_csv('./data/ci_all.csv')

    # file = './data/ci_all.csv'
    # js_file = './data/ci_all.json'
    # csv_to_json(file, js_file)
    # sb.dbgout("ci_GMap api success")
    pass

def ci_KaMap():
    # """ (kakao api 사용) 좌표로 정확한 주소 가지고 오기 """
    # df = pd.read_csv("./data/ci_all.csv", index_col=0)
    # url = 'https://dapi.kakao.com/v2/local/geo/coord2address.json'
    # headers = {"Authorization" : f"KakaoAK {kakao_key}"}
    # addr = []
    # x = df['lng']
    # y = df['lat']
    # for i in range(len(x)):
    #     params = {'x' : x[i], 'y': y[i]}
    #     res = requests.get(url, params=params, headers=headers)
    #     if res.status_code != 200:
    #         sb.dbgout(f"ci_inspection api {res.status_code}")
    #     addr.append(res.json()['documents'][0]['address']['address_name'] if res.json()['documents'] else None)
    # df['addr'] = addr
    # df.dropna(subset=['addr'], inplace=True)
    # df = df[['ciSeq', 'addr', 'lat', 'lng']]
    # df['ciSeq'] = df['ciSeq'].astype('int')
    # df.sort_values(by='ciSeq', inplace=True)
    # df.reset_index(drop=True, inplace=True)
    # df.to_csv("./raw_data/loc.csv")
    pass

def ci_accident():
    """ 전국 놀이시설 사고 정보 """
    try:
        cahAction = [] # 사고 조치
        cahContent = [] # 사고 처리 내용
        cahDate = [] # 사고일자
        cahSeq = [] # 사고 일련번호
        ciCode1 = [] # 놀이시설 코드1
        ciCode2 = [] # 놀이시설 코드2
        cahCode = [] # 사고원인 코드

        cnt = 1
        while True:
            url = f"http://openapi.cpf.go.kr/openapi/service/rest/ChildPlyFcltSafeAcdntInfoService/getSafeAcdntInfo?ServiceKey={key}&numOfRows=1000&pageNo={cnt}"
            res = requests.get(url)
            if res.status_code != 200:
                sb.dbgout(f"ci_accident API Response Error!(Code : {res.status_code})")
                break
            res.encoding = "UTF-8"
            html = res.text
            soup = bs(html, 'html.parser')
            total = int(soup.select_one("totalcount").text)
            items = soup.select_one("items")
            for item in items:
                cahAction.append(item.select_one("cahAction").text)
                cahContent.append(item.select_one("cahContent").text)
                cahDate.append(item.select_one("cahDate").text)
                cahSeq.append(item.select_one("cahSeq").text)
                ciCode1.append(item.select_one("ciCode1").text if item.select_one("ciCode1") else None)
                ciCode2.append(item.select_one("ciCode2").text if item.select_one("ciCode2") else None)
                cahCode.append(item.select_one("code22").text)
            if cnt == math.ceil(total/1000):
                break
            cnt += 1
            
        df = pd.DataFrame([cahAction, cahContent, cahDate, cahSeq, ciCode1, ciCode2, cahCode]).T
        df.columns = ['cahAction', 'cahContent', 'cahDate', 'cahSeq', 'ciCode1', 'ciCode2', 'cahCode']
        df.drop(df[df['ciCode1'].isnull()].index, inplace=True)
        df['ciCode1'] = df['ciCode1'].astype('int')
        df['ciCode2'] = df['ciCode2'].astype('int')
        df.reset_index(drop=True, inplace= True)

        a = ['-'] * len(df)
        df['ciCode'] = df['ciCode1'].astype('str') + a + df['ciCode2'].astype('str')
        df.drop(['ciCode1', 'ciCode2'], axis=1, inplace=True)
        df = df[['cahSeq', 'cahDate', 'cahContent', 'cahAction', 'cahCode', 'ciCode']]
        df['cahSeq'] = df['cahSeq'].astype('int')
        df.sort_values(by='cahSeq', inplace=True)
        df.set_index('cahSeq', drop= True, inplace= True)
        df['cahContent'] = df['cahContent'].apply(lambda x: re.sub(r"\s+", " ", x))
        df['cahAction'] = df['cahAction'].apply(lambda x: re.sub(r"\s+", " ", x))
        df.to_csv("/home/ubuntu/Pipeline_project/data/ci_accident.csv")

        file = '/home/ubuntu/Pipeline_project/data/ci_accident.csv'
        js_file = '/home/ubuntu/Pipeline_project/data/ci_accident.json'
        csv_to_json(file, js_file)
        sb.dbgout("ci_accident DATA LOCAL STORAGE SUCCESS")
    except Exception as ex:
        sb.dbgout(f"ci_accident FAIL! -> {str(ex)}")

def ci_inst():
    """ 전국 놀이기구 정보 """
    try:
        caSeq = []          # 기구일련번호
        caInstNo = []       # 관리번호
        caCuser = []        # 기구확인자ID
        caCdate = []        # 기구확인시점
        ciInstCode = []      # 놀이기구유형코드
        ciInst = []         # 놀이기구유형코드명
        ciSeq = []          # 놀이시설일련번호
        caInstCode = [] # 설치검사번호

        cnt = 1
        while True:
            url = f'http://openapi.cpf.go.kr/openapi/service/rest/ChildPlyFcltUtensilInfoService/getFcltUtensilInfo?ServiceKey={key}&numOfRows=10000&pageNo={cnt}'
            res = requests.get(url)
            if res.status_code != 200:
                sb.dbgout(f"ci_inst API Response Error!(Code : {res.status_code})")
                break
            res.encoding='UTF-8'
            html = res.text
            soup = bs(html, 'html.parser')
            total = int(soup.select_one("totalcount").text)
            items = soup.select_one("items")
            for item in items:
                ciSeq.append(item.select_one("ciSeq").text)
                caSeq.append(item.select_one("caSeq").text)
                caInstNo.append(item.select_one("caInstNo").text if item.select_one("caInstNo") else None)
                caCuser.append(item.select_one("caCuser").text if item.select_one("caCuser") else None)
                caCdate.append(item.select_one("caCdate").text if item.select_one("caCdate") else None)
                ciInstCode.append(item.select_one("code5").text if item.select_one("code5") else None)
                ciInst.append(item.select_one("name5").text if item.select_one("name5") else None)
                caInstCode.append(item.select_one("caInstCode").text if item.select_one("caInstCode") else None)
            if cnt == math.ceil(total/10000):
                break
            cnt += 1

        df = pd.DataFrame([caSeq, caInstNo, caCuser, caCdate, ciInstCode, ciInst, ciSeq, caInstCode]).T
        df.columns = ['caSeq', 'caInstNo','caCuser', 'caCdate', 'ciInstCode', 'ciInst', 'ciSeq', 'caInstCode']
        df['ciSeq'] = df['ciSeq'].astype('int')
        df.sort_values(by='ciSeq', inplace=True)
        df.set_index('ciSeq', drop=True, inplace=True)
        df.to_csv('/home/ubuntu/Pipeline_project/raw_data/ci_inst_raw.csv')

        file = '/home/ubuntu/Pipeline_project/raw_data/ci_inst_raw.csv'
        js_file = '/home/ubuntu/Pipeline_project/raw_data/ci_inst_raw.json'
        csv_to_json(file, js_file)
        sb.dbgout("ci_inst DATA LOCAL STORAGE SUCCESS")
    except Exception as ex:
        sb.dbgout(f"ci_inst FAIL! -> {str(ex)}")

def ci_inspection():
    # 정윤 작성
    """ 놀이시설 점검 여부 정보 """
    try:
        ciSeq = []
        ctmTestName = []    # 검사종류코드명
        ciInstallDate = []  # 설치일자
        ctmValidDate = []   # 유효기간
        ctmPass = []        # 합부판정(YN)
        ctmTestDate = []    # 검사일자
        ctmDeptCode = []    # A:한국건설생활환경시험연구원,B:한국기계전기전자시험연구원,C:비상재난안전협회,ETC:기타
        ctmNotiDate = []    # 판정일자
        ctmRequNo = []
        ctmReviewAt = []

        cnt = 1
        while True:
            url = f"http://openapi.cpf.go.kr/openapi/service/rest/ChildPlyFcltSafeInspctInfoService/getFcltSafeInspctInfo?serviceKey={key}&numOfRows=10000&pageNo={cnt}"
            res = requests.get(url)
            if res.status_code != 200:
                sb.dbgout(f"ci_inspection API Response Error!(Code : {res.status_code})")
                break
            res.encoding = "UTF-8"
            html = res.text
            soup = bs(html, 'html.parser')
            total = int(soup.select_one("totalCount").text)
            items = soup.select_one("items")
            for item in items:
                ciSeq.append(item.select_one("ciSeq").text if item.select_one("ciSeq") else None)
                ctmTestName.append(item.select_one('name6').text if item.select_one('name6') else None)
                ciInstallDate.append(item.select_one('ciInstall').text if item.select_one('ciInstall') else None)
                ctmValidDate.append(item.select_one('ctmValidDate').text if item.select_one('ctmValidDate') else None)
                ctmPass.append(item.select_one('ctmPass').text if item.select_one('ctmPass') else None)
                ctmTestDate.append(item.select_one('ctmTestDate').text if item.select_one('ctmTestDate') else None)
                ctmDeptCode.append(item.select_one('code16').text if item.select_one('code16') else None)
                ctmNotiDate.append(item.select_one('ctmNotiDate').text if item.select_one('ctmNotiDate') else None)
                ctmRequNo.append(item.select_one('ctmRequNo').text if item.select_one('ctmRequNo') else None)
                ctmReviewAt.append(item.select_one('ctmReviewAt').text if item.select_one('ctmReviewAt') else None)
            if cnt == math.ceil(total/10000):
                    break
            cnt += 1

        df_s = pd.DataFrame([ctmRequNo, ctmTestName, ciInstallDate, ctmValidDate, ctmPass, ctmTestDate, ctmDeptCode, ctmNotiDate, ctmReviewAt, ciSeq]).T
        df_s.columns = ['ctmRequNo', 'ctmTestName', 'ciInstallDate', 'ctmValidDate', 'ctmPass', 'ctmTestDate', 'ctmDeptCode', 'ctmNotiDate', 'ctmReviewAt', 'ciSeq']
        df_s.dropna(subset=['ctmRequNo'], inplace=True)
        df_s['ctmNotiDate'] = pd.to_datetime(df_s['ctmNotiDate'])
        df_s.drop(df_s[df_s['ctmNotiDate'].isnull()].index, inplace=True)
        df_s.sort_values(by='ctmNotiDate', ascending=False, inplace=True)
        df_s.set_index('ctmRequNo', drop=True, inplace=True)
        df_s.to_csv('/home/ubuntu/Pipeline_project/raw_data/ci_inspection_raw.csv')

        file = '/home/ubuntu/Pipeline_project/raw_data/ci_inspection_raw.csv'
        js_file = '/home/ubuntu/Pipeline_project/raw_data/ci_inspection_raw.json'
        csv_to_json(file, js_file)
        sb.dbgout("ci_inspection DATA LOCAL STORAGE SUCCESS")
    except Exception as ex:
        sb.dbgout(f"ci_inspection FAIL! -> {str(ex)}")

def ci_manager():
    # 정윤 작성
    """ 전국 어린이놀이시설 관리자 정보 """
    try:
        ciSeq = [] # 놀이시설일련번호
        ciIndGroup = [] # 개인단체 구분코드
        csStart = [] # 시작일자
        csEnd = [] # 종료일자
        csSeq = [] # 시설관계자일련번호

        cnt = 1
        while True:
            url = f"http://openapi.cpf.go.kr/openapi/service/rest/ChildPlyFcltMngrInfoService/getFcltManageInfo?serviceKey={key}&numOfRows=10000&pageNo={cnt}"
            res = requests.get(url)
            if res.status_code != 200:
                sb.dbgout(f"ci_manager API Response Error!(Code : {res.status_code})")
                break
            res.encoding = "UTF-8"
            html = res.text
            soup = bs(html, 'html.parser')
            total = int(soup.select_one("totalCount").text)
            items = soup.select_one("items")
            for item in items:
                ciSeq.append(item.select_one("ciSeq").text if item.select_one("ciSeq") else None)
                ciIndGroup.append(item.select_one("name17").text if item.select_one("name17") else None)
                csStart.append(item.select_one("csStart").text if item.select_one("csStart") else None)
                csEnd.append(item.select_one("csEnd").text if item.select_one("csEnd") else None)
                csSeq.append(item.select_one("csSeq").text if item.select_one("csSeq") else None)
            if cnt == math.ceil(total/10000):
                    break
            cnt += 1

        col = ['ciSeq', 'ciIndGroup', 'csStart','csEnd','csSeq']
        df_m = pd.DataFrame([ciSeq, ciIndGroup, csStart, csEnd, csSeq]).T
        df_m.columns = col
        df_m.dropna()
        df_m.reset_index(drop=True, inplace=True)
        df_m['csSeq'] = df_m['csSeq'].astype('int')
        df_m['ciSeq'] = df_m['ciSeq'].astype('int')
        df_m.sort_values(by=['csSeq', 'ciSeq'], inplace=True)
        df_m.set_index('csSeq', drop=True, inplace=True)
        df_m.to_csv("/home/ubuntu/Pipeline_project/raw_data/ci_manager_raw.csv")

        file = '/home/ubuntu/Pipeline_project/raw_data/ci_manager_raw.csv'
        js_file = '/home/ubuntu/Pipeline_project/raw_data/ci_manager_raw.json'
        csv_to_json(file, js_file)
        sb.dbgout("ci_manager DATA LOCAL STORAGE SUCCESS")
    except Exception as ex:
        sb.dbgout(f"ci_manager FAIL! -> {str(ex)}")

def cah_cause():
    """ 시설 설치 위치 코드 정보 """
    try:
        cahCode = []
        cahCodeName = []

        url = f'http://openapi.cpf.go.kr/openapi/service/rest/CodeInfoService/getPlyFcltCmmnCodeInfo?serviceKey={key}&numOfRows=100&codeId=CID022'
        res = requests.get(url)
        if res.status_code != 200:
            sb.dbgout(f"cah_cause API Response Error!(Code : {res.status_code})")
        res.encoding='UTF-8'
        html = res.text
        soup = bs(html, 'html.parser')
        items = soup.select_one("items")
        for item in items:
            cahCode.append(item.select_one('code').text)
            cahCodeName.append(item.select_one('codeNm').text)

        df = pd.DataFrame([cahCode, cahCodeName]).T
        df.columns = ['cahCode', 'cahCodeName']
        df.set_index('cahCode', drop=True, inplace= True)
        df.to_csv('/home/ubuntu/Pipeline_project/data/cah_cause.csv')

        file = '/home/ubuntu/Pipeline_project/data/cah_cause.csv'
        js_file = '/home/ubuntu/Pipeline_project/data/cah_cause.json'
        csv_to_json(file, js_file)
        sb.dbgout("cah_cause DATA LOCAL STORAGE SUCCESS")
    except Exception as ex:
        sb.dbgout(f"cah_cause FAIL! -> {str(ex)}")

def ci_operCode():
    """ 시설 운영 코드 정보 """
    try:
        ciOperCode = []
        opCodeName = []
        columns = ['ciOperCode', 'opCodeName']

        url = f'http://openapi.cpf.go.kr/openapi/service/rest/CodeInfoService/getPlyFcltCmmnCodeInfo?serviceKey={key}&numOfRows=10&codeId=CID002'
        res = requests.get(url)
        if res.status_code != 200:
            sb.dbgout(f"ci_operCode API Response Error!(Code : {res.status_code})")
        res.encoding='UTF-8'
        html = res.text
        soup = bs(html, 'html.parser')
        items = soup.select_one("items")
        for item in items:
            ciOperCode.append(item.select_one('code').text)
            opCodeName.append(item.select_one('codeNm').text)

        df = pd.DataFrame([ciOperCode, opCodeName]).T
        df.columns = columns
        df.set_index('ciOperCode', drop=True, inplace=True)
        df.to_csv('/home/ubuntu/Pipeline_project/data/ci_operCode.csv')

        file = '/home/ubuntu/Pipeline_project/data/ci_operCode.csv'
        js_file = '/home/ubuntu/Pipeline_project/data/ci_operCode.json'
        csv_to_json(file, js_file)
        sb.dbgout("ci_operCode DATA LOCAL STORAGE SUCCESS")
    except Exception as ex:
        sb.dbgout(f"ci_operCode FAIL! -> {str(ex)}")

def ci_placeCode():
    """ 시설 설치 위치 코드 정보 """
    try:
        ciPlaceCode = []
        plCodeName = []

        url = f'http://openapi.cpf.go.kr/openapi/service/rest/CodeInfoService/getPlyFcltCmmnCodeInfo?serviceKey={key}&numOfRows=100&codeId=CID001'
        res = requests.get(url)
        if res.status_code != 200:
            sb.dbgout(f"ci_placeCode API Response Error!(Code : {res.status_code})")
        res.encoding='UTF-8'
        html = res.text
        soup = bs(html, 'html.parser')
        items = soup.select_one("items")
        for item in items:
            ciPlaceCode.append(item.select_one('code').text)
            plCodeName.append(item.select_one('codeNm').text)

        df = pd.DataFrame([ciPlaceCode, plCodeName]).T
        df.columns = ['ciPlaceCode', 'plCodeName']
        df.set_index('ciPlaceCode', drop=True, inplace= True)
        df.to_csv('/home/ubuntu/Pipeline_project/data/ci_placeCode.csv')

        file = '/home/ubuntu/Pipeline_project/data/ci_placeCode.csv'
        js_file = '/home/ubuntu/Pipeline_project/data/ci_placeCode.json'
        csv_to_json(file, js_file)
        sb.dbgout("ci_placeCode DATA LOCAL STORAGE SUCCESS")
    except Exception as ex:
        sb.dbgout(f"ci_placeCode FAIL! -> {str(ex)}")

def hdfsmsg():
    try:
        os.system('hdfs dfs -copyFromLocal -f /home/ubuntu/Pipeline_project/* /home/hjyoon/Pipeline_project/')
        sb.dbgout(f"ALL FILES HDFS STORAGE DONE")
    except Exception as ex:
        sb.dbgout(f"HDFS STORAGE FAIL! {str(ex)}")

#=================================================
#                      Python                    #
#=================================================

ciAll = PythonOperator(
    task_id='ciAll',
    python_callable=ci_all,
    dag=dag
)

ciGoogleMap = PythonOperator(
    task_id='ciGoogleMap',
    python_callable=ci_GMap,
    dag=dag
)

ciAcci = PythonOperator(
    task_id='ciAcci',
    python_callable=ci_accident,
    dag=dag
)

ciInst = PythonOperator(
    task_id='ciInst',
    python_callable=ci_inst,
    dag=dag
)

ciInspect = PythonOperator(
    task_id='ciInspect',
    python_callable=ci_inspection,
    dag=dag
)

ciManage =  PythonOperator(
    task_id='ciManage',
    python_callable=ci_manager,
    dag=dag
)

cahCause = PythonOperator(
    task_id='cahCause',
    python_callable=cah_cause,
    dag=dag
)

ciOper = PythonOperator(
    task_id='ciOper',
    python_callable=ci_operCode,
    dag=dag
)

ciPlace = PythonOperator(
    task_id='ciPlace',
    python_callable=ci_placeCode,
    dag=dag
)

#=================================================
#                      Hadoop                    #
#=================================================

checkJPS = BashOperator(
    task_id='checkJPS',
    bash_command="""
                    if [ -z $(jps | grep "ResourceManager") ] || [ -z $(jps | grep NodeManager) ]
                    then echo "Not Running"; start-yarn.sh
                    else echo "Running"
                    fi

                    if [ -z $(jps | grep "NameNode") ] || [ -z $(jps | grep "DataNode") ]
                    then echo "Not Running"; start-dfs.sh
                    else echo "Running"
                    fi
                """
)

toHDFS = PythonOperator(
    task_id='toHDFS',
    python_callable=hdfsmsg,
    dag=dag
)

#=================================================
#                      Spark                     #
#=================================================

ciAllSpark = SparkSubmitOperator(
    task_id='ciAllSpark',
    application='/home/ubuntu/airflow/dags/spark_code/ci_all.py',
    conn_id='spark_default',
    dag=dag
)

ciAcciSpark = SparkSubmitOperator(
    task_id='ciAcciSpark',
    application='/home/ubuntu/airflow/dags/spark_code/ci_accident.py',
    conn_id='spark_default',
    dag=dag
)

ciInstSpark = SparkSubmitOperator(
    task_id='ciInstSpark',
    application='/home/ubuntu/airflow/dags/spark_code/ci_inst.py',
    conn_id='spark_default',
    dag=dag
)

ciManageSpark = SparkSubmitOperator(
    task_id='ciManageSpark',
    application='/home/ubuntu/airflow/dags/spark_code/ci_manager.py',
    conn_id='spark_default',
    dag=dag
)

cahCauseSpark = SparkSubmitOperator(
    task_id='cahCauseSpark',
    application='/home/ubuntu/airflow/dags/spark_code/ci_cah_cause.py',
    conn_id='spark_default',
    dag=dag
)

ciFloorSpark = SparkSubmitOperator(
    task_id='ciFloorSpark',
    application='/home/ubuntu/airflow/dags/spark_code/ci_floor_info.py',
    conn_id='spark_default',
    dag=dag
)

ciOperSpark = SparkSubmitOperator(
    task_id='ciOperSpark',
    application='/home/ubuntu/airflow/dags/spark_code/ci_operation.py',
    conn_id='spark_default',
    dag=dag
)

ciPlaceSpark = SparkSubmitOperator(
    task_id='ciPlaceSpark',
    application='/home/ubuntu/airflow/dags/spark_code/ci_place_info.py',
    conn_id='spark_default',
    dag=dag
)


#=================================================
#                      Slack                     #
#=================================================

slack = PythonOperator(
    task_id='sendmsg',
    python_callable=sb.dbgout,
    op_args=['ALL DONE!'],
    dag=dag
)

[ciAll >> ciGoogleMap] >> checkJPS >> toHDFS >> ciAllSpark >> slack
[ciAcci, ciInst, ciInspect, ciManage, cahCause, ciOper, ciPlace] >> checkJPS >> toHDFS >> [ciAcciSpark, ciInstSpark, ciManageSpark, cahCauseSpark, ciFloorSpark, ciOperSpark, ciPlaceSpark]  >> slack
