#-*- coding: utf-8 -*-
import sys
sys.path.append('/usr/local/lib/python3.7/dist-packages')
import requests
from datetime import datetime
from pyspark.sql import SparkSession

myToken = ''

def post_message(token, channel, text) :
    response = requests.post('https://slack.com/api/chat.postMessage',
                            headers = {'Authorization' : 'Bearer '+token},
                            data = {'channel' : channel, 'text' : text})
    print(response)

def dbgout(message):
    """오류메세지를 부탁해!"""
    strbuf = datetime.now().strftime('[%m/%d %H:%M:%S] ') + message
    post_message(myToken, '#pipeline', strbuf)

# 정윤 작성
try :
    spark = SparkSession.builder.master("yarn").appName("ci_accident").getOrCreate()

    ci_accident = spark.read.option("header","true").csv("/home/hjyoon/Pipeline_project/data/ci_accident.csv")
    ci_accident.createOrReplaceTempView("ci_accident")
    ci_accident = spark.sql("select cast(cahseq as integer) as cahseq, cahdate, cahcontent, cahaction, cahCode, cicode from ci_accident where cahCode LIkE 'R%' order by cahseq")
    ci_accident.createOrReplaceTempView("ci_accident")

    ci_accident.write.format("csv").mode("overwrite").save("./project_data/ci_accident")
    ci_accident.coalesce(1).write.format("csv").mode("overwrite").option("header","true").save("./project/ci_accident")

    user="root"
    password="1234"
    url="jdbc:mysql://localhost:3306/children"
    driver="com.mysql.cj.jdbc.Driver"
    dbtable="ciaccident"

    ci_accident.write.mode("overwrite").option("truncate","true").jdbc(url, dbtable, properties={"driver": driver, "user": user, "password": password})
    dbgout("ci_accident SPARK-SUBMIT SUCCESS")
except Exception as ex:
    dbgout(f"ci_accident SPARK-SUBMIT FAIL! -> {str(ex)}")
