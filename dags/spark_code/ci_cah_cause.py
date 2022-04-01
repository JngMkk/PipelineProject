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
try:
    spark = SparkSession.builder.master("yarn").appName("ci_cah_cause").getOrCreate()
    
    ci_cah_cause = spark.read.option("header","true").csv("/home/hjyoon/Pipeline_project/data/cah_cause.csv")
    ci_cah_cause.createOrReplaceTempView("ci_cah_cause")
    
    ci_cah_cause.write.format("csv").mode("overwrite").save("./project_data/ci_cah_cause")
    ci_cah_cause.coalesce(1).write.option("header","true").format("csv").mode("overwrite").save("./project/ci_cah_cause")
    
    user="root"
    password="1234"
    url="jdbc:mysql://localhost:3306/children"
    driver="com.mysql.cj.jdbc.Driver"
    dbtable="cicahcause"
    
    ci_cah_cause.write.mode("overwrite").option("truncate","true").jdbc(url, dbtable, properties={"driver": driver, "user": user, "password": password})
    dbgout("cah_cause SPARK-SUBMIT SUCCESS")
except Exception as ex:
    dbgout(f"cah_cause SPARK-SUBMIT FAIL! -> {str(ex)}")
