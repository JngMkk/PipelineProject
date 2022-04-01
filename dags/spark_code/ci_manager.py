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
    spark = SparkSession.builder.master("yarn").appName("ci_manager").getOrCreate()
    
    ci_manager = spark.read.option("header","true").csv("/home/hjyoon/Pipeline_project/raw_data/ci_manager_raw.csv")
    ci_manager.createOrReplaceTempView("ci_manager")
    
    ci_manager = spark.sql("select cast(csSeq as integer) as csSeq,ciSeq,ciIndgroup from ci_manager where ciindgroup is not null order by csSeq")
    ci_manager = ci_manager.dropDuplicates(['csSeq']).orderBy('csSeq')
    ci_manager.createOrReplaceTempView("ci_manager")
    
    ci_manager.write.option("header","true").format("csv").mode("overwrite").save("./project_data/ci_manager")
    ci_manager.coalesce(1).write.format("csv").mode("overwrite").save("./project/ci_manager")
    
    user="root"
    password="1234"
    url="jdbc:mysql://localhost:3306/children"
    driver="com.mysql.cj.jdbc.Driver"
    dbtable="cimanager"
    
    ci_manager.write.mode("overwrite").option("truncate","true").jdbc(url, dbtable, properties={"driver": driver, "user": user, "password": password})
    dbgout("ci_manager SPARK-SUBMIT SUCCESS")
except Exception as ex:
    dbgout(f"ci_manager SPARK-SUBMIT FAIL! -> {str(ex)}")
