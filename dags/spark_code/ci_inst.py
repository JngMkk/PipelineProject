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

try:
  spark = SparkSession.builder.master("yarn").appName("ci_inst").getOrCreate()

  ci_inst = spark.read.option("header","true").csv("/home/hjyoon/Pipeline_project/raw_data/ci_inst_raw.csv")
  ci_inst.createOrReplaceTempView("ci_inst")

  ci_inst = spark.sql("select cast(caseq as integer) as caseq, ciseq, cainstno, cacuser, cacdate, ciinstcode, ciinst, cainstcode from ci_inst order by case when caseq is null then 0 else 1 end, caseq")
  ci_inst.createOrReplaceTempView("ci_inst")

  ci_inst.write.option("header","true").format("csv").mode("overwrite").save("./project_data/ci_inst")
  ci_inst.coalesce(1).write.format("csv").mode("overwrite").save("./project/ci_inst")

  user="root"
  password="1234"
  url="jdbc:mysql://localhost:3306/children"
  driver="com.mysql.cj.jdbc.Driver"
  dbtable="ciinst"

  ci_inst.write.mode("overwrite").option("truncate","true").jdbc(url, dbtable,   properties={"driver": driver, "user": user, "password": password})
  dbgout("ci_inst SPARK-SUBMIT SUCCESS")
except Exception as ex:
  dbgout(f"ci_inst SPARK-SUBMIT FAIL! -> {str(ex)}")
