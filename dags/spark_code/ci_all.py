#-*- coding: utf-8 -*-
import sys
sys.path.append('/usr/local/lib/python3.7/dist-packages')
import requests
from datetime import datetime
from pyspark.sql import SparkSession

# 정윤 작성
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
    spark = SparkSession.builder.master("yarn").appName("ci_all").getOrCreate()

    ci_inst = spark.read.option("header","true").csv("/home/hjyoon/Pipeline_project/raw_data/ci_inst_raw.csv")
    ci_inst.createOrReplaceTempView("ci_inst")

    ci_inspection = spark.read.option("header","true").csv("/home/hjyoon/Pipeline_project/raw_data/ci_inspection_raw.csv")
    ci_inspection.createOrReplaceTempView("ci_inspection")
    ci_inspection = spark.sql("select distinct(ciseq) as ciseq, last_value(ctmnotidate) over(partition by ciseq order by ctmnotidate rows between unbounded preceding and unbounded following) as ctmnotidate, ctmreviewat from ci_inspection where ctmnotidate is not null")
    ci_inspection.createOrReplaceTempView("ci_inspection")
    ci_inspection = spark.sql("select cast(ciseq as integer),ctmnotidate,ctmreviewat from ci_inspection order by ciseq")
    ci_inspection.createOrReplaceTempView("ci_inspection")

    ci_all = spark.read.option("header","true").csv("/home/hjyoon/Pipeline_project/data/ci_all.csv")
    ci_all.createOrReplaceTempView("ci_all")

    ci_all = spark.sql("""
    select cast(a.ciSeq as integer) as ciSeq, ciCode, a.ciName, a.ciMinGong, a.ciInOUt, a.ciPlaceCode, a.ciDuty, a.ciOperCode, a.ciAddr, cast(a.lat as float), cast(a.lng as float), i.ctmnotidate, i.ctmreviewat
    from ci_all a join ci_inspection i on a.ciSeq = i.ciSeq  
    order by ciSeq
    """)
    ci_all.createOrReplaceTempView("ci_all")

    ci_all = spark.sql("""
    select a.ciSeq,a.ciCode, a.ciName, a.ciMinGong, a.ciInOUt, a.ciPlaceCode, a.ciDuty, a.ciOperCode, a.ciAddr, cast(a.lat as float), cast(a.lng as float), a.ctmnotidate, a.ctmreviewat, i.ciInstCode AS ciFloorCode
    from ci_all a join ci_inst i on a.ciSeq = i.ciSeq
    where i.ciinst LIKE '충격%'
    order by a.ciSeq
    """)
        
    ci_all.createOrReplaceTempView("ci_all")
    ci_all = ci_all.dropDuplicates(['ciCode']).orderBy('ciSeq')    

    ci_all.write.format("csv").mode("overwrite").save("./project_data/ci_all")
    ci_all.coalesce(1).write.format("csv").mode("overwrite").option("header","true").save("./project/ci_all")
    
    user="root"
    password="1234"
    url="jdbc:mysql://localhost:3306/children"
    driver="com.mysql.cj.jdbc.Driver"
    dbtable="ciall"
    
    ci_all.write.mode("overwrite").option("truncate","true").jdbc(url, dbtable, properties={"driver": driver, "user": user, "password": password})
    dbgout("ci_all SPARK-SUBMIT SUCCESS")

except Exception as ex:
    dbgout(f"ci_all SPARK-SUBMIT FAIL! -> {str(ex)}")
