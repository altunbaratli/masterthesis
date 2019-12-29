#!/usr/bin/env python
# coding: utf-8

# In[74]:


from pyspark import SparkContext, SQLContext, SparkConf
from pyspark.sql import SparkSession
from pyspark.sql import Row
from pyspark.sql import functions as F
from pyspark.sql.functions import from_utc_timestamp
import os

spark.stop()

conf = SparkConf().setAppName("PySpark App").setMaster("local").set("spark.jars", "C:\\Program Files\\PostgreSQL\\12\\postgresql-42.2.8.jar")
sc = SparkContext(conf=conf)
sqlContext = SQLContext(sc)
spark = SparkSession     .builder     .appName("Python Spark SQL basic example").enableHiveSupport().getOrCreate()


# In[83]:


data_file_A ="C:\\Users\\AltunBaratli\\Desktop\\homeA-environmental\\2012-Jul-1.csv"
data_file_B ="C:\\Users\\AltunBaratli\\Desktop\\homeB-environmental\\2012-Jul-1.csv"
data_file_C ="C:\\Users\\AltunBaratli\\Desktop\\homeC-environmental\\2012-July-1.csv"

sdfData_A=spark.read.csv(data_file_A, header=False, sep=",").toDF("TimestampUTC","insideTemp_A","outsideTemp_A","insideHumidity_A","outsideHumidity_A","windSpeed_A","windDirectionDegrees_A","windGust_A","windGustDirectionDegrees_A","rainRate_A","rain_A","windChill_A","heatindex_A").cache()
sdfData_B=spark.read.csv(data_file_B, header=False, sep=",").toDF("TimestampUTC","insideTemp_B","outsideTemp_B","insideHumidity_B","outsideHumidity_B","windSpeed_B","windDirectionDegrees_B","windGust_B","windGustDirectionDegrees_B","rainRate_B","rain_B","windChill_B","heatindex_B").cache()
# sdfData_C=spark.read.csv(data_file_C, header=False, sep=",").toDF("TimestampUTC","insideTemp","outsideTemp","insideHumidity","outsideHumidity","windSpeed","windDirectionDegrees","windGust","windGustDirectionDegrees","rainRate","rain","windChill","heatindex").cache()


# In[76]:


sqltext = '''(SELECT * FROM public.test_table) t'''

jdbcDF = spark.read     .format("jdbc")     .option("url", "jdbc:postgresql://localhost:5432/postgres")     .option("dbtable", sqltext)     .option("user", os.environ['LOCAL_POSTGRES_USER'])     .option("password", os.environ['LOCAL_POSTGRES_PASS'])     .option("driver", "org.postgresql.Driver")     .load()

jdbcDF.show()


# In[65]:


df_A = sdfData_A.withColumn("TimestampUTC", F.from_unixtime(F.col("TimestampUTC"), 'yyyy-MM-dd HH:mm:ss.SS').cast("timestamp"))
df_B = sdfData_B.withColumn("TimestampUTC", F.from_unixtime(F.col("TimestampUTC"), 'yyyy-MM-dd HH:mm:ss.SS').cast("timestamp"))
df_B.show()


# In[124]:


df_join = sdfData_A.join(sdfData_B, ['TimestampUTC']).select('TimestampUTC', 'insideTemp_A', 'insideTemp_B')
df_join.printSchema()


# In[89]:


df_join.write     .format("jdbc")     .mode("overwrite")     .option("url", "jdbc:postgresql://localhost:5432/postgres")     .option("dbtable", "public.test_table5589")     .option("user", os.environ['LOCAL_POSTGRES_USER'])     .option("password", os.environ['LOCAL_POSTGRES_PASS'])     .option("driver", "org.postgresql.Driver")     .save()


# In[128]:


import requests
for row in df_join.limit(20).rdd.collect():
    pload = {'color':row.insideTemp_A.split('.', 1)[0],'make':row.insideTemp_B.split('.', 1)[0], 'model':'0', 'owner':'PySpark'}
    r = requests.post('http://localhost:8081/createCar',json = pload)
    print(r.text)


# In[130]:


import json
from urllib.request import urlopen
def convert_single_object_per_line(json_list):
    json_string = ""
    for line in json_list:
        json_string += json.dumps(line) + "\n"
    return json_string


def parse_dataframe(json_data):
    r = convert_single_object_per_line(json_data)
    mylist = []
    for line in r.splitlines():
        mylist.append(line)
    rdd = sc.parallelize(mylist)
#     df = sqlContext.jsonRDD(rdd)
    df = sqlContext.read.json(rdd)
    return df


url = "http://localhost:8081/queryallcars"
response = urlopen(url)
data = response.read()
json_data = json.loads(data)
df_json = parse_dataframe(json_data)
df_json.show()


# In[131]:


flattened = df_json.select("Key", "Record.color", "Record.make", "Record.model", "Record.decision")
flattened.show()


# In[132]:


flattened.write     .format("jdbc")     .mode("overwrite")     .option("url", "jdbc:postgresql://localhost:5432/postgres")     .option("dbtable", "public.test_table54")     .option("user", os.environ['LOCAL_POSTGRES_USER'])     .option("password", os.environ['LOCAL_POSTGRES_PASS'])     .option("driver", "org.postgresql.Driver")     .save()


# In[ ]:




