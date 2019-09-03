import re
from pyspark.sql import SparkSession
from pyspark.sql.types import DateType
from pyspark.sql import functions as f
from pyspark.sql import types as t
from pyspark.sql import Row
from pyspark.sql.types import *
from pyspark.sql.functions import col

spark = SparkSession\
    .builder\
    .appName("Python Spark SQL basic example")\
    .config("spark.some.config.option", "some-value")\
    .getOrCreate()
spark.sparkContext.setLogLevel('ERROR')
spark.conf.set('spark.sql.session.timeZone', 'Europe/Berlin')

df = spark.read.json('./jsonsplit/xaa.jsonl').cache()

col_names  = df.columns
regex = re.compile('\d+')
tmp = [y for x in [re.findall(regex, c) for c in col_names] for y in x]
tmp_index = spark.createDataFrame(list(map(lambda x: Row(index=x), tmp))).distinct().sort(col('index')).collect()
index = [int(i.asDict()['index']) for i in tmp_index]

from pyspark.sql.functions import lit

main = df.select('*').cache()

all_columns_df = df.select('*')

for i in index:
   col_name = 'body_pr' + str(i) + 'ca'
   if not col_name in col_names:
       all_columns_df = all_columns_df.withColumn(col_name, lit(None))    
   col_name = 'body_pr' + str(i) + 'cc'
   if not col_name in col_names:
       all_columns_df = all_columns_df.withColumn(col_name, lit(None))    
   col_name = 'body_pr' + str(i) + 'id'
   if not col_name in col_names:
       all_columns_df = all_columns_df.withColumn(col_name, lit(None))    
   col_name = 'body_pr' + str(i) + 'nm'
   if not col_name in col_names:
       all_columns_df = all_columns_df.withColumn(col_name, lit(None))    
   col_name = 'body_pr' + str(i) + 'pr'
   if not col_name in col_names:
       all_columns_df = all_columns_df.withColumn(col_name, lit(None))    
   col_name = 'body_pr' + str(i) + 'qt'
   if not col_name in col_names:
       all_columns_df = all_columns_df.withColumn(col_name, lit(None))    
   col_name = 'body_pr' + str(i) + 'va'
   if not col_name in col_names:
       all_columns_df = all_columns_df.withColumn(col_name, lit(None))


all_columns_df

bodies_schema = StructType([StructField('ms_id', StringType()),StructField('prca', StringType()),StructField('prcc', StringType()),StructField('prid', StringType()),StructField('prnm', StringType()),StructField('prpr', StringType()),StructField('prqt', StringType()),StructField('prva', StringType())])


bodies = all_columns_df.rdd \
   .flatMap(lambda x: [Row(
           ms_id=x.message_id, prca=x['body_pr'+str(c)+'ca'], 
           prcc=x['body_pr'+str(c)+'cc'], prid=x['body_pr'+str(c)+'id'], 
           prnm=x['body_pr'+str(c)+'nm'], prpr=x['body_pr'+str(c)+'pr'], 
           prqt=x['body_pr'+str(c)+'qt'], prva=x['body_pr'+str(c)+'va']) 
           for c in index]).filter(lambda x:x.ms_id != None and (x.prca != None or x.prcc != None or x.prid != None or x.prnm != None or x.prpr != None or x.prqt != None or x.prva != None)).toDF(bodies_schema)


result = main.alias('main') \
   .join(bodies.alias('bodies'), col('main.message_id') == col('bodies.ms_id'), 'left_outer').cache()


result = result.drop('ms_id')
result.printSchema()
result.groupBy('message_id').count().sort(col('count').desc()).show(10)
result.select('prca', 'prcc', 'prid', 'prnm', 'prpr', 'prqt', 'body_t', 'body_pa', 'body_cid').filter(result['message_id'] == 'c1221ba6-b9e8-11e9-9682-1bf3885e65d7').filter(result['body_pa'] == 'purchase').show(50, truncate=False)
