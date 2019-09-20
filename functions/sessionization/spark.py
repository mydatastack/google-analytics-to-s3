from pyspark.sql import SparkSession
from pyspark.sql.types import DateType, StringType, StructType, StructField
from pyspark.sql import functions as f
from pyspark.sql.window import Window
from pyspark.sql import types as t
from pyspark.sql import Row
from pyspark.sql.window import Window
import sys
import re
import os

spark = SparkSession\
    .builder\
    .appName("Python Spark SQL basic example")\
    .getOrCreate()

#spark.sparkContext.setLogLevel('ERROR')
spark.conf.set('spark.driver.memory', '6g')
spark.conf.set('spark.sql.session.timeZone', 'Europe/Berlin')

df = spark.read.json('./non-ecommerce/*.jsonl')
print("The number of partitions after read:", df.rdd.getNumPartitions())

from datetime import datetime
tsfm = "%Y-%m-%d %H:%M:%S"
time_start = datetime.now().strftime("%Y-%m-%d %H:%M:%S") 
dt_start = datetime.strptime(time_start, tsfm) 
print("Start time: ", dt_start)
#################### Start importing dependencies ###############

columns_to_drop = [
        'body__cbt',
        'body__cst',
        'body__gbt',
        'body__gst',
        'body__r',
        'body__s',
        'body__u',
        'body__utma',
        'body__utmht',
        'body__utmz',
        'body__v',
        'body_a',
        'body_clt',
        'body_dit',
        'body_dns',
        'body_gjid',
        'body_gtm',
        'body_jid', # needed for linking data between doubleclick and ga
        'body_pdt', # page download times
        'body_plt', # page load times
        'body_rrt', # redirect response time
        'body_srt', # server response time
        'body_tcp', # tcp connect time
        ]


def parse_url(url: str) -> tuple:
    return urlparse.urlparse(url)

def path_is_empty(url: str) -> str:
    return url.path if len(str(url.path)) != 0 else None 

def extract_path_value(path: str) -> str:
    if path == None:
        return '' 
    else:
        return path 

def split_path(path: str) -> list:
    return path.split('/')[1:]

def construct_levels(p: list):
    path = list(filter(None, p))
    if len(path) == 1:
        return [str('/' + path[0]), '', '', ''] 
    if len(path) == 2:
        return [str('/' + path[0]), str('/' + path[1]), '', ''] 
    if len(path) == 3:
        return [str('/' + path[0]), str('/' + path[1]), str('/' + path[2]), '']
    if len(path) >= 4:
        return [str('/' + path[0]), str('/' + path[1]), str('/' + path[2]), str('/' + path[3])]
    else:
        return ['', '', '', '']
    
def parse_page_path(url):
    return pipe([
            parse_url,
            path_is_empty,
            extract_path_value,
            split_path,
            construct_levels,
            ]) (url)


def filter_tmp(xs: list) -> list:
    return xs
    return [
            sublist
            for sublist in xs if any(v for v in sublist) 
            ] 


################## End importing dependencies ################

df_columns = df.columns

required_columns = [
        "body_el",
        "body_ev",
        "body_pa",
        "body_dr",
        "body_fl",
        "body_cu",
        "body_col",
        "body_cos",
        "body_tcc",
        "body_ti",
        "body_tr",
        "body_ts",
        "body_tt"
        ]

def add_required_colum(column, df_columns):
    if not column in df_columns: 
        global df
        df = df.withColumn(column, f.lit(None))

for column in required_columns:
    add_required_colum(column, df_columns)

## start renaming the column body_t according to GA360
def hits_type(t: str) -> str:
    if t == 'pageview':
        return 'PAGE'
    elif t == 'screenview':
        return 'APPVIEW'
    elif t == 'event':
        return 'EVENT'
    elif t == 'transaction':
        return 'TRANSACTION'
    elif t == 'item':
        return 'ITEM'
    elif t == 'social':
        return 'SOCIAL'
    elif t == 'exception':
        return 'EXCEPTION'
    elif t == 'timing':
        return 'TIMING'
    else:
        return 'UNKNOWN'


udf_map_hits_type = f.udf(hits_type)

df = df.withColumn(
        'hits_type',
        udf_map_hits_type(df['body_t']))

## end renaming the column 

df_clean = df.drop(*columns_to_drop)
reparitioned_df_clean = df_clean

reparitioned_df_clean.createOrReplaceTempView('clicks')

required_columns_query = 'select *, null as body_el, null as body_ev from clicks'

with_columns = spark.sql(required_columns_query)

query = """ 
                select *,
                null as global_session_id, -- sum(is_new_session) over (order by body_cid, received_at_apig) as global_session_id,
                sum(is_new_session) over (partition by body_cid order by received_at_apig) as user_session_id
                from (
                    select *,
                    case when received_at_apig - last_event >= (60000 * 30)
                    or last_event is null
                    then 1 else 0 end as is_new_session
                from (
                    select *, 
                        from_unixtime(cast(received_at_apig/1000 as bigint)) as ts, 
                        lag(received_at_apig, 1) 
                        over (partition by body_cid order by received_at_apig) as last_event 
                        from clicks
                        ) last
                ) final
                where not body_t='adtiming' or not body_t='timing'

    """

sessions = spark.sql(query)
# 569123 count(body_cid) 

# filter out adtiming and timing events
sessions_clean = sessions.filter(~sessions['body_t'].isin(['adtiming', 'timing']))
sessions_clean.createOrReplaceTempView('sessions')
# 489102 count(body_cid) 

sessions_count = spark.sql('select count(body_cid) from sessions where ts between "2019-08-09" and "2019-08-10" and is_new_session="1"')
#sessions_count.show()

import sys

session_id_query = """ select *, sha(concat(a.body_cid, a.first_value, a.last_value)) as visit_id,
                        row_number() over (partition by body_cid order by received_at_apig asc) as event_sequence
from
          (
          select *, first_value(received_at_apig) over (partition by body_cid, user_session_id order by is_new_session desc) as first_value,
          last_value(received_at_apig) over (partition by body_cid, user_session_id) as last_value
          from sessions
          ) a
"""

from pyspark.sql.functions import spark_partition_id

def show_partition_id(df):
    return df.select("visit_id", spark_partition_id().alias("partition_id")).show(1000, False)

with_session_ids = spark.sql(session_id_query)
partitioned = with_session_ids
#show_partition_id(with_session_ids)
#show_partition_id(partitioned)
w = Window.partitionBy(f.col("visit_id"))

with_session_ids = partitioned\
    .withColumn(
            'total_revenue_per_session',
            f.when(f.col("is_new_session") == '1', 
                    f.sum(f.when((f.col("body_t") == 'event') & (f.col("body_pa") == 'purchase'), f.col("body_tr")).otherwise(f.lit(''))
                  ).over(w)).otherwise(f.lit('')))

import urllib

## start parsing the source
import urllib.parse as urlparse
from functools import partial, reduce

pipe = lambda fns: lambda x: reduce(lambda v, f: f(v), fns, x)

channels = [
        'utm_source', 
        'gclid', 
        'gclsrc', 
        'dclid', 
        'fbclid', 
        'mscklid', 
        'direct', 
        ]

def match(xs):
    return [s for s in xs if any(xz in s for xz in channel_list)]

def extract_query_value(xs):
    if len(xs) == 0:
        return 'direct=(direct)' 
    else:
        return xs[4]

def parse_url(url: str):
    return urlparse.urlparse(url)

def query_is_empty(url: str):
    return url if len(str(url.query)) != 0 else []

def split_item(item):
    return item.split('=')

def split_query(qr: str):
    query = qr.split('&')
    query_clean = [x for x in query if x and x.find('=') > 0]
    return dict(split_item(item) for item in query_clean)

def identify_channel(channel_list: list, qr: str):
    channel = [s for s in qr if any(xz in s for xz in channel_list)]
    if len(channel) == 0:
        return '(direct)'
    elif channel[0] == 'gclid' or channel[0] == 'gclsrc' or channel[0] == 'dclid':
        return 'google'
    elif channel[0] == 'fbclid':
        return 'facebook'
    elif channel[0] == 'mscklid':
        return 'bing'
    elif channel[0] == 'utm_source':
        return qr[channel[0]]
    elif channel[0] == 'direct':
        return '(direct)' 
    else:
        return '(not set)' 

def parse_dl_source(url):
    return pipe([
            parse_url,
            query_is_empty,
            extract_query_value,
            split_query,
            partial(identify_channel, channels),
            ]) (url)

def split_hostname(body_dr: str) -> str:
    hostname = parse_url(body_dr).netloc
    hostname_splitted = hostname.split('.')
    try:
        if 'www' in hostname_splitted:
            return hostname_splitted[1]
        elif len(hostname_splitted) == 3:
            return hostname_splitted[1]
        elif len(hostname_splitted) == 2:
            return hostname_splitted[0]
        else:
            return hostname
    except Exception as e:
        print(e)
        return hostname
 
def parse_dr_source(body_dl: str, body_dr: str):
    if body_dr.find('android-app') == 0:
        return body_dr.split('//')[1]
    hostname = split_hostname(body_dr) 
    empty_query_dl = len(query_is_empty(parse_url(body_dl))) == 0
    empty_query_dr = len(query_is_empty(parse_url(body_dr))) == 0  
    query_dl = split_query(extract_query_value(query_is_empty(parse_url(body_dl))))
    query_dr = split_item(extract_query_value(query_is_empty(parse_url(body_dr))))
    if hostname == 'googleadservices':
        return 'google'
    elif empty_query_dl and empty_query_dr: 
        return hostname 
    elif not empty_query_dl and 'utm_source' in query_dl:
        return query_dl['utm_source']
    elif not empty_query_dr:
        return hostname
    elif not empty_query_dl and 'ref' in query_dl:
        return query_dl['ref']
    elif not empty_query_dl:
        return identify_channel(channels, query_dl)
    else:
        return '(not set)'

def extract_source_source(is_new_session, body_dl, body_dr):
    if (is_new_session == 1 and body_dr is None):
        return parse_dl_source(body_dl) 
    elif (is_new_session == 1 and body_dr is not None):
        return parse_dr_source(body_dl, body_dr)
    else:
        return '(not set)' 

## end parsing the source

## start parsing the campaign

def identify_campaign(qr: dict):
    return qr['utm_campaign'] if 'utm_campaign' in qr else '(not set)'

def parse_source_campaign(url):
    return pipe([
            parse_url,
            query_is_empty,
            extract_query_value,
            split_query,
            identify_campaign,
            ]) (url)

def parse_dr_campaign(body_dl: str):
    empty_query = len(query_is_empty(parse_url(body_dl))) == 0
    query = split_query(extract_query_value(query_is_empty(parse_url(body_dl))))
    if empty_query: 
        return '(not set)' 
    elif not empty_query:
        return identify_campaign(query)
    else:
        return '(not set)'

def extract_source_campaign(is_new_session, body_dl, body_dr):
    if (is_new_session == 1 and body_dr is None):
        return parse_source_campaign(body_dl) 
    if (is_new_session == 1 and body_dr is not None):
        return parse_dr_campaign(body_dl)
    else:
        return '(not set)' 
## end parsing the campaign

## start parsing the medium

def identify_medium(qr: dict):
    if 'utm_medium' in qr:
        return qr['utm_medium']
    if 'gclid' in qr:
        return 'paid'
    else:
        return '(none)'

def parse_source_medium(url: str):
    return pipe([
            parse_url,
            query_is_empty,
            extract_query_value,
            split_query,
            identify_medium,
            ]) (url)

search_engines = [
        'google', 
        'yahoo', 
        'bing', 
        'aol', 
        'ask', 
        'comcast', 
        'nexttag', 
        'local', 
        ]

paid_channels = [
        'gclid', 
        'gclsrc', 
        'dclid', 
        'fbclid', 
        'mscklid', 
        ]

def match(xs):
    return [s for s in xs if any(xz in s for xz in paid_channel)]

def parse_dr_medium(body_dr: str, body_dl: str):
    hostname = body_dr.split('//')[-1].split('/')[0].split('.')[1]
    empty_query = len(query_is_empty(parse_url(body_dl))) == 0
    query = split_query(extract_query_value(query_is_empty(parse_url(body_dl))))

    if hostname == 'googleadservices':
        return 'paid'
    elif empty_query and hostname in search_engines:
        return 'organic'
    elif (empty_query and hostname not in search_engines) or (not empty_query and 'ref' in query):
        return 'referral'
    elif not empty_query and any(key in query for key in paid_channels):
        return 'paid'
    elif not empty_query and 'utm_medium' in query:
        return query['utm_medium']
    else:
        return '(none)'


def extract_source_medium(is_new_session, body_dl, body_dr):
    if (is_new_session == 1 and body_dr is None):
        return parse_source_medium(body_dl) 
    elif (is_new_session == 1 and body_dr is not None):
        return parse_dr_medium(body_dr, body_dl)
    else:
        return '(none)'
## end parsing the medium

## start parsing the keyword

def identify_keyword(qr: dict):
    if 'utm_term' in qr:
        return qr['utm_term']
    else:
        return '(not set)'

def parse_source_keyword(url: str):
    return pipe([
            parse_url,
            query_is_empty,
            extract_query_value,
            split_query,
            identify_keyword,
            ]) (url)

def parse_dr_keyword(body_dr: str, body_dl: str):
    hostname = body_dr.split('//')[-1].split('/')[0].split('.')[1]

    if hostname in search_engines:
        return '(not provided)'
    else:
        return '(not set)'

def extract_source_keyword(is_new_session, body_dl, body_dr, traffic_source_medium):
    if traffic_source_medium == 'organic':
        return '(not provided)'
    if (is_new_session == 1 and body_dr is None):
        return parse_source_keyword(body_dl) 
    else:
        return '(not set)'

## end parsing the keyword

## start parsing the adContent

def identify_ad_content(qr: dict):
    if 'utm_content' in qr:
        return qr['utm_content']
    else:
        return '(not set)'

def parse_source_ad_content(url: str):
    return pipe([
            parse_url,
            query_is_empty,
            extract_query_value,
            split_query,
            identify_ad_content,
            ]) (url)

def extract_source_ad_content(is_new_session, body_dl, body_dr):
    if (is_new_session == 1 and body_dr is None):
        return parse_source_ad_content(body_dl) 
    else:
        return '(not set)'

## end parsing the adContent

## start parsing is_true_direct

## end parsing is_true_direct

traffic_source_referral_path = f.udf(lambda x: x)
traffic_source_campaign = f.udf(extract_source_campaign)
traffic_source_source = f.udf(extract_source_source)
traffic_source_medium = f.udf(extract_source_medium)
traffic_source_keyword = f.udf(extract_source_keyword)
traffic_source_ad_content = f.udf(extract_source_ad_content)
traffic_source_is_true_direct = f.udf(lambda x: 'True' if x == '(direct)' else None)

with_session_ids = with_session_ids.withColumn(
    'traffic_source_source',
    traffic_source_source(
            with_session_ids['is_new_session'], 
            with_session_ids['body_dl'], 
            with_session_ids['body_dr']))

with_session_ids = with_session_ids.withColumn(
    'traffic_source_campaign',
    traffic_source_campaign(
            with_session_ids['is_new_session'], 
            with_session_ids['body_dl'], 
            with_session_ids['body_dr']))

with_session_ids = with_session_ids.withColumn(
    'traffic_source_medium',
    traffic_source_medium(
            with_session_ids['is_new_session'], 
            with_session_ids['body_dl'], 
            with_session_ids['body_dr']))

with_session_ids = with_session_ids.withColumn(
    'traffic_source_keyword',
    traffic_source_keyword(
            with_session_ids['is_new_session'], 
            with_session_ids['body_dl'], 
            with_session_ids['body_dr'],
            with_session_ids['traffic_source_medium']
            ))

with_session_ids = with_session_ids.withColumn(
    'traffic_source_ad_content',
    traffic_source_ad_content(
            with_session_ids['is_new_session'], 
            with_session_ids['body_dl'], 
            with_session_ids['body_dr']))

with_session_ids = with_session_ids.withColumn(
    'traffic_source_is_true_direct',
    traffic_source_is_true_direct(
            with_session_ids['traffic_source_source'])) 

udf_page_path = f.udf(parse_page_path, t.ArrayType(t.StringType()))

with_session_ids = with_session_ids\
                         .withColumn('page_path_level_one', udf_page_path(with_session_ids['body_dl'])[0])\
                         .withColumn('page_path_level_two', udf_page_path(with_session_ids['body_dl'])[1])\
                         .withColumn('page_path_level_three', udf_page_path(with_session_ids['body_dl'])[2])\
                         .withColumn('page_path_level_four', udf_page_path(with_session_ids['body_dl'])[3])\

## start with the extraction of landingpage
def extract_landing_page(is_new_session, body_dl):
    if is_new_session == 1:
        url = parse_url(body_dl)
        return url.path
    else:
        return None

## end with the extraction of landingpage

udf_extract_landing_page = f.udf(extract_landing_page)

with_session_ids = with_session_ids.withColumn(
        'landing_page',
        udf_extract_landing_page(
                with_session_ids['is_new_session'],
                with_session_ids['body_dl']
                ))

udf_extract_page_path = f.udf(lambda url: parse_url(url).path)

with_session_ids = with_session_ids.withColumn(
        'page_path',
        udf_extract_page_path(with_session_ids['body_dl'])) 

udf_extract_hostname = f.udf(lambda url: parse_url(url).netloc)

with_session_ids = with_session_ids.withColumn(
        'hostname',
        udf_extract_hostname(with_session_ids['body_dl']))


## start calculating hits_eCommerceAction_action_type

def action_type(pa: str) -> str:
    if pa == 'click':
        return 1
    elif pa == 'detail':
        return 2
    elif pa == 'add':
        return 3
    elif pa == 'checkout':
        return 5
    elif pa == 'purchase':
        return 6
    elif pa == 'refund':
        return 7
    elif pa == 'checkout_option':
        return 8
    else:
        return 0

udf_map_action_type = f.udf(action_type)

with_session_ids = with_session_ids.withColumn(
        'action_type',
        udf_map_action_type(with_session_ids['body_pa']))

## end calculating hits_eCommerceAction_action_type


## start unflattening the product data
col_names  = with_session_ids.columns
regex = re.compile('\d+')
tmp = [y for x in [re.findall(regex, c) for c in col_names] for y in x]
tmp_index = spark.createDataFrame(list(map(lambda x: Row(index=x), tmp))).distinct().sort(f.col('index')).collect()
index = [int(i.asDict()['index']) for i in tmp_index]

main = with_session_ids.select('*')

all_columns_df = with_session_ids.select('*')

for i in index:
   col_name = 'body_pr' + str(i) + 'ca'
   if not col_name in col_names:
       all_columns_df = all_columns_df.withColumn(col_name, f.lit(None))    
   col_name = 'body_pr' + str(i) + 'cc'
   if not col_name in col_names:
       all_columns_df = all_columns_df.withColumn(col_name, f.lit(None))    
   col_name = 'body_pr' + str(i) + 'id'
   if not col_name in col_names:
       all_columns_df = all_columns_df.withColumn(col_name, f.lit(None))    
   col_name = 'body_pr' + str(i) + 'nm'
   if not col_name in col_names:
       all_columns_df = all_columns_df.withColumn(col_name, f.lit(None))    
   col_name = 'body_pr' + str(i) + 'pr'
   if not col_name in col_names:
       all_columns_df = all_columns_df.withColumn(col_name, f.lit(None))    
   col_name = 'body_pr' + str(i) + 'qt'
   if not col_name in col_names:
       all_columns_df = all_columns_df.withColumn(col_name, f.lit(None))    
   col_name = 'body_pr' + str(i) + 'va'
   if not col_name in col_names:
       all_columns_df = all_columns_df.withColumn(col_name, f.lit(None))


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
   .join(bodies.alias('bodies'), f.col('main.message_id') == f.col('bodies.ms_id'), 'left_outer')

result = result.drop('ms_id')
## end unflattening the product data


## starting calculating productRevenue

def product_revenue(qt: int, pr: int, action_type: int) -> int:
    if action_type == '6':
        return float(qt) * float(pr)
    else:
        return None

udf_product_revenue = f.udf(product_revenue)

result = result.withColumn(
        'product_revenue',
        udf_product_revenue(
                result['prqt'],
                result['prpr'],
                result['action_type']
                ))
## ending calculating productRevenue
#revenue_per_session = result\
#        .select('visit_id', 'body_tr', 'action_type', 'body_t')\
#        .where('action_type == 6 and body_t ==  "event"')\
#        .distinct()\
#        .groupBy('visit_id')\
#        .agg({'body_tr': 'sum'})\
#        .withColumnRenamed('sum(body_tr)', 'revenue_per_session')
#
#revenue_per_session.select('revenue_per_session', 'visit_id').show(5, False)

result.createOrReplaceTempView('final')

rename_query = """
    select 
        body_cid as fullVisitorId, 
        visit_id as visitId,
        message_id as requestId,
        user_session_id as visitNumber,
        first_value as visitStartTime,
        date_format(ts, "yMMdd") as date,
        ifnull(body_dr, '') as trafficSource_referralPath,
        traffic_source_campaign as trafficSource_campaign,
        traffic_source_source as trafficSource_source,
        traffic_source_medium as trafficSource_medium,
        traffic_source_keyword as trafficSource_keyword,
        traffic_source_ad_content as trafficSource_ad_content,
        -- trafficSource_adwordsCkickInfo_campaignId
        -- trafficSource_adwordsClickInfo_adGroupId
        -- trafficSource_adwordsClickInfo_creativeId
        -- trafficSource_adwordsClickInfo_criteriaId
        -- trafficSource_adwordsClickInfo_page
        -- trafficSource_adwordsClickInfo_slot
        -- trafficSource_adwordsClickInfo_criteriaParameters
        -- trafficSource_adwordsClickInfo_gclid
        -- trafficSource_adwordsClickInfo_customerId
        -- trafficSource_adwordsClickInfo_adNetworkType
        -- trafficSource_adwordsClickInfo_targetingCriteria_boomUserlistId
        -- trafficSource_adwordsClickInfo_isVideoAd
        geo_continent as geoNetwork_continent,
        geo_sub_continent as geoNetwork_subContinent,
        geo_country as geoNetwork_country,
        geo_region as geoNetwork_region,
        geo_metro as geoNetwork_metro,
        geo_city as geoNetwork_city,
        geo_city_id as geoNetwork_cityId,
        geo_network_domain as geoNetwork_networkDomain,
        geo_latitude as geoNetwork_latitude,
        geo_longitude as geoNetwork_longitude,
        geo_network_location as geoNetwork_networkLocation,
        device_client_name as device_browser,
        device_client_version as device_browserVersion,
        body_vp as device_browserSize,
        device_os_name as device_operatingSystem,
        device_os_version as device_operatingSystemVersion,
        device_is_mobile as device_isMobile,
        device_device_brand as device_mobileDeviceBranding,
        device_device_model as device_mobileDeviceModel,
        device_device_input as device_mobileInputSelector,
        device_device_info as device_mobileDeviceInfo,
        device_device_name as device_mobileDeviceMarketingName,
        ifnull(body_fl, '') as device_flashVersion,
        body_je as device_javaEnabled,
        body_ul as device_language,
        body_sd as device_screenColors,
        body_sr as device_screenResolution,
        device_device_type as device_deviceCategory,
        landing_page as landingPage,
        body_ec as hits_eventInfo_eventCategory,
        body_ea as hits_eventInfo_eventAction,
        ifnull(body_el, '') as hits_eventInfo_eventLabel,
        ifnull(body_ev, '') as hits_eventInfo_eventValue,
        event_sequence as hits_hitNumber,
        ts as hits_time, -- needs to be calculated from the session start
        hour(ts) as hits_hour,
        minute(ts) as hits_minute,
        '' as hits_isSecure, -- depricated can be removed
        ifnull(body_ni, '') as hits_isInteractive,
        '' as hits_referer,
        page_path as hits_page_pagePath,
        hostname as hits_page_hostname,
        body_dt as hits_page_pageTitle,
        '' as hits_page_searchKeyword,
        '' as hits_page_searchCategory,
        page_path_level_one as hits_page_pagePathLevel1,
        page_path_level_two as hits_page_pagePathLevel2,
        page_path_level_three as hits_page_pagePathLevel3,
        page_path_level_four as hits_page_pagePathLevel4,
        ifnull(body_ti, '') as hits_item_transactionId,
        -- hits_item_productName,
        -- hits_item_productCategory,
        -- hits_item_productSku,
        -- hits_item_itemQuantity,
        -- hits_item_itemRevenue,
        ifnull(body_cu, '') as hits_item_currencyCode,
        '' as hits_item_localItemRevenue,
        ifnull(body_col, '') as hits_eCommerceAction_option,
        ifnull(body_cos, '') as hits_eCommerceAction_step,
        action_type as hits_eCommerceAction_action_type,
        ifnull(body_tcc, '') as hits_transation_transactionCoupon,
        ifnull(body_ti, '') as hits_transaction_transactionId,
        ifnull(body_tr, '') as hits_transaction_transactionRevenue,
        total_revenue_per_session as totals_transactionRevenue,
        ifnull(body_ts, '') as hits_transaction_transactionShipping,
        ifnull(body_tt, '') as hits_transaction_transactionTax,
        hits_type,
        prca as hits_product_v2ProductCategory,
        -- prcc -> Product Coupon Code, fields needs to reconsidered
        prid as hits_product_productSKU,
        prnm as hits_product_v2ProductName,
        prpr as hits_product_productPrice,
        prqt as hits_product_productQuantity,
        prva as hits_product_productVariant,
        product_revenue as hits_product_productRevenue,
        is_new_session
        from final
"""

renaming = spark.sql(rename_query)
renaming.createOrReplaceTempView('export')


export_sessions = spark.sql("""
        select 
            fullVisitorId, 
            visitId, 
            visitNumber, 
            visitStartTime, 
            date, 
            trafficSource_campaign,
            trafficSource_source,
            trafficSource_medium,
            trafficSource_keyword,
            trafficSource_ad_content,
            geoNetwork_continent,
            geoNetwork_subContinent,
            geoNetwork_country,
            geoNetwork_region,
            geoNetwork_metro,
            geoNetwork_city,
            geoNetwork_cityId,
            geoNetwork_networkDomain,
            geoNetwork_latitude,
            geoNetwork_longitude,
            geoNetwork_networkLocation,
            device_browser,
            device_browserVersion,
            device_browserSize,
            device_operatingSystem,
            device_operatingSystemVersion,
            device_isMobile,
            device_mobileDeviceBranding,
            device_mobileDeviceModel,
            device_mobileInputSelector,
            device_mobileDeviceInfo,
            device_mobileDeviceMarketingName,
            device_flashVersion,
            device_javaEnabled,
            device_language,
            device_screenColors,
            device_screenResolution,
            device_deviceCategory,
            totals_transactionRevenue,
            landingPage,
            hits_type
        from export
        where is_new_session='1'
        """) 

export_hits_pageviews = spark.sql("""
        select
            fullVisitorId,
            visitId,
            visitStartTime,
            hits_hitNumber,
            hits_time,
            hits_hour,
            hits_minute,
            hits_isSecure,
            hits_isInteractive,
            hits_referer,
            hits_page_pagePath,
            hits_page_hostname,
            hits_page_pageTitle,
            hits_page_pagePathLevel1,
            hits_page_pagePathLevel2,
            hits_page_pagePathLevel3,
            hits_page_pagePathLevel4,
            hits_eventInfo_eventCategory,
            hits_eventInfo_eventAction,
            hits_eventInfo_eventLabel,
            hits_eventInfo_eventValue,
            hits_type
        from export
        where hits_type='PAGE'
        """)

export_hits_events = spark.sql("""
        select
           fullVisitorId,
           visitId,
           visitStartTime,
           hits_hitNumber,
           hits_time,
           hits_hour,
           hits_minute,
           hits_isSecure,
           hits_isInteractive,
           hits_referer,
           hits_page_pagePath,
           hits_page_hostname,
           hits_page_pageTitle,
           hits_page_pagePathLevel1,
           hits_page_pagePathLevel2,
           hits_page_pagePathLevel3,
           hits_page_pagePathLevel4,
           hits_eventInfo_eventCategory,
           hits_eventInfo_eventAction,
           hits_eventInfo_eventLabel,
           hits_eventInfo_eventValue,
           hits_type
        from export
        where hits_type='EVENT'
        and hits_product_productSKU is null
        """)

export_products = spark.sql("""
        select
            fullVisitorId,
            visitid,
            visitStartTime,
            hits_hitNumber,
            hits_time,
            hits_hour,
            hits_minute,
            hits_product_productPrice,
            hits_product_productQuantity,
            '' as hits_product_productRefundAmount,
            hits_product_productSKU,
            hits_product_productVariant,
            hits_eCommerceAction_option,
            hits_eCommerceAction_step,
            hits_eCommerceAction_action_type,
            hits_item_transactionId,
            hits_product_productRevenue,
            hits_transaction_transactionRevenue,
            hits_type
        from export
        where hits_product_productSKU <> '' 
        and hits_type="EVENT"
        """) 

#export_sessions.select("*").show(10)
#export_hits_pageviews.select('*').show(10)
#export_hits_events.select('*').show(10)
#export_sessions.printSchema()
export_sessions.select('*')\
    .coalesce(1)\
    .write\
    .option('header', 'true')\
    .mode("Overwrite")\
    .csv('export/sessions')

export_hits_pageviews.select('*')\
    .coalesce(1)\
    .write\
    .option('header', 'true')\
    .mode("Overwrite")\
    .csv('export/pageviews')

export_hits_events.select('*')\
    .coalesce(1)\
    .write\
    .option('header', 'true')\
    .mode("Overwrite")\
    .csv('export/events')

export_products.select('*')\
    .coalesce(1)\
    .write\
    .option('header', 'true')\
    .mode("Overwrite")\
    .csv('export/products')


time_end = datetime.now().strftime("%Y-%m-%d %H:%M:%S") 
dt_end = datetime.strptime(time_end, tsfm) 
print("End time: ", dt_end)
print("Running time: ", (dt_end - dt_start).seconds, " seconds")


