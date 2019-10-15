from pyspark.sql import SparkSession
from pyspark.context import SparkContext
from pyspark.sql.types import DateType, StringType, StructType, StructField, DoubleType, IntegerType, TimestampType, BooleanType, LongType, ArrayType
from pyspark.sql.functions import first, col, expr, when, reverse, spark_partition_id, sum, lit, monotonically_increasing_id, unix_timestamp, current_timestamp, to_timestamp, current_date, date_sub, desc, date_add, udf
from pyspark.sql.window import Window
from pyspark.sql import Row
from pyspark.sql.window import Window
from schemas import session_schema, static_schema, static_required_fields, enhanced_ecom_schema
from datetime import datetime, timedelta
from columns import columns_to_drop
import urllib.parse as urlparse
from functools import partial, reduce
import sys
import re
import os

# helper function for the pipelines
pipe = lambda fns: lambda x: reduce(lambda v, f: f(v), fns, x)

# inspecting partitions
def show_partition_id(df, col):
    return df.select(col, spark_partition_id().alias("partition_id")).show(1000, True)

# splitting the date into partitions 
def split_date(JOB_DATE):
    job_date = datetime.strptime(JOB_DATE, "%Y-%m-%d").date()
    return (job_date.strftime("%Y"), job_date.strftime("%m"), job_date.strftime("%d"))


def spark_context():
    spark = SparkSession\
        .builder\
        .appName("Python Spark SQL basic example")\
        .getOrCreate()

    spark.conf.set('spark.driver.memory', '6g')
    spark.conf.set('spark.sql.session.timeZone', 'Europe/Berlin')
    return spark

def read_data(spark, path):
    return spark.read.format("json")\
            .option("mode", "FAILFAST")\
            .option("inferSchema", "false")\
            .option("path", path)\
            .load()

def validate_fields(row, required_fields=static_required_fields):
    fields = row.asDict()
    available_fields = set(fields)
    na_fields = required_fields - available_fields 
    dct = dict.fromkeys(na_fields, None)
    merged = {**fields, **dct}
    return Row(**merged)

def load_session(spark, path, file_format, schema):
    try:
        return spark\
                .read\
                .format(file_format)\
                .option("path", path)\
                .option("inferSchema", "false")\
                .load()
    except Exception as e:
        print(e)
        return spark.createDataFrame([], schema)

def partial_pipe_page_path(main_fn, url):
    return pipe([
            parse_url,
            path_is_empty,
            extract_path_value,
            split_path,
            main_fn,
            ]) (url)

def partial_pipe_udf(main_fn, url):
    return pipe([
            parse_url,
            query_is_empty,
            extract_query_value,
            split_query,
            main_fn,
            ]) (url)

def parse_url(url):
    return urlparse.urlparse(url)

def path_is_empty(url):
    return url.path if len(str(url.path)) != 0 else None 

def extract_path_value(path):
    if path == None:
        return '' 
    else:
        return path 

def split_path(path):
    return path.split('/')[1:]

def construct_levels(p):
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
    return partial_pipe_page_path(construct_levels, url)

## start renaming the column body_t according to GA360
def hits_type(t):
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

def rename_hits_type(input_df, fn):
    fudf = udf(fn)
    return input_df\
                .withColumn(
                'hits_type',
                fudf(input_df['body_t']))



def add_user_session_id(spark, input_df):
    input_df.createOrReplaceTempView('clicks')
    return spark.sql(""" 
                select *,
                null as global_session_id, 
                -- sum(is_new_session) over (order by body_cid, received_at_apig) as global_session_id,
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

    """)


def calculate_visit_id(spark, input_df):
    input_df.createOrReplaceTempView('sessions')
    return spark.sql(""" 
        select *, 
        sha(concat(a.body_cid, a.first_value, a.last_value)) as visit_id,
        row_number() over (partition by body_cid order by received_at_apig asc) as event_sequence
            from
              (
              select *, 
              first_value(received_at_apig) over 
              (partition by body_cid, user_session_id order by is_new_session desc) as first_value,
              last_value(received_at_apig) over 
              (partition by body_cid, user_session_id) as last_value
                from sessions
              ) a
    """)



def get_total_revenue(input_df):
    w = Window.partitionBy(col("visit_id"))
    return input_df\
                .withColumn(
                    'total_revenue_per_session',
                    when(col("is_new_session") == '1', 
                        sum(when((col("body_t") == 'event') & (col("body_pa") == 'purchase'), 
                                col("body_tr")).otherwise(lit(''))
                        ).over(w)).otherwise(lit('')))



## start parsing the source

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

def parse_url(url):
    return urlparse.urlparse(url)

def query_is_empty(url):
    return url if len(str(url.query)) != 0 else []

def split_item(item):
    return item.split('=')

def split_query(qr):
    query = qr.split('&')
    query_clean = [x for x in query if x and x.find('=') > 0]
    return dict(split_item(item) for item in query_clean)

def identify_channel(channel_list, qr):
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
    return partial_pipe_udf(partial(identify_channel, channels), url)

def split_hostname(body_dr):
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
 
def parse_dr_source(body_dl, body_dr):
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

def identify_campaign(qr):
    return qr['utm_campaign'] if 'utm_campaign' in qr else '(not set)'

def parse_source_campaign(url):
    return partial_pipe_udf(identify_campaign, url)

def parse_dr_campaign(body_dl):
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

def identify_medium(qr):
    if 'utm_medium' in qr:
        return qr['utm_medium']
    if 'gclid' in qr:
        return 'paid'
    else:
        return '(none)'

def parse_source_medium(url):
    return partial_pipe_udf(identify_medium, url)

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

def parse_dr_medium(body_dr, body_dl):
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

def identify_keyword(qr):
    if 'utm_term' in qr:
        return qr['utm_term']
    else:
        return '(not set)'

def parse_source_keyword(url):
    return partial_pipe_udf(identify_keyword, url)

def parse_dr_keyword(body_dr, body_dl):
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

def identify_ad_content(qr):
    if 'utm_content' in qr:
        return qr['utm_content']
    else:
        return '(not set)'

def parse_source_ad_content(url):
    return partial_pipe_udf(identify_ad_content, url)

def extract_source_ad_content(is_new_session, body_dl, body_dr):
    if (is_new_session == 1 and body_dr is None):
        return parse_source_ad_content(body_dl) 
    else:
        return '(not set)'

## end parsing the adContent

def add_column(input_df, col_name, fn_udf, *args):
    return input_df.withColumn(col_name, fn_udf(*args))


## start with the extraction of landingpage
def extract_landing_page(is_new_session, body_dl):
    if is_new_session == 1:
        url = parse_url(body_dl)
        return url.path
    else:
        return None

## end with the extraction of landingpage



## start calculating hits_eCommerceAction_action_type

action_type_dict = {
        "click": 1,
        "detail": 2,
        "add": 3,
        "checkout": 5,
        "purchase": 6,
        "refund": 7,
        "checkout_option": 8
        }

def action_type(lookup, pa):
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


## end calculating hits_eCommerceAction_action_type


## start unflattening the product data
def flatten_pr_data(spark, with_session_ids, schema):
    col_names  = with_session_ids.columns
    regex = re.compile('\d+')
    tmp = [y for x in [re.findall(regex, c) for c in col_names] for y in x]
    tmp_index = spark.createDataFrame(list(map(lambda x: Row(index=x), tmp))).distinct().sort(col('index')).collect()
    index = [int(i.asDict()['index']) for i in tmp_index]

    main = with_session_ids.select('*')

    all_columns_df = with_session_ids.select('*')

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

    bodies = all_columns_df.rdd \
       .flatMap(lambda x: [Row(
               ms_id=x.message_id, prca=x['body_pr'+str(c)+'ca'], 
               prcc=x['body_pr'+str(c)+'cc'], prid=x['body_pr'+str(c)+'id'], 
               prnm=x['body_pr'+str(c)+'nm'], prpr=x['body_pr'+str(c)+'pr'], 
               prqt=x['body_pr'+str(c)+'qt'], prva=x['body_pr'+str(c)+'va']) 
               for c in index]).filter(lambda x:x.ms_id != None and (x.prca != None or x.prcc != None or x.prid != None or x.prnm != None or x.prpr != None or x.prqt != None or x.prva != None)).toDF(schema)

    result = main.alias('main') \
       .join(bodies.alias('bodies'), col('main.message_id') == col('bodies.ms_id'), 'left_outer')

    result = result.drop('ms_id')
    return result

## end unflattening the product data


## starting calculating productRevenue

def product_revenue(qt, pr, action_type):
    if action_type == '6':
        return float(qt) * float(pr)
    else:
        return None

def create_export_table(spark, input_df):
    input_df.createOrReplaceTempView('final')
    return spark.sql("""
        select 
            body_cid as fullVisitorId, 
            visit_id as visitId,
            ifnull(body_uid, '') as userId,
            message_id as requestId,
            ts as timestamp,
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
            ifnull(body_je, '') as device_javaEnabled,
            ifnull(body_ul, '') as device_language,
            ifnull(body_sd, '') as device_screenColors,
            ifnull(body_sr, '') as device_screenResolution,
            device_device_type as device_deviceCategory,
            landing_page as landingPage,
            ifnull(body_ec, '') as hits_eventInfo_eventCategory,
            ifnull(body_ea, '') as hits_eventInfo_eventAction,
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
            ifnull(body_dt, '') as hits_page_pageTitle,
            '' as hits_page_searchKeyword,
            '' as hits_page_searchCategory,
            page_path_level_one as hits_page_pagePathLevel1,
            page_path_level_two as hits_page_pagePathLevel2,
            page_path_level_three as hits_page_pagePathLevel3,
            page_path_level_four as hits_page_pagePathLevel4,
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
            ifnull(body_cu, '') as hits_transaction_currencyCode,
            ifnull(body_ti, '') as hits_item_transactionId,
            ifnull(body_in, '') as hits_item_productName, 
            ifnull(body_ip, '') as hits_item_itemRevenue,
            ifnull(body_iq, '') as hits_item_itemQuantity,
            ifnull(body_ic, '') as hits_item_productSku,
            ifnull(body_iv, '') as hits_item_productCategory,
            ifnull(body_cu, '') as hits_item_currencyCode,
            hits_type,
            prca as hits_product_v2ProductCategory,
            prid as hits_product_productSKU,
            prnm as hits_product_v2ProductName,
            prpr as hits_product_productPrice,
            prqt as hits_product_productQuantity,
            prva as hits_product_productVariant,
            product_revenue as hits_product_productRevenue,
            is_new_session
            from final
    """)


def create_export_sessions_table(spark, input_df):
    input_df.createOrReplaceTempView("export")
    return spark.sql("""
            select 
                fullVisitorId, 
                visitId, 
                userId,
                visitNumber, 
                visitStartTime, 
                date, 
                timestamp,
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


def new_sessions(input_df, date):
    return input_df\
                .withColumn("touchpoints", lit(None))\
                .withColumn("touchpoints_wo_direct", lit(None))\
                .withColumn("first_touchpoint", lit(None))\
                .withColumn("last_touchpoint", lit(None))\
                .select("*")\
                .where(input_df.timestamp.contains(date))


def drop_columns(input_df, *args): 
    return input_df.drop(*args)


def calculate_touchpoints(input_df):
    w1 = Window\
            .partitionBy("fullVisitorId")\
            .orderBy("timestamp")

    first_touchpoint = first(col("trafficSource_source")).over(w1)

    return input_df\
        .orderBy("timestamp")\
        .selectExpr("*",
            "collect_list(trafficSource_source) over (partition by fullVisitorId) as touchpoints")\
          .withColumn("touchpoints_wo_direct", expr("filter(touchpoints, x -> x != '(direct)')"))\
          .orderBy("timestamp")\
          .select("*",
                  first_touchpoint.alias("first_touchpoint"), 
                  when(reverse(col("touchpoints_wo_direct"))[0].isNotNull(), reverse(col("touchpoints_wo_direct"))[0]).otherwise("(direct)").alias("last_touchpoint"))

def export_hits_pageviews_table(spark):
    return spark.sql("""
            select
                fullVisitorId,
                visitId,
                requestId,
                visitStartTime,
                timestamp,
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


def export_hits_events_table(spark):
    return spark.sql("""
            select
               fullVisitorId,
               visitId,
               requestId,
               visitStartTime,
               timestamp,
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


def export_hits_products_table(spark):
    return spark.sql("""
            select
                fullVisitorId,
                visitId,
                requestId,
                visitStartTime,
                timestamp,
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



def export_hits_transactions_table(spark):
    return spark.sql("""
            select
                fullVisitorId,
                visitId,
                requestId,
                visitStartTime,
                timestamp,
                hits_hitNumber,
                hits_time,
                hits_hour,
                hits_transation_transactionCoupon,
                hits_transaction_transactionId,
                hits_transaction_transactionRevenue,
                totals_transactionRevenue,
                hits_transaction_transactionShipping,
                hits_transaction_transactionTax
            from export
            where hits_type="TRANSACTION"
            """)


def export_hits_items_table(spark):
    return spark.sql("""
            select
                fullVisitorId,
                visitId,
                requestId,
                visitStartTime,
                timestamp,
                hits_hitNumber,
                hits_time,
                hits_hour,
                hits_item_transactionId,
                hits_item_productName, 
                hits_item_itemRevenue,
                hits_item_itemQuantity,
                hits_item_productSKU,
                hits_item_productCategory
            from export
            where hits_type="ITEM"
            """)



def pipeline(spark, df, session_history, job_date):
    job_year, job_month, job_day = split_date(job_date)
    df = df.rdd.map(lambda row: validate_fields(row)).toDF(static_schema)
    df = rename_hits_type(df, hits_type)
    sessions = add_user_session_id(spark, df)
    sessions = sessions.filter(~sessions['body_t'].isin(['adtiming', 'timing']))
    with_session_ids = calculate_visit_id(spark, sessions)
    with_session_ids = get_total_revenue(with_session_ids)

    with_session_ids = add_column(
            with_session_ids, 
            "traffic_source_source", 
            udf(extract_source_source),
            with_session_ids['is_new_session'], 
            with_session_ids['body_dl'], 
            with_session_ids['body_dr'])

    with_session_ids = add_column(
            with_session_ids, 
            "traffic_source_campaign",
            udf(extract_source_campaign),
            with_session_ids['is_new_session'], 
            with_session_ids['body_dl'], 
            with_session_ids['body_dr'])

    with_session_ids = add_column(
            with_session_ids,
            "traffic_source_medium",
            udf(extract_source_medium),
            with_session_ids['is_new_session'], 
            with_session_ids['body_dl'], 
            with_session_ids['body_dr'])

    with_session_ids = add_column(
            with_session_ids,
            "traffic_source_keyword",
            udf(extract_source_keyword),
            with_session_ids['is_new_session'], 
            with_session_ids['body_dl'], 
            with_session_ids['body_dr'],
            with_session_ids['traffic_source_medium'])

    with_session_ids = add_column(
            with_session_ids,
            "traffic_source_ad_content",
            udf(extract_source_ad_content),
            with_session_ids['is_new_session'], 
            with_session_ids['body_dl'], 
            with_session_ids['body_dr'])

    with_session_ids = add_column(
            with_session_ids,
            "traffic_source_is_true_direct",
            udf(lambda x: 'True' if x == '(direct)' else None),
            with_session_ids['traffic_source_source']) 

    udf_page_path = udf(parse_page_path, ArrayType(StringType()))

    with_session_ids = with_session_ids\
                           .withColumn(
                             'page_path_level_one', udf_page_path(with_session_ids['body_dl'])[0])\
                           .withColumn(
                             'page_path_level_two', udf_page_path(with_session_ids['body_dl'])[1])\
                           .withColumn(
                             'page_path_level_three', udf_page_path(with_session_ids['body_dl'])[2])\
                           .withColumn(
                              'page_path_level_four', udf_page_path(with_session_ids['body_dl'])[3])\

    with_session_ids = add_column(
            with_session_ids,
            "landing_page",
            udf(extract_landing_page),
            with_session_ids['is_new_session'],
            with_session_ids['body_dl'])

    with_session_ids = add_column(
            with_session_ids,
            "page_path",
            udf(lambda url: parse_url(url).path),
            with_session_ids['body_dl']) 

    udf_extract_hostname = udf(lambda url: parse_url(url).netloc)

    with_session_ids = add_column(
            with_session_ids,
            "hostname",
            udf(lambda url: parse_url(url).netloc),
            with_session_ids['body_dl'])

    udf_map_action_type = udf(partial(action_type, action_type_dict))

    with_session_ids = with_session_ids.withColumn(
            'action_type',
            udf_map_action_type(with_session_ids['body_pa']))
    result = flatten_pr_data(spark, with_session_ids, enhanced_ecom_schema)

    udf_product_revenue = udf(product_revenue)

    result = result.withColumn(
            'product_revenue',
            udf_product_revenue(
                    result['prqt'],
                    result['prpr'],
                    result['action_type']
                    ))

    export_table = create_export_table(spark, result)
        
    export_sessions = create_export_sessions_table(spark, export_table)

    unioned = session_history.union(new_sessions(export_sessions, job_date))

    unioned_dropped = drop_columns(unioned,
                "touchpoints", 
                "touchpoints_wo_direct", 
                "first_touchpoint", 
                "last_touchpoint")

    export_multichannel_sessions = calculate_touchpoints(unioned_dropped)

    export_hits_pageviews = export_hits_pageviews_table(spark)

    export_hits_events = export_hits_events_table(spark)
    
    export_hits_products = export_hits_products_table(spark)

    export_hits_transactions =  export_hits_transactions_table(spark)

    export_hits_items = export_hits_items_table(spark) 

    return (export_multichannel_sessions,
            export_hits_pageviews,
            export_hits_events,
            export_hits_products,
            export_hits_transactions,
            export_hits_items)

def glue(sc):
    from awsglue.context import GlueContext
    from awsglue.utils import getResolvedOptions
    args = getResolvedOptions(sys.argv, [
            "s3bucket", 
            "year_partition",
            "month_partition",
            "day_partition"
            ])
    s3_bucket = args["s3bucket"]
    year_partition = args["year_partition"]
    month_partition = args["month_partition"]
    day_partition = args["day_partition"]
    return (s3_bucket, year_partition, month_partition, day_partition, GlueContext(sc))

def load_data(spark, path):
    return spark.read.format("json")\
            .option("mode", "FAILFAST")\
            .option("inferSchema", "false")\
            .option("path", path)\
            .load()

def filter_df(df, job_date):
    return df\
            .select("*")\
            .where(df.timestamp.contains(job_date))\
            .repartition(1)

def save_sessions_df(df, path):
    df\
     .write\
     .mode("append")\
     .format("parquet")\
     .save(path)

def save_daily_dfs(df, path):
    df\
     .write\
     .mode("overwrite")\
     .format("parquet")\
     .save(path)


def main():
    ENVIRONMENT = os.getenv("ENVIRONMENT")
    try:
        if ENVIRONMENT == "development":
            sc = SparkContext.getOrCreate() 
            s3_bucket, year_partition, month_partition, day_partition, glue_context = glue(sc)
            spark = glue_context.spark_session
            main_path = f"s3n://{s3_bucket}/aggregated/ga/daily"
            load_path = f"s3n://{s3_bucket}/enriched/ga/year={year_partition}/month={month_partition}/day={day_partition}/*" 
            session_path = f"s3n://{s3_bucket}/aggregated/ga/history/sessions/"
            partition_path = f"year={year_partition}/month={month_partition}/day={day_partition}"
            df = load_data(spark, load_path)
            sessions_df = load_session(spark, session_path, "parquet", session_schema)
            job_date = f"{year_partition}-{month_partition}-{day_partition}"
            save_path = "/aggregated/ga/daily"
            export_multichannel_sessions,export_hits_pageviews, export_hits_events, export_hits_products, export_hits_transactions, export_hits_items = pipeline(spark, df, sessions_df, job_date)
            save_sessions_df(filter_df(export_multichannel_sessions, job_date), session_path)
            save_daily_dfs(filter_df(export_multichannel_sessions, job_date), f"{main_path}/type=sessions/{partition_path}/")
            save_daily_dfs(filter_df(export_hits_pageviews, job_date), f"{main_path}/type=pageviews/{partition_path}/")
            save_daily_dfs(filter_df(export_hits_events, job_date), f"{main_path}/type=events/{partition_path}/")
            save_daily_dfs(filter_df(export_hits_products, job_date), f"{main_path}/type=products/{partition_path}/")
            save_daily_dfs(filter_df(export_hits_transactions, job_date), f"{main_path}/type=transactions/{partition_path}/")
            save_daily_dfs(filter_df(export_hits_items, job_date), f"{main_path}/type=items/{partition_path}/")
            return "success"

        elif ENVIRONMENT == "local":
            year_partition = os.getenv("year_partition") 
            month_partition = os.getenv("month_partition") 
            day_partition = os.getenv("day_partition") 
            job_date = f"{year_partition}-{month_partition}-{day_partition}"
            spark = spark_context()
            main_path = "./aggregated/ga/daily"
            session_path = "./aggregated/ga/history/sessions"
            load_path = "./samples/ecommerce-basic/*"
            partition_path = f"year={year_partition}/month={month_partition}/day={day_partition}"
            df = load_data(spark, load_path)
            sessions_df = load_session(spark, session_path, "parquet", session_schema)
            export_multichannel_sessions, export_hits_pageviews, export_hits_events, export_hits_products, export_hits_transactions, export_hits_items = pipeline(spark, df, sessions_df, job_date)
            save_sessions_df(filter_df(export_multichannel_sessions, job_date), "./aggregated/ga/history/sessions")
            save_daily_dfs(filter_df(export_multichannel_sessions, job_date), f"{main_path}/type=sessions/{partition_path}/")
            save_daily_dfs(filter_df(export_hits_pageviews, job_date), f"{main_path}/type=pageviews/{partition_path}/")
            save_daily_dfs(filter_df(export_hits_events, job_date), f"{main_path}/type=events/{partition_path}/")
            save_daily_dfs(filter_df(export_hits_products, job_date), f"{main_path}/type=products/{partition_path}/")
            save_daily_dfs(filter_df(export_hits_transactions, job_date), f"{main_path}/type=transactions/{partition_path}/")
            save_daily_dfs(filter_df(export_hits_items, job_date), f"{main_path}/type=items/{partition_path}/")
            return "success"
        else:
            raise Exception("Need to provide environment variables to run the job")

    except Exception as e:
        print("Something went wrong", e)
        return "error"
     
if __name__ == "__main__":
    main()


