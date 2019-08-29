from pyspark.sql import SparkSession
from pyspark.sql.types import DateType
from pyspark.sql import functions as f
from pyspark.sql import types as t
from pyspark.sql import Row
from columns import columns_to_drop
import sys
from pyspark.sql.window import Window

spark = SparkSession \
    .builder \
    .appName("Python Spark SQL basic example") \
    .config("spark.some.config.option", "some-value") \
    .getOrCreate()
spark.sparkContext.setLogLevel('ERROR')
spark.conf.set('spark.sql.session.timeZone', 'Europe/Berlin')

save_location = './'

df = spark.read.json('output.jsonl')
df.createOrReplaceTempView('clicks')

df_clean = df.drop(*columns_to_drop)
df_clean.createOrReplaceTempView('clicks')


query = """ 
                select *,
                sum(is_new_session) over (order by body_cid, received_at_apig) as global_session_id,
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

with_session_ids = spark.sql(session_id_query)

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
        return None 

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


#with_session_ids.select('body_cid','is_new_session', 'user_session_id', 'ts', 'global_session_id', 'visit_id', 'body_t', 'traffic_source_source', 'traffic_source_medium', 'landing_page').filter(with_session_ids['user_session_id'] > 0).filter(with_session_ids['body_cid'] == '1001319705.1560936493').show(150, truncate=False)
with_session_ids.createOrReplaceTempView('final')
#spark.sql('select * from final').show(5)
#spark.sql('select * from final limit 1000').coalesce(1).write.option('header', 'true').csv('export')
#with_session_ids.printSchema()
#spark.sql('select body_cid, body_pa as product_action, body_tr as revenue, body_pr1ca, body_pr1id, body_pr1nm, body_pr1pr, body_pr1qt from final where body_pa="purchase"').show()

cities = spark.sql('select geo_city, geo_country, count(distinct body_cid) as visitors from final where ts between "2019-08-09" and "2019-08-10" group by geo_city, geo_country order by visitors desc')
#cities.show(50)


visitors_total = spark.sql('select count(body_cid) as total_visits from final where ts between "2019-08-09" and "2019-08-10" and is_new_session="1"')
#visitors_total.show()

visitors_by_medium = spark.sql('select traffic_source_medium, count(body_cid) as visits from final where ts between "2019-08-09" and "2019-08-10" and is_new_session="1" group by traffic_source_medium order by visits desc')
#visitors_by_medium.show(50)

rename_query = """
    select 
        body_cid as fullVisitorId, 
        visit_id as visitId,
        user_session_id as visitNumber,
        first_value as visitStartTime,
        ts as date,
        body_dr as trafficSource_referralPath,
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
        ua_detected_client_name as device_browser,
        ua_detected_client_version as device_browserVersion,
        body_vp as device_browserSize,
        ua_detected_os_name as device_operatingSystem,
        ua_detected_os_version as device_operatingSystemVersion,
        ua_detected_is_mobile as device_isMobile,
        ua_detected_device_brand as device_mobileDeviceBranding,
        ua_detected_device_model as device_mobileDeviceModel,
        ua_detected_device_input as device_mobileInputSelector,
        ua_detected_device_info as device_mobileDeviceInfo,
        ua_detected_device_name as device_mobileDeviceMarketingName,
        body_fl as device_flashVersion,
        body_je as device_javaEnabled,
        body_ul as device_language,
        body_sd as device_screenColors,
        body_sr as device_screenResolution,
        ua_detected_device_type as device_deviceCategory,
        landing_page as landingPage
        from final
"""

renaming = spark.sql(rename_query)
renaming.createOrReplaceTempView('export')

save = spark.sql('select * from export limit 1000')
#export = save.coalesce(1).write.option('header', 'true').csv('export')

# calculates total revenue for the date
# 22.664.41
#spark.sql('select round(sum(body_tr)) as revenue from final where ts between "2019-08-09" and "2019-08-10" and body_pa="purchase" and body_t="event"').show()
#spark.sql('select visit_id, traffic_source_source, traffic_source_medium, sum(body_tr) as revenue from final where ts between "2019-08-09" and "2019-08-10" and body_pa="purchase" and body_t="event" group by visit_id, traffic_source_source, traffic_source_medium').show()

#visit_id_revenue = spark.sql('select visit_id, body_tr as revenue from final where ts between "2019-08-09" and "2019-08-10" and body_pa="purchase" and body_t="event"')
#visit_id_source = spark.sql('select *, traffic_source_source, traffic_source_medium, visit_id from final where ts between "2019-08-09" and "2019-08-10" and is_new_session="1"')
#join_revenue_source = visit_id_revenue.join(visit_id_source, visit_id_revenue['visit_id'] == visit_id_source['visit_id'])
#join_revenue_source.createOrReplaceTempView('revenue_table')

#spark.sql('select sum(revenue) as total_revenue from revenue_table').show()
#spark.sql('select traffic_source_medium, round(sum(revenue)) as total_revenue from revenue_table group by traffic_source_medium order by total_revenue desc').show()

#spark.sql('select count(distinct body_cid) from final where ts between "2019-08-09" and "2019-08-10"').show()
#spark.sql('select traffic_source_source, traffic_source_medium, count(distinct body_cid) as visitors from final where ts between "2019-08-09" and "2019-08-10" and is_new_session="1" group by traffic_source_source, traffic_source_medium order by visitors desc').show()
#number_of_visitors = spark.sql('select distinct body_cid from final where is_new_session="1" and ts between "2019-08-09" and "2019-08-10"').count()
#print('number of visitors for 09.08.2019: ', number_of_visitors)

#spark.sql('select count(distinct body_cid) as nutzer from final where ts between "2019-08-09" and "2019-08-10" and is_new_session="1"').show()
#spark.sql('select count(distinct body_cid) as bots from final where ts between "2019-08-09" and "2019-08-10" and ua_detected_is_bot="false"').show()

#spark.sql('select traffic_source_medium, count(distinct body_cid) as visitors from final where ts between "2019-08-09" and "2019-08-10" and is_new_session="1" group by traffic_source_medium order by visitors desc ').show()

#number_of_visitors = spark.sql('select traffic_source_medium, count(distinct body_cid) as visitors from final where ts between "2019-08-09" and "2019-08-10" and is_new_session="1" group by traffic_source_medium order by visitors desc')
#number_of_visitors.select('traffic_source_medium', 'visitors').agg({'visitors': 'sum'}).show(50)
#number_of_visitors.select('*').show(50)
#spark.sql('select geo_city, count(distinct body_cid) as visitors from final where ts between "2019-08-09" and "2019-08-10" group by geo_city order by visitors desc').show(500, truncate=False) 
#spark.sql('select geo_city, ip, ua_detected_device_type from final where ts between "2019-08-09" and "2019-08-10" and geo_city="NaN"').show(100)
