from pyspark.sql import SparkSession
from pyspark.sql.types import DateType
from pyspark.sql import functions as f
from pyspark.sql import types as t
from pyspark.sql import Row
import sys
from pyspark.sql.window import Window

spark = SparkSession \
    .builder \
    .appName("Python Spark SQL basic example") \
    .config("spark.some.config.option", "some-value") \
    .getOrCreate()
spark.sparkContext.setLogLevel('ERROR')

save_location = './'

df = spark.read.json('output.jsonl')
df.createOrReplaceTempView('clicks')

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

    """

sessions = spark.sql(query)


# filter out adtiming and timing events
sessions_clean = sessions.filter(~sessions['body_t'].isin(['adtiming', 'timing']))
sessions_clean.createOrReplaceTempView('sessions')

referrer_data = spark.sql('select body_t, body_cid, is_new_session, body_dr, body_dl from ' +
          'sessions where is_new_session=1')

session_ids = spark.sql('select body_cid, user_session_id, is_new_session, dense_rank() over (partition by body_cid, user_session_id order by is_new_session desc) as dense_rank from sessions where body_cid="192399564.1565269823"')

import sys

session_ids_new = sessions_clean.withColumn("session_id", f.sha2(f.concat_ws('||', sessions_clean['body_cid'], sessions_clean['user_session_id']), 0))

spark.sql('select body_cid, is_new_session, user_session_id, ' +
          'sha(concat(body_cid, user_session_id)) as session_id ' +
          'from sessions')

session_id_query = """
          select *, first_value(received_at_apig) over (partition by body_cid order by is_new_session desc) as first_value,
          last_value(received_at_apig) over (partition by body_cid) as last_value
          from sessions
"""

session_id_query_ = """ select *, sha(concat(a.body_cid, a.first_value, a.last_value)) as session_id,
                        row_number() over (partition by body_cid order by received_at_apig asc) as event_sequence
from
          (
          select *, first_value(received_at_apig) over (partition by body_cid order by is_new_session desc) as first_value,
          last_value(received_at_apig) over (partition by body_cid) as last_value
          from sessions
          ) a
"""

with_session_ids = spark.sql(session_id_query_)


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

def split_query(qr: str):
    return dict(item.split('=') for item in qr.split('&'))

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
        return qr[channel[0]] 
    else:
        return '(not set)' 

def parse_source_source(url):
    return pipe([
            parse_url,
            query_is_empty,
            extract_query_value,
            split_query,
            partial(identify_channel, channels),
            ]) (url)
 
def extract_source_source(is_new_session, body_dl, body_dr):
    if (is_new_session == 1 and body_dr is None):
        return parse_source_source(body_dl) 
    elif (is_new_session == 1 and body_dr is not None):
        hostname = body_dr.split('//')[-1].split('/')[0].split('.')[1]
        if hostname == 'googleadservices':
            return 'google'
        else:
            return hostname
    else:
        return None
## end parsing the source

## start parsing the medium

## end parsing the medium

traffic_source_referral_path = f.udf(lambda x: x)
traffic_source_campaign = f.udf(lambda x: x)
traffic_source_source = f.udf(extract_source_source)
traffic_source_medium = f.udf(lambda x: x)
traffic_source_keyword = f.udf(lambda x: x)
traffic_source_adContent = f.udf(lambda x: x)
traffic_source_is_true_direct = f.udf(lambda x: x)

with_session_ids = with_session_ids.withColumn(
    'traffic_source_source',
    traffic_source_source(
            with_session_ids['is_new_session'], 
            with_session_ids['body_dl'], 
            with_session_ids['body_dr']))

with_session_ids = with_session_ids.withColumn(
    'traffic_source_medium',
    traffic_source_medium(
            with_session_ids['is_new_session'], 
            with_session_ids['body_dl'], 
            with_session_ids['body_dr']))

with_session_ids.select('traffic_source_source', 'body_dl').filter(with_session_ids['is_new_session'] > 0).show(1000, truncate=False)

