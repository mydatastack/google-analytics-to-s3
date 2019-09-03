from pyspark.sql import SparkSession
import itertools
from functools import reduce
import operator

spark = SparkSession\
    .builder\
    .appName("Python Spark SQL basic example")\
    .config("spark.some.config.option", "some-value")\
    .getOrCreate()
spark.sparkContext.setLogLevel('ERROR')
spark.conf.set('spark.sql.session.timeZone', 'Europe/Berlin')

save_location = './'

from pyspark.sql.types import StructType, StructField, StringType

required_columns = [
        'body_v',
        'body_tid',
        'body_aip',
        'body_ds',
        'body_qt',
        'body_z',
        'body_cid',
        'body_uid',
        'body_sc',
        'body_uip',
        'body_ua'
        'body_goid',
        'body_dr',
        'body_cn',
        'body_cs',
        'body_cm',
        'body_ck',
        'body_cc',
        'body_ci',
        'body_gclid',
        'body_dclid',
        'body_sr',
        'body_vp',
        'body_de',
        'body_sd',
        'body_ul',
        'body_je',
        'body_fl',
        'body_t',
        'body_ni',
        'body_dl',
        'body_dh',
        'body_dp',
        'body_dt',
        'body_cd',
        'body_linkid',
        'body_an',
        'body_aid',
        'body_av',
        'body_aiid',
        'body_ec',
        'body_ea',
        'body_el',
        'body_ev',
        'body_in',
        'body_ip',
        'body_iq',
        'body_ic',
        'body_iv',
        'body_pa',
        'body_ti',
        'body_ta',
        'body_tr',
        'body_tt',
        'body_ts',
        'body_tcc',
        'body_pal',
        'body_cos',
        'body_col',
        'body_promoa',
        'body_cu',
        'body_sn',
        'body_sa',
        'body_st',
        'body_utc',
        'body_utv',
        'body_utt',
        'body_utl',
        'body_plt',
        'body_dns',
        'body_pdt',
        'body_rrt',
        'body_tcp',
        'body_srt',
        'body_dit',
        'body_clt',
        'body_exd',
        'body_exf',
        'body_xid',
        'body_xvar'
        ]

def generate_schema(xs: list) -> list:
    pass

def flatten_columns(opt: tuple) -> str:
    if len(opt) == 2:
        (param1, index1) = opt
        return str(param1) + str(index1) 
    elif len(opt) == 3:
        (param1, index1, param2) = opt
        result = [param1 + str(x) + param2 for x in range(1,index1 + 1)]
        return result[0] 
    elif len(opt) == 4:
        (param1, index1, param2, index2) = opt
        return str(param1) + str(index1) + str(param2) + str(index2)
    elif len(opt) == 5:
        (param1, index1, param2, index2, param3) = opt
        return str(param1) + str(index1) + str(param2) + str(index2) + str(param3) 
    elif len(opt) == 6:
        (param1, index1, param2, index2, param3, index3) = opt
        return str(param1) + str(index1) + str(param2) + str(index2) + str(param3) + str(index3)


indexed_columns = [
       # ('cg', 5),
        ('pr', 200, 'id'),
        ('pr', 200, 'nm'),
        ('pr', 200, 'br'),
        ('pr', 200, 'ca'),
        ('pr', 200, 'va'),
        ('pr', 200, 'pr'),
        ('pr', 200, 'qt'),
        ('pr', 200, 'cc'),
        ('pr', 200, 'ps'),
       # ('pr', 200, 'cd', 200),
       # ('pr', 200, 'cm', 200),
       # ('il', 200, 'id'),
       # ('il', 200, 'nm'),
       # ('il', 200, 'br', 200),
       # ('il', 200, 'pi', 200, 'ca'),
       # ('il', 200, 'pi', 200, 'va'),
       # ('il', 200, 'pi', 200, 'ps'),
       # ('il', 200, 'pi', 200, 'pr'),
       # ('il', 200, 'pi', 200, 'cd', 200),
       # ('il', 200, 'pi', 200, 'cm', 200),
       # ('promo', 200, 'id'),
       # ('promo', 200, 'nm'),
       # ('promo', 200, 'cr'),
       # ('promo', 200, 'ps'),
       # ('cd', 200),
       # ('cm', 200)
        ]

tst = [flatten_columns(x) for x in indexed_columns]
print(tst)

fields = [StructField(c, StringType(), True) for c in required_columns]
df = spark.read.json('./output.jsonl', StructType(fields)).cache()
#df.printSchema()
df.filter(df['body_t'] == 'event')
