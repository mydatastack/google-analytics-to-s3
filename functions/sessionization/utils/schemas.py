from pyspark.sql.types import DateType, StringType, StructType, StructField, DoubleType, IntegerType, TimestampType, BooleanType, LongType, ArrayType

session_schema = StructType([
        StructField("fullVisitorId", StringType(), True),
        StructField("visitId", StringType(), True),
        StructField("userId", StringType(), True),
        StructField("visitNumber", IntegerType(), True), 
        StructField("visitStartTime", LongType(), True), 
        StructField("date", IntegerType(), True),
        StructField("timestamp", TimestampType(), True),
        StructField("trafficSource_campaign", StringType(), True),
        StructField("trafficSource_source", StringType(), True), 
        StructField("trafficSource_medium", StringType(), True),
        StructField("trafficSource_keyword", StringType(), True),
        StructField("trafficSource_ad_content", StringType(), True),
        StructField("geoNetwork_continent", StringType(), True),
        StructField("geoNetwork_subContinent", StringType(), True),
        StructField("geoNetwork_country", StringType(), True),
        StructField("geoNetwork_region", StringType(), True),
        StructField("geoNetwork_metro", StringType(), True),
        StructField("geoNetwork_city", StringType(), True),
        StructField("geoNetwork_cityId", IntegerType(), True),
        StructField("geoNetwork_networkDomain", StringType(), True),
        StructField("geoNetwork_latitude", DoubleType(), True),
        StructField("geoNetwork_longitude", DoubleType(), True),
        StructField("geoNetwork_networkLocation", StringType(), True),
        StructField("device_browser", StringType(), True),
        StructField("device_browserVersion", DoubleType(), True),
        StructField("device_browserSize", StringType(), True),
        StructField("device_operatingSystem", StringType(), True),
        StructField("device_operatingSystemVersion", StringType(), True),
        StructField("device_isMobile", BooleanType(), True),
        StructField("device_mobileDeviceBranding", StringType(), True),
        StructField("device_mobileDeviceModel", StringType(), True), 
        StructField("device_mobileInputSelector", StringType(), True),
        StructField("device_mobileDeviceInfo", StringType(), True),
        StructField("device_mobileDeviceMarketingName", StringType(), True),
        StructField("device_flashVersion", IntegerType(), True),
        StructField("device_javaEnabled", StringType(), True),
        StructField("device_language", StringType(), True),
        StructField("device_screenColors", StringType(), True),
        StructField("device_screenResolution", StringType(), True),
        StructField("device_deviceCategory", StringType(), True),
        StructField("totals_transactionRevenue", StringType(), True),
        StructField("landingPage", StringType(), True),
        StructField("hits_type", StringType(), True),
        StructField("touchpoints", ArrayType(StringType()), True),
        StructField("touchpoints_wo_direct", ArrayType(StringType()), True),
        StructField("first_touchpoint", StringType(), True),
        StructField("last_touchpoint", StringType(), True)
        ])



ga_fields = { 
        "body_v", # protocol version
        "body_tid", # tracking id / web property id
        "body_aip", # anonymize ip
        "body_ds", # data source
        "body_cid", # client id
        "body_uid", # user id
        "body_dr", # document referrer
        "body_cn", # campaign name
        "body_cs", # campaing source
        "body_cm", # campaign medium
        "body_ck", # campaign keyword
        "body_cc", # campaign content
        "body_ci", # campaign id
        "body_gclid", # google ads id
        "body_dclid", # google display ads id
        "body_sr", # screen resolution
        "body_vp", # viewport size
        "body_de", # document ecncoding
        "body_sd", # screen colors 
        "body_ul", # user language
        "body_je", # java enabled
        "body_fl", # flash version 
        "body_t", # hit type
        "body_ni", # non-interaction hit
        "body_dl", # document location url
        "body_dh", # document host name
        "body_dp", # document path
        "body_dt", # document title
        "body_cd", # screen name
        "body_an", # application name
        "body_aid", # application id
        "body_av", # application version
        "body_aiid", # application installer id
        "body_ec", # event category
        "body_ea", # event action
        "body_el", # event label
        "body_ev", # event value
        "body_ti", # transaction id
        "body_ta", # transaction affiliation
        "body_tr", # transaction revenue
        "body_ts", # transaction shipping
        "body_tt", # transaction tax
        "body_in", # item name
        "body_ip", # item price
        "body_iq", # item quantity
        "body_ic", # item code
        "body_iv", # item category
        "body_tcc", # coupon code
        "body_pal", # product action list
        "body_cos", # checkout step
        "body_col", # checkout step option
        "body_cu", # currency code
        "body_pa", # product action
        } 

geo_fields = {
        'geo_metro', 
        'geo_city', 
        'geo_network_domain',
        'geo_continent_code', 
        'geo_longitude', 
        'geo_latitude', 
        'geo_country_iso', 
        'geo_postal_code', 
        'geo_sub_continent', 
        'geo_country', 
        'geo_network_location', 
        'geo_continent', 
        'geo_city_id', 
        'geo_timezone', 
        'geo_region' 
        }

device_fields = {
        'device_device_name', 
        'device_os_version', 
        'device_device_brand', 
        'device_os_name', 
        'device_is_bot', 
        'device_device_info', 
        'device_device_input', 
        'device_client_version', 
        'device_device_model', 
        'device_client_name', 
        'device_is_mobile', 
        'device_device_type' 
        }

api_gateway_fields = {
        'trace_id', 
        'system_source', 
        'message_id', 
        'ip', 
        'received_at_apig', 
        'user_agent', 
        'system_version', 
        }

def enhanced_ecom(param):
    return set([f"body_pr{x}{param}" for x in range(20)])

def custom_dim_metrics(params):
    return set([f"body_{params}{x}" for x in range(10)])


static_required_fields = set()\
            .union(ga_fields)\
            .union(geo_fields)\
            .union(device_fields)\
            .union(api_gateway_fields)\
            .union(enhanced_ecom("id"))\
            .union(enhanced_ecom("nm"))\
            .union(enhanced_ecom("br"))\
            .union(enhanced_ecom("ca"))\
            .union(enhanced_ecom("va"))\
            .union(enhanced_ecom("pr"))\
            .union(enhanced_ecom("qt"))\
            .union(enhanced_ecom("cc"))\
            .union(custom_dim_metrics("cd"))\
            .union(custom_dim_metrics("cm"))\

def field_types(field):
    if field == "device_is_bot":
        return StructField(field, BooleanType(), True)
    elif field == "device_is_mobile":
        return StructField(field, BooleanType(), True)
    elif field == "geo_latitude" or field == "geo_longitude":
        return StructField(field, DoubleType(), True)
    else:
        return StructField(field, StringType(), True)

def field_constructor(fields: set):
    return [field_types(field) for field in fields]

static_schema = StructType(field_constructor(static_required_fields)) 


enhanced_ecom_schema = StructType([
        StructField('ms_id', StringType()),
        StructField('prca', StringType()),
        StructField('prcc', StringType()),
        StructField('prid', StringType()),
        StructField('prnm', StringType()),
        StructField('prpr', StringType()),
        StructField('prqt', StringType()),
        StructField('prva', StringType())
        ])

