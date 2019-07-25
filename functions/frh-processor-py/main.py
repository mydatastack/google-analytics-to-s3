from functools import partial, reduce
import json
from urllib.parse import urlparse, parse_qsl 
from device_detector import SoftwareDetector
from base64 import b64decode, b64encode
import maxminddb

pipe = lambda fns: lambda x: reduce(lambda v, fn: fn(v), fns, x) 

def get_prop(prop: str, record: dict) -> list:
    return record[prop]

def decode_records(record: dict) -> dict:
    try:
        recordId = record['recordId']
        data = record['data']
        b64_encoded = b64decode(data)
        deserialized = json.loads(b64_encoded)
        return (recordId, record, deserialized)
    except:
        return ()

def decode_data(xs: list) -> list:
    return [
            decode_records(x)
            for x in xs
            ] 

parse_body_query = lambda data: dict(parse_qsl(data['body']))

def parse_ga_body_payload(xs: list) -> list:
    return [
            (recordId, record, data, parse_body_query(data), data['user_agent'], data['ip']) 
            for recordId, record, data in xs
            ]

def detect(user_agent: str) -> dict:
    device = SoftwareDetector(user_agent).parse()
    is_bot = device.is_bot()
    if is_bot:
        return {'is_bot': True}
    else:
        client_name = device.client_name()
        client_type = device.client_type()
        client_version = device.client_version()
        os_name = device.os_name()
        os_version = device.os_version()
        device_type = device.device_type()
        return {
                'is_bot': False, 
                'client_name': client_name, 
                'client_type': client_type,
                'client_version': client_version,
                'os_name': os_name,
                'os_version': os_version,
                'device_type': device_type,
                } 

def parse_user_agent(xs: list) -> list:
    return [
            (recordId, record, data, ga_body, detect(user_agent), ip)
            for recordId, record, data, ga_body, user_agent, ip in xs
            ]

def extract_ip_data(reader, user_agent: dict, ip: str) -> dict:
    if user_agent['is_bot']:
        return {'ip': ip}
    else:
        location = reader.get(ip)
        city = location['city']['names']['en'] 
        country = location['country']['names']['en'] 
        country_iso = location['country']['iso_code']
        continent_code = location['continent']['code']
        continent_name = location['continent']['names']['en']
        longitude = location['location']['longitude']
        latitude = location['location']['latitude']
        timezone = location['location']['time_zone']
        postal_code = location['postal']['code']
        return {
                'city': city,
                'postal_code': postal_code,
                'country': country,
                'country_iso': country_iso,
                'continent_code': continent_code,
                'continent_name': continent_name,
                'longitude': longitude,
                'latitude': latitude,
                'timezone': timezone
                } 

def ip_lookup(xs: list) -> list:
    try:
        reader = maxminddb.open_database('./mmdb/GeoLite2-City.mmdb')
    except:
        return xs
    else: 
        return [
                (recordId, record, data, ga_body, user_agent, extract_ip_data(reader, user_agent, ip))
                for recordId, record, data, ga_body, user_agent, ip in xs
                ]
    finally:
        reader.close()

def convert_tuple_to_dict(xs: list) -> list:
    return [
            {
             'recordId': recordId, 
             'result': 'Ok', 
             'data': dict(data, **{'body':ga_body}, **{'ua_detected': user_agent}, **{'geo': ip}),
             }
            for recordId, record, data, ga_body, user_agent, ip in xs
            ]
          

def json_b64_encode(xs: list) -> list:
    try:
        return [
                {
                 'recordId': record['recordId'],
                 'result': record['result'],
                 'data': b64encode((json.dumps(record['data']) + '\n').encode())
                }
                for record in xs
                ]
    except Exception as e:
         print(e)
         return [
                {
                 'recordId': record['recordId'],
                 'result': 'ProcessingFailed',
                 'data': json.dumps(record['data'])
                }
                for record in xs
                ]

def program(event: dict) -> list:
    return pipe([
            partial(get_prop, 'records'),
            decode_data,
            parse_ga_body_payload,
            parse_user_agent,
            ip_lookup,
            convert_tuple_to_dict, 
            json_b64_encode,
           ]) (event)

def handler(event: dict, ctx: dict) -> list:
    return program(event) 

