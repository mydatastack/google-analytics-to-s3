from functools import partial, reduce
import json
from urllib.parse import urlparse, parse_qsl 
from device_detector import SoftwareDetector, DeviceDetector
from base64 import b64decode, b64encode
import maxminddb
import boto3
client = boto3.client('s3')
import re
from typing import Generator
from flatten_json import flatten
import user_agents 

pipe = lambda fns: lambda x: reduce(lambda v, f: f(v), fns, x) 

parse_body_query = lambda data: dict(parse_qsl(data['body']))

#def detect(user_agent: str) -> dict:
#    device = SoftwareDetector(user_agent).parse()
#    is_bot = device.is_bot()
#    if is_bot:
#        return {'is_bot': True}
#    else:
#        return {
#                'is_bot': False, 
#                'client_name': device.client_name(), 
#                'client_type': device.client_type(),
#                'client_version': device.client_version(),
#                'os_name': device.os_name(),
#                'os_version': device.os_version(),
#                'device_type': 'smartphone' if device.os_name() == 'iOS' else device.device_type(),
#                'is_mobile': True if device.device_type() == 'smartphone' or device.os_name() == 'iOS' else False,
#                } 
#

def client_type(ua):
    if ua.is_mobile:
        return 'mobile'
    elif ua.is_tablet:
        return 'tablet'
    elif ua.is_pc:
        return 'desktop'
    else:
        return '(not set)'

def detect(ua: str) -> dict:
    user_agent = user_agents.parse(ua)
    if user_agent.is_bot:
        return {'is_bot': True}
    else:
        return {
                'is_bot': False,
                'client_name': user_agent.browser.family,
                'client_version': user_agent.browser.version_string,
                'os_name': user_agent.os.family,
                'os_version': user_agent.os.version_string,
                'device_type': client_type(user_agent),
                'is_mobile': user_agent.is_mobile,
                'device_name': user_agent.device.family,
                'device_brand': user_agent.device.brand,
                'device_model': user_agent.device.model,
                'device_input': '(not set)',
                'device_info': '(not set)', 
                }

def s3_event_adapter(event: dict) -> Generator[str, None, None]:
    s3 = event['Records'][0]['s3']
    bucket = s3['bucket']['name']
    key = s3['object']['key']
    obj = client.get_object(Bucket=bucket, Key=key) 
    body = obj['Body']
    yield body.read().decode('utf-8')

def frh_json(lines: Generator[str, None, None]) -> Generator[str, None, None]:
    return ( 
            re.sub("}{", "}\n{", line, flags=re.UNICODE)  
            for line in lines
           )

def dec(data):
    print(data)
    return json.loads(data)


def parse_ga_body_payload_generator(xs: Generator[str, None, None]) -> Generator[str, None, None]:
    return (
            (entry, parse_body_query(entry), entry['user_agent'], entry['ip'])
            for data in xs
            for entry in data
            )

def split_files(data: Generator[str, None, None]) -> Generator[str, None, None]:
    for x in data:
        yield x.splitlines()

def json_decode(data: Generator[str, None, None]) -> Generator[str, None, None]:
    for x in data:
       yield [json.loads(line) for line in x] 
    
def parse_user_agent_generator(xs: Generator[str, None, None]) -> Generator[str, None, None]:
    return (
            (data, ga_body, detect(user_agent), ip)
            for data, ga_body, user_agent, ip in xs
            )

def extract_ip_data(reader, user_agent: dict, ip: str) -> dict:
    if user_agent['is_bot']:
        return {'ip': ip}
    else:
        try:
            location = reader.get(ip)
        except Exception as e:
            return {
                    'city': '(not set)',
                    'sub_continent': '(not set)',
                    'postal_code': '(not set)', 
                    'region': '(not set)',
                    'metro': '(not set)',
                    'city_id': '(not set)',
                    'country': '(not set)', 
                    'country_iso': '(not set)', 
                    'continent': '(not set)', 
                    'continent_code': '(not set)',
                    'longitude': '(not set)', 
                    'latitude': '(not set)', 
                    'timezone': '(not set)',
                    'network_domain': '(not set)',
                    'network_location': '(not set)',
                    } 
        else:
            try: 
                return {
                        'continent': location['continent']['names']['en'],
                        'sub_continent': '(not set)',
                        'country': location['country']['names']['en'],
                        'region': location['subdivisions'][0]['names']['en'],
                        'metro': '(not set)',
                        'city': location['city']['names']['en'],
                        'city_id': location['city']['geoname_id'],
                        'network_domain': '(not set)',
                        'network_location': '(not set)',
                        'postal_code': location['postal']['code'],
                        'country_iso': location['country']['iso_code'],
                        'continent_code': location['continent']['code'],
                        'longitude': location['location']['longitude'],
                        'latitude': location['location']['latitude'],
                        'timezone': location['location']['time_zone']
                        } 
            except KeyError as e:
                return {
                        'city': '(not set)',
                        'sub_continent': '(not set)',
                        'postal_code': '(not set)', 
                        'region': '(not set)',
                        'metro': '(not set)',
                        'city_id': '(not set)',
                        'country': '(not set)', 
                        'country_iso': '(not set)', 
                        'continent': '(not set)', 
                        'continent_code': '(not set)',
                        'longitude': '(not set)', 
                        'latitude': '(not set)', 
                        'timezone': '(not set)',
                        'network_domain': '(not set)',
                        'network_location': '(not set)',
                        } 


def ip_lookup_generator(xs: Generator[str, None, None]) -> Generator[str, None, None]:
    try:
        reader = maxminddb.open_database('./mmdb/GeoLite2-City.mmdb')
    except Exception as e:
        print(e)
        print('something goes wrong, becauset there is no file it throws an error')
        return xs
    else: 
        return (
                (data, ga_body, user_agent, extract_ip_data(reader, user_agent, ip))
                for data, ga_body, user_agent, ip in xs
                )
    #finally:
        #reader.close()
        
def convert_tuple_to_dict_generator(xs: Generator[str, None, None]) -> Generator[str, None, None]:
    return (
            dict(data, **{'body':ga_body}, **{'ua_detected': user_agent}, **{'geo': ip})
            for data, ga_body, user_agent, ip in xs
            )

def flatten_json_function(xs: Generator[dict, None, None]) -> Generator[dict, None, None]:
    return (
            flatten(data) 
            for data in xs
           )

def write_output(xsgen: Generator[str, None, None]) -> ():
    with open('output.jsonl', 'a') as f:
        for line in xsgen:
            f.write(str(line) + '\n')

def s3_list_adapter(entry):
    bucket = entry.bucket_name
    key = entry.key
    obj = client.get_object(Bucket=bucket, Key=key)
    body = obj['Body']
    yield body.read().decode('utf-8')

def log_generator(iterations: int, xs: Generator[str, None, None]) -> ():
    counter = 0
    for x in xs:
        if counter == iterations:
            break
        else:
            counter += 1
            print(x)

def program(event: dict) -> Generator[str, None, None]:
    return pipe([
            s3_list_adapter,
            #s3_event_adapter,
            frh_json,
            split_files,
            json_decode,
            parse_ga_body_payload_generator,
            parse_user_agent_generator,
            ip_lookup_generator,
            #partial(log_generator, 10),
            convert_tuple_to_dict_generator,
            flatten_json_function,
            (lambda dct: (json.dumps(line) for line in dct))
           ]) (event)

def handler(event: dict, ctx: dict) -> str:
    try:
        write_output(program(event))
    except Exception as e:
        print(e)
        print('the message comes from exception')
        return 'error'
    else:
        return 'success'

if __name__ == '__main__':
    import unittest

    class TestHandler(unittest.TestCase):
        def test_read_file_data(self):
            pass
        def test_run_handler(self):
            with open('payload_s3.json') as file:
                try:
                    event = json.load(file)
                except:
                    print('payload.json can\'t be parsed')
                else:
                    self.assertEqual(handler(event, None), 'success')

    unittest.main()

