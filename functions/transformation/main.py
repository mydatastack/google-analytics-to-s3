from functools import partial, reduce
import json
from urllib.parse import urlparse, parse_qsl 
from base64 import b64decode, b64encode

pipe = lambda fns: lambda x: reduce(lambda v, f: f(v), fns, x) 

def get_prop(prop: str, record: dict) -> list:
    return record[prop]

def decode_records(record: dict) -> tuple:
    try:
        recordId = record['recordId']
        data = record['data']
        b64_encoded = b64decode(data)
        deserialized = json.loads(b64_encoded)
        return (recordId, deserialized)
    except:
        return ()

def decode_data(xs: list) -> list:
    return [
            decode_records(x)
            for x in xs
           ] 

def anonymize_ip(address):
    if address.find(".") > 0: # ipv4
        splitv4 = address.split(".")
        extracted = splitv4[:3]
        extracted.append('0')
        return ".".join(extracted)
    elif address.find(":") > 0: # ipv6
        splitv6 = address.split(":")
        extracted = splitv6[:3]
        extracted.extend(['0000', '0000', '0000', '0000', '0000'])
        return ":".join(extracted)
    else:
        return "0.0.0.0" 

def mask_ip(xs: dict) -> dict:
    return [
            (recordId, dict(body, **{"ip": anonymize_ip(body["ip"])}))
            for recordId, body in xs 
           ]

parse_body_query = lambda data: dict(parse_qsl(data['body']))

def parse_ga_body_payload(xs: list) -> list:
    return [
            (recordId, dict(data, **{"body": parse_body_query(data)})) 
            for recordId, data in xs
           ]

def flatten_dict(dd, separator='_', prefix=''):
    return { prefix + separator + k if prefix else k : v
             for kk, vv in dd.items()
             for k, v in flatten_dict(vv, separator, kk).items()
             } if isinstance(dd, dict) else { prefix : dd }

def flatten_body(xs: list) -> list:
    return [
            (recordId, dict(**flatten_dict(data)))
            for recordId, data in xs
           ]

def convert_tuple_to_dict(xs: list) -> list:
    return [
            {
             'recordId': recordId, 
             'result': 'Ok', 
             'data': data 
             }
            for recordId, data in xs
            ]
          

def json_b64_encode(xs: list) -> list:
    try:
        return [
                {
                 'recordId': record['recordId'],
                 'result': record['result'],
                 'data': b64encode(json.dumps(record['data']).encode('utf-8') + b'\n').decode('utf-8')
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
            mask_ip,
            parse_ga_body_payload,
            flatten_body,
            convert_tuple_to_dict, 
            json_b64_encode,
           ]) (event)

def handler(event: dict, ctx: dict) -> dict:
    records = program(event)
    return {'records': records} 

if __name__ == '__main__':
    import unittest

    class TestHandler(unittest.TestCase):
        def test_read_file_data(self):
            pass
        def test_run_handler(self):
            with open('payload.json') as file:
                try:
                    event = json.load(file)
                except:
                    print('payload.json can\'t be parsed')
                else:
                    self.assertEqual(len(handler(event, None).get('records')), 11)
        def test_anonymize_ipv4(self):
           self.assertEqual(anonymize_ip("255.255.255.255"), "255.255.255.0")

        def test_anonymize_ipv6(self):
            self.assertEqual(anonymize_ip("ffff:ffff:ffff:ffff:ffff:ffff:ffff:ffff"), "ffff:ffff:ffff:0000:0000:0000:0000:0000")

    unittest.main()
