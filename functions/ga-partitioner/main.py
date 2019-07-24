import json
from functools import partial, reduce
from collections import namedtuple
from urllib.parse import unquote
from itertools import groupby
import boto3
import time
import re

s3 = boto3.resource('s3')
pipe = lambda *args: lambda x: reduce(lambda a, fn: fn(a), args, x)

S3Object = namedtuple('S3MetaData', ('bucket', 'key'))


def get_records(event: dict) -> list: 
    return event['Records']

def get_list(event: list) -> list:
    return [
            el['s3'] 
            for el in event
           ]

def get_s3metadata(s3meta: list) -> S3Object: 
    return [
            S3Object(el['bucket']['name'], unquote(el['object']['key'])) 
            for el in s3meta
           ]

def load_file(m: list) -> str:
    try:
        obj = s3.Object(m[0].bucket, m[0].key)
        return obj.get()['Body'].read().decode('utf-8')
    except Exception as e:
        print(e) 
        return [] 


def split_str(data: str) -> list: 
    return data.split('\n') 

def remove_new_lines(data: list) -> list:
    return [
            line 
            for line in data
            if line
           ]

def decode_json(data: list) -> list:
    return [
            json.loads(line)
            for line in data
           ]
def get_valid_filename(s):
    s = str(s).strip().replace(' ', '_')
    return re.sub(r'(?u)[^-\w.]', '', s).lower()[:50]

def take_props(data: list) -> list:
    by_types = []
    for el in data:
        if el['body']['t'] == 'event':
            by_types.append(
                 (el['body']['tid'].lower(), el['body']['ds'], el['body']['t'], get_valid_filename(el['body']['ec']), el)
                 ) 
        else: 
            by_types.append(
                    (el['body']['tid'].lower(), el['body']['ds'], el['body']['t'], 'all', el)
                    )
    return by_types

def group_by_ds(data: list) -> list:
    return [
            (tid, ds, event, event_type, list(dt for tid, ds, ev, et, dt in data)) 
            for tid, g1 in groupby(data, key=lambda x: x[0])
            for ds, g2 in groupby(g1, key=lambda x: x[1])
            for event, g3 in groupby(g2, key=lambda x: x[2])
            for event_type, data in groupby(g3, key=lambda x: x[3])
            ]

def sort_data(data: list) -> list:
    return sorted(data, key=lambda t: (t[0],t[1],t[2],t[3])) 

def folder_name_events(*args):
    return f'system_source={args[0]}/tracking_id={args[1]}/data_source={args[2]}/event_type={args[3]}_{args[4]}/{args[5]}'

def folder_name_all(*args):
    return f'system_source={args[0]}/tracking_id={args[1]}/data_source={args[2]}/event_type={args[3]}/{args[4]}'


def construct_keys(event: dict, data: list) -> list:
     keys = pipe(
                sns_adapter,
                get_list,
                get_s3metadata,
            ) (event)          
     bucket = keys[0].bucket
     folders = keys[0].key.split('/')
     base_folders = "/".join(folders[1:2])
     partition_folders = "/".join(folders[2:6])
     with_folder = []
     for tid, ds, event, event_type, body in data:
         if event_type != 'all':
             with_folder.append(
                        (bucket, 
                            tid, 
                            folder_name_events(
                                    base_folders, 
                                    tid, 
                                    ds,
                                    event, 
                                    event_type, 
                                    partition_folders
                            ),
                            ds, 
                            event, 
                            event_type,
                            body) 
                     )
         else:
             with_folder.append(
                        (bucket, 
                            tid, 
                            folder_name_all(
                                    base_folders, 
                                    tid, 
                                    ds,
                                    event, 
                                    partition_folders
                            ),
                            ds, 
                            event, 
                            event_type,
                            body) 
                     )
     return with_folder


def construct_files(data, ts=time.strftime("%Y-%m-%dT%H:%M:%S%z", time.gmtime())):  
    bucket, tid, folder, ds, event, event_type, body = data
    key = f'{tid}-{event}_{event_type}-{ts}' if event_type != 'all' else f'{tid}-{event}-{ts}'
    body_json = [json.dumps(record) for record in body]
    new_line_delimited = '\n'.join(body_json)
    return s3.Object(bucket, 'processed/' + folder + '/' + key).put(Body=new_line_delimited)

def save_to_s3(data: list, ts=time.strftime("%Y-%m-%dT%H:%M:%S%z", time.gmtime())) -> str:
    try:
        operations = [construct_files(slice) for slice in data] 
        return 'success'
    except Exception as e:
        print('it comes from the exception')
        print(e) 
        return e

def sns_adapter(event):
    records = event['Records']
    messages = [record['Sns']['Message'] for record in records]
    try:
        decoded = [json.loads(message) for message in messages]
        records_list = [record['Records'] for record in decoded]
        flat_records_list = [
                item
                for sublist in records_list
                for item in sublist
                ]
        return flat_records_list
    except Exception as e:
        print(e)
        return []

def program(event):
    return pipe( 
                sns_adapter,
                get_list,
                get_s3metadata,
                load_file,
                split_str,
                remove_new_lines,
                decode_json,
                take_props,
                sort_data,
                group_by_ds,
                partial(construct_keys, event),
             )(event)

def handler(event, ctx):
    try:
        return save_to_s3(program(event))
    except Exception as e:
        print(e)
        return e 
        
