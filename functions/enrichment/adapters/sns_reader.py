import boto3
import json
import re
from functools import reduce
from typing import Generator
from urllib.parse import unquote
s3 = boto3.resource("s3")
pipe = lambda fns: lambda x: reduce(lambda v, f: f(v), fns, x)

def get_params(event: dict) -> tuple:
    record = event["Records"][0]
    sns = record["Sns"]
    message = sns["Message"]
    message_ = json.loads(message)
    record_ = message_["Records"][0]
    s3 = record_["s3"]
    bucket = s3["bucket"]["name"] 
    key = s3["object"]["key"]
    return (bucket, unquote(key))

def load_file(params: tuple) -> Generator[list, None, None]:
    try:
        obj = s3.Object(params[0], params[1])
    except Exception as e:
        print(e)
        yield []
    else:
        yield obj.get()['Body'].read().decode('utf-8')

def json_decode(payload):
    return (
            json.loads(element)
            for line in payload
            for element in line
           )
def frh_json(lines: Generator[str, None, None]) -> Generator[str, None, None]:
    return ( 
            re.sub("}{", "}\n{", line, flags=re.UNICODE)  
            for line in lines
           )
def split_files(data: Generator[str, None, None]) -> Generator[str, None, None]:
    return (
            x.splitlines()
            for x in data
            )


def sns_reader(event: dict) -> list:
    return pipe([
            get_params,
            load_file,
            frh_json,
            split_files,
            json_decode,
            ]) (event)
