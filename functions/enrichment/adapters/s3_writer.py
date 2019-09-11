from typing import Generator
import boto3
s3 = boto3.resource("s3")

def get_params(event: dict) -> tuple:
    record = event["Records"][0]
    s3 = record["s3"]
    bucket = s3["bucket"]["name"]
    key = s3["object"]["key"]
    return (bucket, key)

def change_key(params: tuple) -> tuple:
    bucket, key = params
    key_xs = key.split('/')[1:]
    key_modified = ["enriched"] + key_xs
    joined_key = '/'.join(key_modified)
    return (bucket, joined_key)


def s3_writer(event: dict, message: Generator[dict, None, None]) -> tuple:
    if message["result"] == "Ok":
        params = get_params(event)
        new_params = change_key(params)
        bucket, key = new_params
        s3.Object(bucket, key + ".jsonl").put(Body=message["data"]) 
        return (True, None, None)
    else:
        return (False, payload, event)

def s3_failed(info: tuple) -> ():
    success, payload, event = info
    if success:
        return "success"
    else:
        return "error"
