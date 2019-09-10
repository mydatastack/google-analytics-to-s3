import boto3
client = boto3.client("s3")


def get_params(event):
    record = event["Records"][0]
    s3 = record["s3"]
    bucket = s3["bucket"]["name"]
    key = s3["object"]["key"]
    return (bucket, key)

def s3_trigger(event: dict) -> list:
    params = get_params(event)
    print(params)

def main(first=None):
    print("From the input adapter")
