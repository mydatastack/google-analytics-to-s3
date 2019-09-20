import boto3
from functools import reduce, partial
from main import handler
pipe = lambda fns: lambda x: reduce(lambda v, f: f(v), fns, x)

s3 = boto3.resource('s3')
bucket = s3.Bucket('dld-berlin-pilot-tarasowski-googleanal-databucket-1snopav0ewwct')
bucket_name = 'dld-berlin-pilot-tarasowski-googleanal-databucket-1snopav0ewwct' 

keys = [key for key in bucket.objects.filter()]

counter = 0

for key in keys:
    counter += 1
    print(counter)
    handler(key, None)
