import boto3
from functools import reduce, partial
from main import handler

pipe = lambda fns: lambda x: reduce(lambda v, f: f(v), fns, x)

s3 = boto3.resource('s3')
bucket = s3.Bucket('dld-berlin-pilot-tarasowski-googleanal-databucket-1snopav0ewwct')
bucket_name = 'dld-berlin-pilot-tarasowski-googleanal-databucket-1snopav0ewwct' 

keys = [key for key in bucket.objects.filter()]

first = keys[0]
key = first.key
splitted = key.split('/')
remove_folder = splitted[1:]
add_name = ['post-processed'] + remove_folder
new_key = '/'.join(add_name)

def create_new_key(old_key):
    return pipe([
            (lambda entry: entry.key),
            (lambda key: key.split('/')),
            (lambda xs: ['post-processed'] + xs[1:]),
            (lambda xs: '/'.join(xs)) 
            ]) (old_key)

new_keys = [create_new_key(old_key) for old_key in keys]

counter = 0

for key in keys:
    counter += 1
    print(counter)
    handler(key, None)
