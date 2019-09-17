import os
import paramiko
import shutil
from datetime import date, datetime, timedelta
import boto3
import re
import pysftp
s3 = boto3.resource("s3")
s3_client = boto3.client("s3")

yesterday = (datetime.now() - timedelta(1))
yesterday_year = yesterday.strftime("%Y")
yesterday_month = yesterday.strftime("%m")
yesterday_day = yesterday.strftime("%d")

file_date = yesterday_year + yesterday_month + yesterday_day

def get_bucket_url():
    try:
        return os.environ["S3_BUCKET"]
    except KeyError:
        return "tarasowski-main-dev-machine-googleanal-databucket-cl4te8jo5be1"

S3_BUCKET = get_bucket_url()
key = "/aggregated/ga" + "/year=" + yesterday_year + "/month=" + yesterday_month + "/day=" + yesterday_day + "/"
date_folder = "/year=" + yesterday_year + "/month=" + yesterday_month + "/day=" + yesterday_day + "/"
sessions = key + "sessions/"
products = key + "products/"
pageviews = key + "pageviews/"
events = key + "events/"

def filter_files(keys: list) -> list:
    return [ file_object for file_object in keys if bool(re.findall(r"^aggregated\/ga" + re.escape(date_folder) + "(sessions|pageviews|products|events)" + ".*csv$", file_object)) ] 

def list_bucket_content(bucket):
    return [objects.key for objects in s3.Bucket(bucket).objects.all()]

def tmp_download(keys: list) -> ():
    return [s3_client.download_file(S3_BUCKET, key, "/tmp/" + key.split("/")[5] + "_" + file_date + ".csv") for key in keys]

def zip_tmp():
    shutil.make_archive("/tmp/" + yesterday_year + yesterday_month + yesterday_day, "zip", "/tmp")

def ftp_upload():
    ssh = paramiko.SSHClient()
    ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    port = 22
    transport = paramiko.Transport((host, port))
    ssh.connect(host, port=port, look_for_keys=False, pkey = mykey) 
    


def handler(event: dict, ctx: dict) -> ():
    #x = list_bucket_content(S3_BUCKET)
    #y = filter_files(x)
    #z = tmp_download(y)
    #i = zip_tmp()
    ftp_upload()

if __name__ == '__main__':
    import unittest

    class TestHandler(unittest.TestCase):
        def test_read_file_data(self):
            pass
        def test_run_handler(self):
            self.assertEqual(handler(None, None), "success")

    unittest.main()

