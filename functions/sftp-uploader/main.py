import os
import paramiko
import shutil
from datetime import date, datetime, timedelta
import boto3
import re
import io
from functools import reduce, partial

pipe = lambda fns: lambda x: reduce(lambda v, f: f(v), fns, x)

s3 = boto3.resource("s3")
s3_client = boto3.client("s3")
ssm = boto3.client("ssm")
dynamodb = boto3.resource("dynamodb")

key_password = ssm.get_parameter(Name="odoscope_key_password")["Parameter"]["Value"]
key_username = ssm.get_parameter(Name="odoscope_username")["Parameter"]["Value"]
key_hostname = ssm.get_parameter(Name="odoscope_host")["Parameter"]["Value"]
key_ssh = ssm.get_parameter(Name="odoscope_ssh_key", WithDecryption=True)["Parameter"]["Value"]

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

def get_table_name():
    try:
        return os.environ["UPLOAD_STATE_TABLE"]
    except KeyError:
        return "upload_state"

S3_BUCKET = get_bucket_url()
date_folder = "/year=" + yesterday_year + "/month=" + yesterday_month + "/day=" + yesterday_day + "/"

def list_bucket_content(bucket):
    return [objects.key for objects in s3.Bucket(bucket).objects.all()]

def filter_files(keys: list) -> list:
    return [ file_object for file_object in keys if bool(re.findall(r"^aggregated\/ga" + re.escape(date_folder) + "(sessions|pageviews|products|events)" + ".*csv$", file_object)) ] 

def tmp_download(keys: list) -> ():
    return [s3_client.download_file(S3_BUCKET, key, "/tmp/" + key.split("/")[5] + "_" + file_date + ".csv") for key in keys]

def zip_tmp(y_year, y_month, y_day, *args) -> ():
    shutil.make_archive("/tmp/" + y_year + y_month + y_day, "zip", "/tmp")

def get_ssh_key(key_password: str, key_ssh: str, *args):
    try:
        keyfile = io.StringIO(key_ssh)
        mykey = paramiko.RSAKey.from_private_key(keyfile, password= key_password)
    except Exception as e:
        print(e)
        raise Exception("couldn't get the ssh key") 
    else:
        return mykey

def sftp_connect(mykey: str):
    try:
        ssh = paramiko.SSHClient()
        ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        ssh.connect(hostname = key_hostname, username = key_username, pkey= mykey)
    except Exception as e:
        print(e)
        raise Exception("cloudn't connect to the sftp")
    else:
        return ssh

def sftp_upload(ssh) -> ():
    try:
        ftp_client = ssh.open_sftp()
        ftp_client.put(f"/tmp/{file_date}.zip", f"/incoming/{file_date}.zip")
        ftp_client.close()
    except Exception as e:
        print(e)
        raise Exception("count't upload the files to sftp")
    else:
        return "success"

def construct_params(yy, ym, yd, *args):
    now = datetime.now()
    current_year = now.year
    iso_timestamp = now.isoformat()
    compressed_files = filter_files(list_bucket_content(S3_BUCKET))
    return ("success", {
               "year": str(current_year),
               "uploaded_at": str(iso_timestamp),
               "compressed_files": compressed_files, 
               "uploaded_file_name": f"{yy}{ym}{yd}.zip",
               "status": "success"
                })

def db_state_update(table_name, params):
    if params[0] == "success":
        table = dynamodb.Table(table_name)
        table.put_item(
                Item = params[1],
                ReturnValues = "NONE"

                )
        return "success"
    else:
        return "error"


def handler(event: dict, ctx: dict) -> ():
    return pipe([
        list_bucket_content,
        filter_files,
        tmp_download,
        partial(zip_tmp, yesterday_year, yesterday_month, yesterday_day),
        partial(get_ssh_key, key_password, key_ssh),
        sftp_connect,
        sftp_upload,
        partial(construct_params, yesterday_year, yesterday_month, yesterday_day), 
        partial(db_state_update, get_table_name()),
    ]) (S3_BUCKET)

if __name__ == '__main__':
    import unittest

    class TestHandler(unittest.TestCase):
        @unittest.skip("testing db")
        def test_run_handler(self):
            self.assertEqual(handler(None, None), "success")

        @unittest.skip("testing db")
        def test_filter_files(self):
            url = f"aggregated/ga{date_folder}events/part-00000-3418d3cf-68e9-486f-ae13-f60d3d44ed94-c000.csv"
            self.assertEqual(filter_files([url]), [url])

        def test_db_state_update(self):
            params = construct_params(yesterday_year, yesterday_month, yesterday_day) 
            self.assertEqual(db_state_update(get_table_name(), params), "success")

    unittest.main()

