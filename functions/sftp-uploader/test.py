import pysftp
import paramiko
from base64 import decodebytes
import io
from pathlib import Path
import os
from os.path import expanduser
import boto3

ssm = boto3.client("ssm")
key_password = ssm.get_parameter(Name="odoscope_key_password")["Parameter"]["Value"]
key_username = ssm.get_parameter(Name="odoscope_username")["Parameter"]["Value"]
key_hostname = ssm.get_parameter(Name="odoscope_host")["Parameter"]["Value"]

directory = "/incoming/"
f = open(os.path.join(expanduser('~'), ".ssh", "odoscope-new.key"), "r")
s = f.read()
keyfile = io.StringIO(s)
mykey = paramiko.RSAKey.from_private_key(keyfile, password= key_password)

keypath = os.path.join(expanduser('~'), ".ssh", "odoscope")

### test
ssh = paramiko.SSHClient()
ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
ssh.connect(hostname = key_hostname, username = key_username, pkey= mykey)
