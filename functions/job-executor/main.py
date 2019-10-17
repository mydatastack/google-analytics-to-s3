import boto3
import os
from datetime import datetime, timedelta
from functools import reduce

client = boto3.client("glue")
pipe = lambda fns: lambda x: reduce(lambda v, f: f(v), fns, x)

def yesterday(_):
    yesterday = datetime.strftime(datetime.now() - timedelta(1), "%Y-%m-%d")
    yesterday_date = datetime.strptime(yesterday, "%Y-%m-%d").date()
    year = yesterday_date.strftime("%Y")
    month = yesterday_date.strftime("%m")
    day = yesterday_date.strftime("%d")
    return (year, month, day)

def env_vars(yesterday):
    year, month, day = yesterday 
    try:
        JOB_NAME = os.environ["JOB_NAME"]
        YEAR_PARTITION = os.environ["YEAR_PARTITION"]   
        MONTH_PARTITION = os.environ["MONTH_PARTITION"]
        DAY_PARTITION = os.environ["DAY_PARTITION"] 
        return (JOB_NAME, YEAR_PARTITION, MONTH_PARTITION, DAY_PARTITION)
    except:
        JOB_NAME = os.environ["JOB_NAME"]
        return (JOB_NAME, year, month, day)

def run_job(config):
    JOB_NAME, YEAR_PARTITION, MONTH_PARTITION, DAY_PARTITION = config
    return client.start_job_run(
            JobName = JOB_NAME,
            Arguments = {
            "--year_partition": YEAR_PARTITION, 
            "--month_partition": MONTH_PARTITION, 
            "--day_partition": DAY_PARTITION 
            })

def handler(event, ctx):
    return pipe([
            yesterday,
            env_vars,
            run_job,
            (lambda x: print(x) or "job started")
            ])(event)

if __name__ == "__main__":
    handler(None, None)
