import boto3
import os

client = boto3.client("glue")

JOB_NAME = os.environ["JOB_NAME"]

def run_job():
    response = client.start_job_run(
            JobName = JOB_NAME,
            Arguments = {
            "--year_partition": "2019",
            "--month_partition": "09",
            "--day_partition": "20"
            })

def main(event, ctx):
    run_job()


if __name__ == "__main__":
    main(None, None)
