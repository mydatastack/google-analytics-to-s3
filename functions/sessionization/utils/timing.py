# start measuring runtime of the job
tsfm = "%Y-%m-%d %H:%M:%S"
time_start = datetime.now().strftime("%Y-%m-%d %H:%M:%S") 
job_start = datetime.strptime(time_start, tsfm) 
print("Job Start Time: ", job_start)


# measuring runtime of the script
time_end = datetime.now().strftime("%Y-%m-%d %H:%M:%S") 
job_end = datetime.strptime(time_end, tsfm) 
print("End time: ", job_end)
print("Running time: ", (job_end - job_start).seconds, " seconds")
