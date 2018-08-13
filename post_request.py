from googleapiclient.discovery import build
from oauth2client.client import GoogleCredentials

credentials = GoogleCredentials.get_application_default()
service = build('dataflow', 'v1b3', credentials=credentials)

# Set the following variables to your values.
JOBNAME = '[JOBNAME]'
PROJECT = '[PROJECT]'
BUCKET = '[BUCKET]'
TEMPLATE = '[TEMPLATE-NAME]'

GCSPATH = "gs://{bucket}/dataflow/{template}".format(bucket=BUCKET, template=TEMPLATE)
BODY = {
    "jobName": "{jobname}".format(jobname=JOBNAME),
    "parameters": {
        "path": "gs://{bucket}/demo/file.csv".format(bucket=BUCKET),
        "source": "source_name",
    },
    "environment": {
        "tempLocation": "gs://{bucket}/dataflow/temp".format(bucket=BUCKET),
    }
}

request = service.projects().templates().launch(projectId=PROJECT, gcsPath=GCSPATH, body=BODY)
response = request.execute()

print(response)
