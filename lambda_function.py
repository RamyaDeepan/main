import json
import time
import Consonants as constant
import datetime
import requests
# import pyarrow
import boto3
import pandas as pd
from io import BytesIO
from io import StringIO
from json import dumps


#from datetime import datetime as dt
bucket = 'rinith' # already created on S3
client = boto3.client('ssm')
sns = boto3.client("sns", region_name="eu-north-1")
ssm = boto3.client("ssm", region_name="eu-north-1")
s3_resource = boto3.resource('s3')
url = ssm.get_parameter(Name=constant.urlapi, WithDecryption=True)["Parameter"]["Value"]
#url = "https://results.us.securityeducation.com/api/reporting/v0.1.0/phishing"
my_headers = {'x-apikey-token' : 'YV5iYWWji13z25LX/ZAQOwmHKmHqdpQk8pQzIcSQ5hGl3cpog'}
bucket = 'api9085' # already created on S3
s3_prefix = "result1/csvfiles"
def get_datetime():
    dt = datetime.datetime.now()
    return dt.strftime("%Y%m%d"), dt.strftime("%H:%M:%S")
datestr, timestr = get_datetime()
fname = f"data_api_api_{datestr}_{timestr}.csv"
file_prefix = "/".join([s3_prefix, fname])
def send_sns_success():
    success_sns_arn = ssm.get_parameter(Name=constant.SUCCESSNOTIFICATIONARN, WithDecryption=True)["Parameter"]["Value"]
    component_name = constant.COMPONENT_NAME
    env = ssm.get_parameter(Name=constant.ENVIRONMENT, WithDecryption=True)['Parameter']['Value']
    success_msg = constant.SUCCESS_MSG
    sns_message = (f"{component_name} :  {success_msg}")
    print(sns_message, 'text')
    succ_response = sns.publish(TargetArn=success_sns_arn,Message=json.dumps({'default': json.dumps(sns_message)}),
        Subject= env + " : " + component_name,MessageStructure="json")
    return succ_response
def send_error_sns():
   
    error_sns_arn = ssm.get_parameter(Name=constant.ERRORNOTIFICATIONARN)["Parameter"]["Value"]
    env = ssm.get_parameter(Name=constant.ENVIRONMENT, WithDecryption=True)['Parameter']['Value']
    error_message=constant.ERROR_MSG
    component_name = constant.COMPONENT_NAME
    sns_message = (f"{component_name} : {error_message}")
    err_response = sns.publish(TargetArn=error_sns_arn,Message=json.dumps({'default': json.dumps(sns_message)}),    Subject=env + " : " + component_name,
        MessageStructure="json")
    return err_response
try:
    r = requests.get(url, headers=my_headers)
    while r.status_code == 403:

        time.sleep(30)
        print("The URL is not hit")
    if r.status_code != 404:
        print("the URl is HIT ")
        d=r.json()
        df = pd.DataFrame(d['data'])
        df1 = df[df.columns.drop(['attributes'])]
        a = [d.values() for d in df.attributes]
        b = [d.keys() for d in df.attributes]
        x = b.pop(0)
        df2 = pd.DataFrame(a, columns=x)
        print(df2)
        csv_buffer =StringIO()
        df2.to_csv(csv_buffer)
        s3_resource.Object(bucket, file_prefix).put(Body=csv_buffer.getvalue())
        print("CSV File Written")
        send_sns_success()
        
except:
    print("There is an error")
    send_error_sns()


def lambda_handler(event, context):
    # TODO implement
    return {
        'statusCode': 200,
        'body': json.dumps('Hello from Lambda!')
    }