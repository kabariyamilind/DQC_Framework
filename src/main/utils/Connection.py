import boto3
import json
class Connection():
    def __init__(self,spark):
        self.spark = spark


    def getRedshiftConnectionParams(self):
        
        sm_client = boto3.client(service_name='secretsmanager'
                                 , region_name="ap-south-1")
        get_secret_value_response = sm_client.get_secret_value(SecretId="")
        # print(get_secret_value_response)
        sec_dir = get_secret_value_response['SecretString']
        # print(sec_dir)
        sec_values = json.loads(sec_dir)
        user = sec_values['username']
        passwd = sec_values['password']
        host = sec_values['host']
        port = sec_values['port']
        return user,passwd,host,port

    def getHanaConnectionParams(self):
        sm_client = boto3.client(service_name='secretsmanager'
                                 , region_name="ap-south-1")
        get_secret_value_response = sm_client.get_secret_value(SecretId="")
        # print(get_secret_value_response)
        sec_dir = get_secret_value_response['SecretString']
        # print(sec_dir)
        sec_values = json.loads(sec_dir)
        user = sec_values['username']
        passwd = sec_values['password']
        host = sec_values['host']
        port = sec_values['port']
        return user, passwd, host, port