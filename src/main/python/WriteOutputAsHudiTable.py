import boto3
import time
class WriteOutputAsHudiTable():
    def __init__(self, yaml_configs,profile_or_check_name,profile_or_checks):
        self.yaml_configs = yaml_configs
        self.profile_or_check_name =profile_or_check_name
        self.profile_or_checks = profile_or_checks

    def writeOutputChecks(self, outputDf):
        hudiOptions = {
            "hoodie.table.name": "output_checks",
            "hoodie.datasource.write.recordkey.field": "constraint,data_date,check,input_source,table_name",
            "hoodie.datasource.write.precombine.field": "run_timestamp",
            'hoodie.datasource.write.partitionpath.field': "input_source,table_name,data_date,check",
            "hoodie.datasource.write.operation": "upsert",
            "hoodie.upsert.shuffle.parallelism": "1"
        }
        outputDf.write.format("org.apache.hudi") \
            .options(**hudiOptions) \
            .mode("Append") \
            .save(self.yaml_configs[0]['metrics_config']['checks'][self.profile_or_check_name]['metrics_write_path'])

    def writeOutputProfile(self,outputDf):
        hudiOptions = {
            "hoodie.table.name": "output_metrics",
            "hoodie.datasource.write.recordkey.field": "instance,name,data_date,profile_name,input_source,table_name",
            "hoodie.datasource.write.precombine.field": "run_timestamp",
            'hoodie.datasource.write.partitionpath.field': "input_source,table_name,data_date,profile_name",
            "hoodie.datasource.write.operation": "upsert",
            "hoodie.upsert.shuffle.parallelism":"1"
        }

        outputDf.write.format("org.apache.hudi") \
            .options(**hudiOptions) \
            .mode("Append") \
            .save(self.yaml_configs[0]['metrics_config']['profiles'][self.profile_or_check_name]['metrics_write_path'])

    def updatePartitionProfile(self, source_type, data_date, profile_name, table_name, hudi_table_name):
        query = "ALTER TABLE development.{5} ADD " \
                "PARTITION (" \
                "input_source = \'{0}\'," \
                "table_name=\'{3}\'," \
                "data_date=\'{1}\'," \
                "profile_name=\'{2}\') " \
                "LOCATION " \
                "\'{4}/{0}/{3}/{1}/{2}/\'".format(source_type, data_date, profile_name, table_name,self.yaml_configs[0]['metrics_config'][self.profile_or_checks][self.profile_or_check_name]['metrics_write_path'],hudi_table_name)

        print(query)
        athena_client = boto3.client('athena', region_name='ap-south-1')
        response = athena_client.start_query_execution(
            QueryString=query,
            QueryExecutionContext={
                'Database': 'development'
            }
        )
        executionId = response['QueryExecutionId']
        status = 'SUCCEEDED'
        while True:
            response = athena_client.get_query_execution(
                QueryExecutionId=executionId
            )
            status = response['QueryExecution']['Status']['State']
            if status == 'SUCCEEDED' or status == 'FAILED':
                time.sleep(1)
                break

    def updatePartitionCheck(self, source_type, data_date, profile_name, table_name, hudi_table_name):
        query = "ALTER TABLE development.{5} ADD " \
                "PARTITION (" \
                "input_source = \'{0}\'," \
                "table_name=\'{3}\'," \
                "data_date=\'{1}\'," \
                "check=\'{2}\') " \
                "LOCATION " \
                "\'{4}/{0}/{3}/{1}/{2}/\'".format(source_type, data_date, profile_name, table_name,self.yaml_configs[0]['metrics_config'][self.profile_or_checks][self.profile_or_check_name]['metrics_write_path'],hudi_table_name)

        print(query)
        athena_client = boto3.client('athena', region_name='ap-south-1')
        response = athena_client.start_query_execution(
            QueryString=query,
            QueryExecutionContext={
                'Database': 'development'
            }
        )
