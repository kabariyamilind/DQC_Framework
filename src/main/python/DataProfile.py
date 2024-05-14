from pyspark.sql import  DataFrame, SQLContext
import boto3
class DataProfile():

    def __init__(self,env):
        self.env = env
        pass

    def cleanOldStateIfExists(self,dictColMatMapping,data_date):
        if self.env.upper() in ['DEV','PROD']:
            conn = boto3.client(service_name="s3",region_name='ap-south-1')
            stateStorePath = "s3://dqc-metadata/DQC_statestore/"+self.env+"/"+dictColMatMapping['input_data_params']["source_type"]+"/"+dictColMatMapping['input_data_params']["table_name"]+"/"+dictColMatMapping['profile_name']+"/"+str(data_date)+"/"
            bucket = str(stateStorePath).split("/")[2]
            keyList = str(stateStorePath).split("/")[3:]
            key = "/".join(keyList)
            paginator = conn.get_paginator('list_objects_v2')
            pages = paginator.paginate(Bucket=bucket, Prefix=key)
            delete_us = dict(Objects=[])
            for item in pages.search('Contents'):
                if item == None:
                    break
                delete_us['Objects'].append(dict(Key=item['Key']))
                print(delete_us)
                if len(delete_us['Objects']) >= 1000:
                    conn.delete_objects(Bucket=bucket, Delete=delete_us)
                    delete_us = dict(Objects=[])
            if len(delete_us['Objects']):
                conn.delete_objects(Bucket=bucket, Delete=delete_us)
        else:
            pass
    def getDataProfiled(self, spark, inputDF, dictColMatMapping, scalaDictColMatMapping,data_date):
        # data = spark._jvm.org.tul.App.testFunction()
        # sqlContext = spark._wrapped
        sc = spark.sparkContext
        sql_context = SQLContext(sc)
        # This should come from process config file
        if self.env.upper() in ['DEV','PROD']:
            stateStorePath = "s3://dqc-metadata/DQC_statestore/"+self.env+"/"+dictColMatMapping['input_data_params']["source_type"]+"/"+dictColMatMapping['input_data_params']["table_name"]+"/"+dictColMatMapping['profile_name']+"/"+str(data_date)+"/"
        else:
            stateStorePath = "C:/Users/mkabariya/OneDrive - Tata CLiQ/Desktop/Test_Location_Store_1/store_file"+"/"+dictColMatMapping['input_data_params']["source_type"]+"/"+dictColMatMapping['input_data_params']["table_name"]+"/"+dictColMatMapping['profile_name']+"/"+str(data_date)+"/"

        # self.cleanOldStateIfExists(stateStorePath)
        # print(dictColMatMapping["state_store_path_daily"]+"/"+dictColMatMapping['input_data_params']["source_type"]+"/"+dictColMatMapping['input_data_params']["table_name"]+"/")
        # buildedAnalysis = AnalysisRunner(spark)
        # buildedProfile = self.profileBuilder.getProfilesBuild(buildedAnalysis,dictColMatMapping['profile_setup_current'])
        outputDf = DataFrame(spark._jvm.org.tul.App.getmetricsComputed(spark._jsparkSession,
                                                                       stateStorePath,
                                                                       inputDF._jdf,
                                                                       scalaDictColMatMapping)
                             ,sql_context)
        outputDf.show(100)
        return outputDf
