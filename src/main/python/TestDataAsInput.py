from src.main.python.InputSourceAbstract import InputSourceAbstract
import logging

class TestDataAsInput(InputSourceAbstract):
    def __init__(self):
        pass

    def readInputDataCreateSparkDF(self, spark, inputDataParams,params):
        inputDataFrame = spark.read.parquet(inputDataParams['input_data_params']['input_file_path'])
        logging.info("input_df df count is {0}".format(inputDataFrame.count()))
        return inputDataFrame

class LocalMetricsTableRead():

    def readMetricsTable(self,spark,table_name):
        metricsDF = spark.read.format('hudi').load("C:/Users/mkabariya/OneDrive - Tata CLiQ/Desktop/hudi_output")
        return metricsDF