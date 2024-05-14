from src.main.python.InputSourceAbstract import InputSourceAbstract

class S3DataAsInput():
    def __init__(self):
        pass

    def readInputDataCreateSparkDF(self, spark, inputDataParams,params):
        inputDataFrame = spark.read.parquet(inputDataParams['input_data_params']['input_file_path'])
        return inputDataFrame