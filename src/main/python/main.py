# This is a sample Python script.
import os
import sys

from pyspark.sql import SparkSession
import logging
from src.main.python.Controller import Controller
from src.main.python.ParseConfig import ParseConfig
from pathlib import Path

sys.path.append(str(Path.cwd())+'\\src\\main\\resource\\')
directory = str(Path.cwd())+'\\src\\main\\resource\\'
sys.path.append(os.path.join(directory, 'TableMetricsConfig.yaml'))
# test_path = os.getcwd().split("\\")[:-2]
# print(os.getcwd())
# sys.path.append(os.getcwd()+"\\src\\main\\resource\\")

# os.path.join(os.getcwd(),"\\src\\test\\resource")

def process_start(spark, path,env):
    # Use a breakpoint in the code line below to debug your script.
    yaml_path = path
    parseConfig = ParseConfig(spark)
    yaml_configs = parseConfig.parseYAMLConfig(yaml_path)
    profile_or_checks ='profiles'
    print(yaml_configs[0]['metrics_config'])
    if yaml_configs[0]['metrics_config']['profiles'] != None:
        for profile in yaml_configs[0]['metrics_config']['profiles']:
            dictColMatMappingCurrent = parseConfig.prepareDictonary(yaml_configs[0]['metrics_config']['profiles'][profile]['profile_setup_current'])
            # dictColMatMappingNew = parseConfig.prepareDictonary(yaml_configs[0]['metrics_config']['profiles'][profile]['profile_setup_new'])
            # dictColMatMapping = {"Completeness": to_scala_seq(spark._jvm, ["pk"])}
            logging.info(dictColMatMappingCurrent)
            scalaDictColMatMapping = spark._jvm.PythonUtils.toScalaMap(dictColMatMappingCurrent)
            controller = Controller(spark, yaml_configs, scalaDictColMatMapping,profile,profile_or_checks,env)
            controller.processFlow()

    profile_or_checks = 'checks'
    if yaml_configs[0]['metrics_config']['checks'] != None:
        for checks in yaml_configs[0]['metrics_config']['checks']:
            print(checks)
            dictColMatMappingCurrent = parseConfig.prepareDictonary(yaml_configs[0]['metrics_config']['checks'][checks]['check_params'])
            # dictColMatMappingNew = parseConfig.prepareDictonary(yaml_configs[0]['metrics_config']['profiles'][profile]['profile_setup_new'])
            # dictColMatMapping = {"Completeness": to_scala_seq(spark._jvm, ["pk"])}
            logging.info(dictColMatMappingCurrent)
            scalaDictColMatMapping = spark._jvm.PythonUtils.toScalaMap(dictColMatMappingCurrent)
            controller = Controller(spark, yaml_configs, scalaDictColMatMapping,checks,profile_or_checks,env)
            controller.processFlow()
    spark.sparkContext._gateway.shutdown_callback_server()
    spark.stop()



# Press the green button in the gutter to run the script.
if __name__ == '__main__':
    #JAR path and params coming from s3
    spark = SparkSession.builder \
            .master("local") \
            .appName("test_App") \
            .config("spark.jars", "C:\\Users\\\\PycharmProjects\\DQC_and_Profiling\\src\\main\\resource\\jars\\*")\
            .getOrCreate()

    process_start(spark)
    yaml_path = "TableMetricsConfig.yaml"
    parseConfig = ParseConfig(spark)
    yaml_configs = parseConfig.parseYAMLConfig(yaml_path)
    dictColMatMappingCurrent = parseConfig.prepareDictonary(yaml_configs[0]['metrics_config']['profile_setup_current'])
    dictColMatMappingNew = parseConfig.prepareDictonary(yaml_configs[0]['metrics_config']['profile_setup_new'])
    # dictColMatMapping = {"Completeness": to_scala_seq(spark._jvm, ["pk"])}
    logging.info(dictColMatMappingCurrent)
    scalaDictColMatMapping = spark._jvm.PythonUtils.toScalaMap(dictColMatMappingCurrent)
    controller = Controller(spark, yaml_configs, scalaDictColMatMapping)
    controller.processFlow()
    # df = spark.read.parquet("C:\\Users\\mkabariya\\IdeaProjects\\DQProfiling\\src\\test\\resources\\data\\part-00031-a618a938-81ea-43d2-8520-5e6c78582193.c000.snappy.parquet")
    # outputDf.show(100)

    # schema = StructType([
    #     StructField(header[i], DateType(), True)
    #     if header[i] in dateFields
    #     else StructField(header[i], StringType(), True)
    #     for i in range(len(header))])


# See PyCharm help at https://www.jetbrains.com/help/pycharm/
