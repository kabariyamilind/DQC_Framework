from pyspark.sql import  DataFrame, SQLContext
import boto3
from pydeequ.checks import *
from pydeequ.verification import *

from src.main.python.CheckBuilder import CheckBuilder
from src.main.python.ConstraintFailureDataIdentifier import ConstraintFailureDataIndetifier


class DataValidate():
    def __init__(self,env):
        self.env = env
        self.checkBuilder = CheckBuilder(self.env)
        self.constraintFailureDataIdentifier = ConstraintFailureDataIndetifier(self.env)
        pass

    def getDataValidated(self, spark, inputDF, dictColMatMapping, scalaDictColMatMapping, data_date,check_level,metricsDF):
        if check_level.lower() == "error":
            check = Check(spark, CheckLevel.Error, dictColMatMapping['check_name'])
        else:
            check = Check(spark, CheckLevel.Warning, dictColMatMapping['check_name'])

        print(dictColMatMapping)
        # check
        # list_test =
        # check = check.satisfies("p_totalprice>5000","custom_statisfy",lambda x:x>0.23)
        builded_check = self.checkBuilder.getChecksBuild(check,dictColMatMapping['check_params'],inputDF)
        checkResults = VerificationSuite(spark)\
            .onData(inputDF)\
            .addCheck(builded_check)\
            .run()

        checkResult_df = VerificationResult.checkResultsAsDataFrame(spark, checkResults)
        failed_checks = checkResult_df.filter(checkResult_df['constraint_status'] == 'Failure')
        failed_checks_list = failed_checks.collect()
        print(failed_checks_list)
        # for failure in failed_checks_list:
        #     print(failure['check'])
        #     self.constraintFailureDataIdentifier\
        #         .alertForConstraintFailure\
        #         (spark,failure['check'], failure['check_level'], failure['check_status'],
        #          failure['constraint'], failure['constraint_status'], failure['constraint_message'])
        # failed_checks.show(100,truncate=False)
        # print(checkResult_df.printSchema())
        if "CHECKMETRICS" in [key.upper() for key in dictColMatMapping['check_params'].keys()]:
            metricsCheckDf = self.checkBuilder.getMetricsChecked(spark,dictColMatMapping['check_params'],inputDF,metricsDF,data_date,dictColMatMapping['check_name'])
            finaldf = checkResult_df.union(metricsCheckDf)
            finaldf.show(100, truncate=False)
            return finaldf
        else:
            return checkResult_df



        # print(checkResults.checks)