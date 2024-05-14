from pyspark.sql.functions import *
from pyspark.sql.types import StructType,StructField, StringType

from src.main.python.ConstraintFailureDataIdentifier import ConstraintFailureDataIndetifier
from src.main.utils.Commons import Commons


class MetricsChecks():
    def __init__(self,env):
        self.env =env
        self.constraintFailureDataIndetifier = ConstraintFailureDataIndetifier(self.env)
        self.commons = Commons()

    schema_check_table = StructType([
        StructField('check', StringType(), True),
        StructField('check_level', StringType(), True),
        StructField('check_status', StringType(), True),
        StructField('constraint', StringType(), True),
        StructField('constraint_status', StringType(), True),
        StructField('constraint_message', StringType(), True),
    ])
    def dataframeBuilder(self,spark,check,check_level,check_status,constraint,constraint_status,constraint_message,paramsForDataReadForFailure):
        data = [( check, check_level,check_status,constraint,constraint_status,constraint_message)]
        print(data)
        df_check = spark.createDataFrame(data,self.schema_check_table)
        if constraint_status.lower() == 'failure':
            self.constraintFailureDataIndetifier.alertForConstraintFailure(spark,check,check_level,check_status,constraint,constraint_status,constraint_message,paramsForDataReadForFailure)
        df_check.show(100)
        return df_check

    def convert_keys_to_uppercase(self,dicttoconvert):
        new_dict = {}
        for key, value in dicttoconvert.items():
            new_key = key.upper()
            new_dict[new_key] = value
        return new_dict


    def getAllMetricsCheckedAndCreateFinalDataframe(self,spark, metrics_to_check,metricsDF,data_date,check_name):
        print(metrics_to_check)
        checkslist = metrics_to_check[0].keys()
        check_df = spark.createDataFrame([], self.schema_check_table)
        if metrics_to_check ==[]:
            return check_df

        for check in checkslist:
            if 'data_date' in metrics_to_check[0][check].keys():
                data_date = self.commons.getDataDate(metrics_to_check[0][check]['data_date'])
            if "HASRATIO" in check.upper():
                # print(metrics_to_check[0][check]
                numeratorParams = self.convert_keys_to_uppercase(self.convert_keys_to_uppercase(metrics_to_check[0][check])['NUMERATORPARAMS'])
                denominatorParams = self.convert_keys_to_uppercase(self.convert_keys_to_uppercase(metrics_to_check[0][check])['DENOMINATORPARAMS'])
                print(numeratorParams['INPUTSOURCE'])
                numerator_value = metricsDF.filter(
                    (col('input_source')==str(numeratorParams['INPUTSOURCE'])) &
                    (col('table_name')==numeratorParams['TABLENAME']) &
                    (col('name')==numeratorParams['NAME']) &
                    (col('instance')==numeratorParams['INSTANCE']) &
                    (col('data_date')==data_date)).select("value","profile_name").dropDuplicates()
                denominator_value = metricsDF.filter(
                    (col('input_source') == str(denominatorParams['INPUTSOURCE'])) &
                    (col('table_name') == denominatorParams['TABLENAME']) &
                    (col('name') == denominatorParams['NAME']) &
                    (col('instance') == denominatorParams['INSTANCE']) &
                    (col('data_date') == data_date)).select("value","profile_name").dropDuplicates()
                profilename_denominator = denominator_value.collect()[0][1]
                profilename_numerator = numerator_value.collect()[0][1]
                denominator_value =denominator_value.collect()[0][0]
                numerator_value = numerator_value.collect()[0][0]
                compare_sign = metrics_to_check[0][check]['compare_sign']
                paramsForDataReadForFailure = [numeratorParams,denominatorParams,profilename_numerator,profilename_denominator,data_date,compare_sign]
                if ">" == compare_sign:
                    if numerator_value/denominator_value > metrics_to_check[0][check]['compare_value']:
                        df = self.dataframeBuilder(spark,check_name,'Warning','Success','RatioConstraint(Ratio('+numeratorParams['INSTANCE']+','+denominatorParams['INSTANCE']+',None))','Success','',paramsForDataReadForFailure)
                    else:
                        df = self.dataframeBuilder(spark,check_name,'Warning','Warning','RatioConstraint(Ratio('+numeratorParams['INSTANCE']+','+denominatorParams['INSTANCE']+',None))','Failure','Value: '+str(numerator_value/denominator_value)+' does not meet the criteria',paramsForDataReadForFailure)
                elif "<" == compare_sign:
                    if numerator_value / denominator_value < metrics_to_check[0][check]['compare_value']:
                        df = self.dataframeBuilder(spark, check_name, 'Warning', 'Success',
                                                   'RatioConstraint(Ratio(' + numeratorParams['INSTANCE'] + ',' +
                                                   denominatorParams['INSTANCE'] + ',None))', 'Success', '',paramsForDataReadForFailure)
                    else:
                        df = self.dataframeBuilder(spark, check_name, 'Warning', 'Warning',
                                                   'RatioConstraint(Ratio(' + numeratorParams['INSTANCE'] + ',' +
                                                   denominatorParams['INSTANCE'] + ',None))', 'Failure',
                                                   'Value: ' + str(
                                                       numerator_value / denominator_value) + ' does not meet the criteria',paramsForDataReadForFailure)
                elif ">=" == compare_sign:
                    if numerator_value / denominator_value >= metrics_to_check[0][check]['compare_value']:
                        df = self.dataframeBuilder(spark, check_name, 'Warning', 'Success',
                                                   'RatioConstraint(Ratio(' + numeratorParams['INSTANCE'] + ',' +
                                                   denominatorParams['INSTANCE'] + ',None))', 'Success', '',paramsForDataReadForFailure)
                    else:
                        df = self.dataframeBuilder(spark, check_name, 'Warning', 'Warning',
                                                   'RatioConstraint(Ratio(' + numeratorParams['INSTANCE'] + ',' +
                                                   denominatorParams['INSTANCE'] + ',None))', 'Failure',
                                                   'Value: ' + str(
                                                       numerator_value / denominator_value) + ' does not meet the criteria',paramsForDataReadForFailure)
                elif "<=" == compare_sign:
                    if numerator_value / denominator_value <= metrics_to_check[0][check]['compare_value']:
                        df = self.dataframeBuilder(spark, check_name, 'Warning', 'Success',
                                                   'RatioConstraint(Ratio(' + numeratorParams['INSTANCE'] + ',' +
                                                   denominatorParams['INSTANCE'] + ',None))', 'Success', '',paramsForDataReadForFailure)
                    else:
                        df = self.dataframeBuilder(spark, check_name, 'Warning', 'Warning',
                                                   'RatioConstraint(Ratio(' + numeratorParams['INSTANCE'] + ',' +
                                                   denominatorParams['INSTANCE'] + ',None))', 'Failure',
                                                   'Value: ' + str(
                                                       numerator_value / denominator_value) + ' does not meet the criteria',paramsForDataReadForFailure)
                check_df = check_df.union(df)
            if "ISCOMPARE" in check.upper():
                # print(metrics_to_check[0][check]
                firstValParams = self.convert_keys_to_uppercase(
                    self.convert_keys_to_uppercase(metrics_to_check[0][check])['FIRSTVALPARAMS'])
                secondValParams = self.convert_keys_to_uppercase(
                    self.convert_keys_to_uppercase(metrics_to_check[0][check])['SECONDVALPARAMS'])
                first_val_value = metricsDF.filter(
                    (col('input_source') == str(firstValParams['INPUTSOURCE'])) &
                    (col('table_name') == firstValParams['TABLENAME']) &
                    (col('name') == firstValParams['NAME']) &
                    (col('instance') == firstValParams['INSTANCE']) &
                    (col('data_date') == data_date)).select("value","profile_name").dropDuplicates()
                second_val_value = metricsDF.filter(
                    (col('input_source') == str(secondValParams['INPUTSOURCE'])) &
                    (col('table_name') == secondValParams['TABLENAME']) &
                    (col('name') == secondValParams['NAME']) &
                    (col('instance') == secondValParams['INSTANCE']) &
                    (col('data_date') == data_date)).select("value","profile_name").dropDuplicates()
                print(second_val_value.collect())

                profilename_secondValParams = second_val_value.collect()[0][1]
                profilename_firstValParams = first_val_value.collect()[0][1]
                first_val_value = first_val_value.collect()[0][0]
                second_val_value = second_val_value.collect()[0][0]
                compare_sign = metrics_to_check[0][check]['compare_sign']
                paramsForDataReadForFailure = [firstValParams, secondValParams,profilename_firstValParams,profilename_secondValParams,data_date,compare_sign,first_val_value,second_val_value]

                if ">" == compare_sign:
                    if first_val_value > second_val_value:
                        df = self.dataframeBuilder(spark, check_name, 'Warning', 'Success',
                                                   'IsCompareConstraint(Compare(' + firstValParams['INSTANCE'] + ',' +
                                                   secondValParams['INSTANCE'] + ',None))', 'Success', '',paramsForDataReadForFailure)
                    else:
                        df = self.dataframeBuilder(spark, check_name, 'Warning', 'Warning',
                                                   'IsCompareConstraint(Compare(' + firstValParams['INSTANCE'] + ',' +
                                                   secondValParams['INSTANCE'] + ',None))', 'Failure',
                                                   'Value: ' + str(first_val_value)+'>'+ str(second_val_value) + ' does not meet the criteria',paramsForDataReadForFailure)
                elif "<" == compare_sign:
                    if first_val_value < second_val_value:
                        df = self.dataframeBuilder(spark, check_name, 'Warning', 'Success',
                                                   'IsCompareConstraint(Compare(' + firstValParams['INSTANCE'] + ',' +
                                                   secondValParams['INSTANCE'] + ',None))', 'Success', '',paramsForDataReadForFailure)
                    else:
                        df = self.dataframeBuilder(spark, check_name, 'Warning', 'Warning',
                                                   'IsCompareConstraint(Compare(' + firstValParams['INSTANCE'] + ',' +
                                                   secondValParams['INSTANCE'] + ',None))', 'Failure',
                                                   'Value: ' + str(first_val_value)+'<'+ str(second_val_value) + ' does not meet the criteria',paramsForDataReadForFailure)
                elif ">=" == compare_sign:
                    if first_val_value >= second_val_value:
                        df = self.dataframeBuilder(spark, check_name, 'Warning', 'Success',
                                                   'IsCompareConstraint(Compare(' + firstValParams['INSTANCE'] + ',' +
                                                   secondValParams['INSTANCE'] + ',None))', 'Success', '',paramsForDataReadForFailure)
                    else:
                        df = self.dataframeBuilder(spark, check_name, 'Warning', 'Warning',
                                                   'IsCompareConstraint(Compare(' + firstValParams['INSTANCE'] + ',' +
                                                   secondValParams['INSTANCE'] + ',None))', 'Failure',
                                                   'Value: ' + str(first_val_value)+'>='+ str(second_val_value) + ' does not meet the criteria',paramsForDataReadForFailure)
                elif "<=" == compare_sign:
                    if first_val_value <= second_val_value:
                        df = self.dataframeBuilder(spark, check_name, 'Warning', 'Success',
                                                   'IsCompareConstraint(Compare(' + firstValParams['INSTANCE'] + ',' +
                                                   secondValParams['INSTANCE'] + ',None))', 'Success', '',paramsForDataReadForFailure)
                    else:
                        df = self.dataframeBuilder(spark, check_name, 'Warning', 'Warning',
                                                   'IsCompareConstraint(Compare(' + firstValParams['INSTANCE'] + ',' +
                                                   secondValParams['INSTANCE'] + ',None))', 'Failure',
                                                   'Value: ' + str(first_val_value)+'<='+ str(second_val_value) + ' does not meet the criteria',paramsForDataReadForFailure)
                elif "=" == compare_sign:
                    if first_val_value == second_val_value:
                        df = self.dataframeBuilder(spark, check_name, 'Warning', 'Success',
                                                   'IsCompareConstraint(Compare(' + firstValParams['INSTANCE'] + ',' +
                                                   secondValParams['INSTANCE'] + ',None))', 'Success', '',paramsForDataReadForFailure)
                    else:
                        df = self.dataframeBuilder(spark, check_name, 'Warning', 'Warning',
                                                   'IsCompareConstraint(Compare(' + firstValParams['INSTANCE'] + ',' +
                                                   secondValParams['INSTANCE'] + ',None))', 'Failure',
                                                   'Value: ' + str(first_val_value)+'='+ str(second_val_value) + ' does not meet the criteria',paramsForDataReadForFailure)

                check_df = check_df.union(df)
        return check_df


    
