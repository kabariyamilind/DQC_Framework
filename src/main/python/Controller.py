from src.main.python.AthenaTableAsInput import AthenaMericsTableRead
from src.main.python.DataValidate import DataValidate
from src.main.python.InputSourceFactory import InputSourceFactory
from src.main.python.DataFilter import DataFilter
from datetime import datetime, timedelta,date
from src.main.python.DataProfile import DataProfile
# from src.main.python.SchemaBuilder import SchemaBuilder
from pyspark.sql.functions import current_date, date_add, current_timestamp,lit
from dateutil.relativedelta import relativedelta

from src.main.python.TestDataAsInput import LocalMetricsTableRead
from src.main.python.WriteOutputAsHudiTable import WriteOutputAsHudiTable
from src.main.utils.Commons import Commons


class Controller():

    def __init__(self, spark, yaml_configs, scalaDictColMatMapping,profile_or_check_name,profile_or_checks,env):

        self.spark = spark
        self.env = env
        self.yaml_configs = yaml_configs
        self.inputFactory = InputSourceFactory()
        self.dataFilter = DataFilter()
        self.dataProfile = DataProfile(self.env)
        # self.schemaBuilder = SchemaBuilder()
        self.scalaDictColMatMapping = scalaDictColMatMapping
        self.profile_or_check_name = profile_or_check_name
        self.profile_or_checks = profile_or_checks
        self.writeOutputAsHudiTable = WriteOutputAsHudiTable(self.yaml_configs, self.profile_or_check_name,self.profile_or_checks)
        self.dataValidate = DataValidate(self.env)
        self.athenaMetricsTable = AthenaMericsTableRead()
        self.localMetricsTable = LocalMetricsTableRead()
        self.commons = Commons()

    def filterAndProfiling(self,params):
        """

        :param params:
        """
        print(params['data_date'])
        inputDF = self.inputFactory \
            .get_input_data_type(
            self.yaml_configs[0]['metrics_config'][self.profile_or_checks][self.profile_or_check_name]['input_data_params']['source_type']) \
            .readInputDataCreateSparkDF(self.spark, self.yaml_configs[0]['metrics_config'][self.profile_or_checks][self.profile_or_check_name],
                                        params)

        # ------------------------------------
        # Parsing of other params
        # ------------------------------------
        other_params = self.yaml_configs[0]['metrics_config'][self.profile_or_checks][self.profile_or_check_name]['other_params']

        # --------------------------------------------------------
        # Check if input data storage require for further analysis
        # --------------------------------------------------------
        if other_params['save_input_data'].upper() == 'YES':
            if self.yaml_configs[0]['metrics_config'][self.profile_or_checks][self.profile_or_check_name]['input_data_params']['source_type']!= 'local':
                dictColMatMapping = self.yaml_configs[0]['metrics_config'][self.profile_or_checks][self.profile_or_check_name]
                if self.profile_or_checks.upper() =='PROFILES':
                    inputDF.write.mode("overwrite").parquet("s3://dqc-metadata/DQC_input_data/"+self.env+"/"+dictColMatMapping['input_data_params']["source_type"]+"/"+dictColMatMapping['input_data_params']["table_name"]+"/"+dictColMatMapping['profile_name']+"/"+str(params['data_date'])+"/")
                if self.profile_or_checks.upper() == 'CHECKS':
                    inputDF.write.mode("overwrite").parquet(
                        "s3://dqc-metadata/DQC_input_data/"+self.env + "/" + dictColMatMapping['input_data_params'][
                            "source_type"] + "/" + dictColMatMapping['input_data_params']["table_name"] + "/" +
                        dictColMatMapping['check_name'] + "/" + str(params['data_date']) + "/")
            else:
                dictColMatMapping = self.yaml_configs[0]['metrics_config'][self.profile_or_checks][
                    self.profile_or_check_name]
                if self.profile_or_checks.upper() =='PROFILES':
                    inputDF.write.mode("overwrite").parquet("C:/Users//Desktop/Test_Location_Store_1/DQC_input_data/"+self.env+"/"+dictColMatMapping['input_data_params']["source_type"]+"/"+dictColMatMapping['input_data_params']["table_name"]+"/"+dictColMatMapping['profile_name']+"/"+str(params['data_date'])+"/")
                if self.profile_or_checks.upper() == 'CHECKS':
                    inputDF.write.mode("overwrite").parquet(
                        "C:/Users//Test_Location_Store_1/DQC_input_data/"+self.env + "/" + dictColMatMapping['input_data_params'][
                            "source_type"] + "/" + dictColMatMapping['input_data_params']["table_name"] + "/" +
                        dictColMatMapping['check_name'] + "/" + str(params['data_date']) + "/")
        print(params['start_date'],params['end_date'])

        # --------------------------------------------------------
        # Calculate Profile on input data
        # --------------------------------------------------------
        if self.profile_or_checks == "profiles":
            self.dataProfile.cleanOldStateIfExists(self.yaml_configs[0]['metrics_config'][self.profile_or_checks][
                                                           self.profile_or_check_name],str(params['data_date']))
            profileData = self.dataProfile.getDataProfiled(self.spark, inputDF,
                                                       self.yaml_configs[0]['metrics_config'][self.profile_or_checks][
                                                           self.profile_or_check_name], self.scalaDictColMatMapping,str(params['data_date']))
            runDateAdded = profileData.withColumn("run_timestamp", current_timestamp())

            # Add data_date based on the starting date of data filter # Think about how to handle filter based on different column type
            dataDateAdded = runDateAdded.withColumn("data_date", lit(params['data_date'].strftime('%Y-%m-%d')))
            profileNameAdded = dataDateAdded.withColumn("profile_name", lit(
                self.yaml_configs[0]['metrics_config'][self.profile_or_checks][self.profile_or_check_name][
                    'profile_name']))
            sourceTypeAdded = profileNameAdded.withColumn("input_source", lit(
                self.yaml_configs[0]['metrics_config'][self.profile_or_checks][self.profile_or_check_name][
                    'input_data_params']['source_type']))
            tableNameAdded = sourceTypeAdded.withColumn("table_name", lit(
                self.yaml_configs[0]['metrics_config'][self.profile_or_checks][self.profile_or_check_name][
                    'input_data_params']['table_name']))
            finalDataFrame = tableNameAdded.withColumn("description", lit(
                self.yaml_configs[0]['metrics_config'][self.profile_or_checks][self.profile_or_check_name]['description']))
            finalDataFrame.show()

            self.writeOutputAsHudiTable.writeOutputProfile(finalDataFrame)
            if self.env.upper() in ['DEV','PROD']:
                self.writeOutputAsHudiTable.updatePartitionProfile(
                    self.yaml_configs[0]['metrics_config'][self.profile_or_checks][self.profile_or_check_name][
                        'input_data_params']['source_type'],
                    params['data_date'].strftime('%Y-%m-%d'),
                    self.yaml_configs[0]['metrics_config'][self.profile_or_checks][self.profile_or_check_name][
                        'profile_name'],
                    self.yaml_configs[0]['metrics_config'][self.profile_or_checks][self.profile_or_check_name][
                        'input_data_params']['table_name'],
                    "dqc_hudi_"+self.env
                )

        if self.yaml_configs[0]['metrics_config'][self.profile_or_checks][self.profile_or_check_name]['input_data_params']['source_type']!= 'local':
            metricsDF = self.athenaMetricsTable.readMetricsTable(self.spark,"dqc_hudi_"+self.env)
        else:
            metricsDF = self.localMetricsTable.readMetricsTable(self.spark, "output_metrics")

        # --------------------------------------------------------
        # Calculate Check on input data
        # --------------------------------------------------------
        if self.profile_or_checks == "checks":
            validated_data = self.dataValidate.getDataValidated(self.spark, inputDF,
                                                       self.yaml_configs[0]['metrics_config'][self.profile_or_checks][
                                                           self.profile_or_check_name], self.scalaDictColMatMapping,str(params['data_date']),self.yaml_configs[0]['metrics_config'][self.profile_or_checks][self.profile_or_check_name]['check_level'],metricsDF)
            runDateAdded = validated_data.withColumn("run_timestamp", current_timestamp())

            # Add data_date based on the starting date of data filter # Think about how to handle filter based on different column type
            dataDateAdded = runDateAdded.withColumn("data_date", lit(params['data_date'].strftime('%Y-%m-%d')))
            sourceTypeAdded = dataDateAdded.withColumn("input_source", lit(
                self.yaml_configs[0]['metrics_config'][self.profile_or_checks][self.profile_or_check_name][
                    'input_data_params']['source_type']))
            tableNameAdded = sourceTypeAdded.withColumn("table_name", lit(
                self.yaml_configs[0]['metrics_config'][self.profile_or_checks][self.profile_or_check_name][
                    'input_data_params']['table_name']))
            finalDataFrame = tableNameAdded.withColumn("description", lit(
                self.yaml_configs[0]['metrics_config'][self.profile_or_checks][self.profile_or_check_name]['description']))
            finalDataFrame.show()
            self.writeOutputAsHudiTable.writeOutputChecks(finalDataFrame)
            if self.env.upper() in ['DEV', 'PROD']:
                self.writeOutputAsHudiTable.updatePartitionCheck(
                    self.yaml_configs[0]['metrics_config'][self.profile_or_checks][self.profile_or_check_name][
                        'input_data_params']['source_type'],
                    params['data_date'].strftime('%Y-%m-%d'),
                    self.yaml_configs[0]['metrics_config'][self.profile_or_checks][self.profile_or_check_name][
                        'check_name'],
                    self.yaml_configs[0]['metrics_config'][self.profile_or_checks][self.profile_or_check_name][
                        'input_data_params']['table_name'],
                    "dqc_hudi_checks_"+self.env
                )
        # finalDataFrame.write.partitionBy("input_source","data_date","table_name","profile_name").mode("overwrite").parquet(self.yaml_configs[0]['metrics_config'][self.profile_or_checks][self.profile]['metrics_write_path'])

    def processFlow(self):

        startDate = self.yaml_configs[0]['metrics_config'][self.profile_or_checks][self.profile_or_check_name]['start_date']
        endDate = self.yaml_configs[0]['metrics_config'][self.profile_or_checks][self.profile_or_check_name]['end_date']
        runType = self.yaml_configs[0]['metrics_config'][self.profile_or_checks][self.profile_or_check_name]['run_type']
        filterColumnName = self.yaml_configs[0]['metrics_config'][self.profile_or_checks][self.profile_or_check_name]['filter_column_name']
        if "current_date" in startDate.lower() or "current_date" in endDate.lower():
            startDate,endDate = self.commons.getStartDateAndEndDate(startDate,endDate)
            print(startDate,endDate)
        # schema = self.schemaBuilder.getSchemaBuiild(self.spark,self.yaml_configs[0]['metrics_config'][self.profile_or_checks][self.profile]['schema'])
        # print(schema)

        # test = self.spark._jvm.org.tul.App.testSchema(self.spark._jsparkSession, schema)
        # print(test)
        if len(filterColumnName) != 0:
            if startDate == "None" and endDate == "None":
                if runType == "daily":
                    start_date = (datetime.now() - timedelta(1)).replace(hour=00, minute=00, second=00, microsecond=0)
                    end_date = (datetime.now()).replace(hour=0, minute=0, second=0, microsecond=0)
                    params = {"start_date":start_date,"end_date":end_date,"filterColumnName":filterColumnName,"data_date":start_date.date()}
                    self.filterAndProfiling(params)
            #     # TODO Write a logic to retrieve date from Airflow -daily run
            #     pass
            elif startDate != "None" and endDate != "None":
                if runType == "daily":
                    start_date = datetime.strptime(startDate, '%Y-%m-%d')
                    if len(endDate) == 10:
                        end_date = datetime.strptime(endDate, '%Y-%m-%d')
                    else:
                        end_date = datetime.strptime(endDate, '%Y-%m-%d')
                    while True:
                        # start_date = datetime.strptime(startDate, '%Y-%m')
                        next_date = start_date + relativedelta(days=1)
                        if str(next_date)[:10] == str(end_date)[0:10]:
                            next_date = end_date
                            params = {"start_date": start_date, "end_date": next_date, "filterColumnName": filterColumnName,"data_date": start_date.date()}
                            self.filterAndProfiling(params)
                            #call function before breaking
                            print(start_date,next_date,start_date.date())
                            break
                        # call function here
                        params = {"start_date": start_date, "end_date": next_date, "filterColumnName": filterColumnName, "data_date": start_date.date()}
                        self.filterAndProfiling(params)
                        print(start_date, next_date,start_date.date())
                        start_date = next_date
                if runType == "monthly":
                    start_date = datetime.strptime(startDate, '%Y-%m')
                    if len(endDate) == 7:
                        end_date = datetime.strptime(endDate, '%Y-%m')
                    else:
                        end_date = datetime.strptime(endDate, '%Y-%m-%d')
                    while True:
                        # start_date = datetime.strptime(startDate, '%Y-%m')
                        next_date = start_date + relativedelta(months=1)
                        if str(next_date)[:7] == str(end_date)[0:7]:
                            next_date = end_date
                            params = {"start_date": start_date, "end_date": next_date, "filterColumnName": filterColumnName,"data_date": start_date.date()}
                            self.filterAndProfiling(params)
                            #call function before breaking
                            print(start_date,next_date)
                            break
                        # call function here
                        params = {"start_date": start_date, "end_date": next_date, "filterColumnName": filterColumnName,"data_date": start_date.date()}
                        self.filterAndProfiling(params)
                        print(start_date, next_date)
                        start_date = next_date

                if runType == "yearly":
                    start_date = datetime.strptime(startDate, '%Y')
                    if len(endDate) == 4:
                        end_date = datetime.strptime(endDate, '%Y')
                    else:
                        end_date = datetime.strptime(endDate,'%Y-%m-%d')

                    while True:
                        next_date = start_date + relativedelta(years=1)
                        if str(next_date)[:4] == str(end_date)[0:4]:
                            print(start_date, next_date)
                            #call function here to cover 2021-01-01 to 2022-01-01
                            params = {"start_date": start_date, "end_date": next_date, "filterColumnName": filterColumnName,"data_date": start_date.date()}
                            self.filterAndProfiling(params)
                            start_date = next_date
                            next_date = end_date
                            params = {"start_date": start_date, "end_date": next_date, "filterColumnName": filterColumnName,"data_date": start_date.date()}
                            self.filterAndProfiling(params)
                            #call function here to cover 2022-01-01 to 2022-01-08
                            print(start_date,next_date)
                            break
                        params = {"start_date": start_date, "end_date": next_date, "filterColumnName": filterColumnName,"data_date": start_date.date()}
                        self.filterAndProfiling(params)
                        # call function here
                        print(start_date, next_date)
                        start_date = next_date
        else:
            """
            # Scan for whole table
            """
            params = {"start_date": startDate, "end_date": endDate, "filterColumnName": filterColumnName,
                      "data_date": date.today()}
            self.filterAndProfiling(params)
                # print(start_date, end_date)
        #     # TODO Write a logic to retrieve date from config for specific run
        #     pass


# -----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------

        #TODO Logic for check of data starts from here
        # checkBuilder = CheckBuilder()
        # checks = self.yaml_configs[0]['metrics_config'][1]['checks']
        # for check in checks:
        #     checkBuilder.getCheckBuild()

        # profileData = self.dataProfile.getDataProfiled(self.spark, filteredDataFrame, self.yaml_configs[0]['metrics_config'][0], self.scalaDictColMatMapping)

    #write_profile_matrics(profileData)
