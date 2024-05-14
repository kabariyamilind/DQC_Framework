from src.main.utils.SendAlert import SendAlert


class ConstraintFailureDataIndetifier():

    def __init__(self,env):
        self.sendAlert = SendAlert()
        self.env = env
        pass

    def alertForConstraintFailure(self, spark, check, check_level, check_status, constraint, constraint_status,
                                  constraint_message,paramsForDataReadForFailure=[]):
        if self.env.upper() in ['DEV','PROD']:
            data_path_root = "s3://dqc-metadata/DQC_input_data/"+self.env
            mismatch_data_path = "s3://dqc-metadata/DQC_mismatch_data/"+self.env
        else:
            data_path_root = 'C:/Users/mkabariya/OneDrive - Tata CLiQ/Desktop/Test_Location_Store_1/DQC_input_data/'+self.env
            mismatch_data_path = "C:/Users/mkabariya/OneDrive - Tata CLiQ/Desktop/Test_Location_Store_1/DQC_mismatch_data/"+self.env
        if 'IsCompareConstraint' in constraint:
            firstTable = paramsForDataReadForFailure[0]
            secondTable = paramsForDataReadForFailure[1]
            profilename_firstValParams = paramsForDataReadForFailure[2]
            profilename_secondValParams = paramsForDataReadForFailure[3]
            data_date = paramsForDataReadForFailure[4]
            readDataForFirstTable = spark.read.parquet(data_path_root+"/"+firstTable["INPUTSOURCE"]+"/"+firstTable["TABLENAME"]+"/"+profilename_firstValParams+"/"+str(data_date)+"/")
            readDataForSecondTable = spark.read.parquet(data_path_root+"/"+secondTable["INPUTSOURCE"]+"/"+secondTable["TABLENAME"]+"/"+profilename_secondValParams+"/"+str(data_date)+"/")
            compare_sign = paramsForDataReadForFailure[5]
            first_val_value = paramsForDataReadForFailure[6]
            second_val_value = paramsForDataReadForFailure[7]

            if compare_sign == "=":
                firstID = readDataForFirstTable.select(firstTable['INSTANCE'])
                seconfID = readDataForSecondTable.select(secondTable['INSTANCE'])
                firstID.printSchema()
                seconfID.printSchema()
                firstID.show(10)
                seconfID.show(10)
                difference = firstID.unionAll(seconfID).dropDuplicates().exceptAll(firstID.intersect(seconfID)).dropDuplicates()
                path = mismatch_data_path + '/' + 'mismatch' + '/' + firstTable["TABLENAME"] + '/' + secondTable[
                    "TABLENAME"] + '/' + firstTable['INSTANCE'] + '/' + secondTable[
                           'INSTANCE'] + '/' + data_date + '/'
                if first_val_value>second_val_value:
                    difference.coalesce(1).write.mode("overwrite").csv(path)
                    message = 'Mismatch in count between column: ' + firstTable['INSTANCE']+' in table: ' +\
                              firstTable["TABLENAME"] + ' and column: ' +\
                              secondTable['INSTANCE'] +' in table: ' + secondTable["TABLENAME"] +'.'+ firstTable["TABLENAME"] +\
                              ' in ' +firstTable["INPUTSOURCE"]+' has high number of records then ' + secondTable["TABLENAME"] +\
                              ' in ' +secondTable["INPUTSOURCE"] +'.' +' Data available at path: '+ path+'.'+'data_date: '+ data_date
                    subject = 'Data Quality Check Failure Alert'
                    self.sendAlert.sendAlertOnSNS(subject, message)
                if first_val_value < second_val_value:
                    difference.coalesce(1).write.mode("overwrite").csv(path)
                    message = 'Mismatch in count between column: ' + secondTable['INSTANCE'] + ' in table: ' + \
                              secondTable["TABLENAME"] + ' and column: ' + \
                              firstTable['INSTANCE'] + ' in table: ' + firstTable["TABLENAME"] + '.' + secondTable[
                                  "TABLENAME"] + \
                              ' in ' + secondTable["INPUTSOURCE"] + ' has high number of records then ' + firstTable[
                                  "TABLENAME"] + \
                              ' in ' + firstTable["INPUTSOURCE"] + '.'+' Data available at path: '+ path +'.'+'data_date: '+ data_date
                    subject = 'Data Quality Check Failure Alert'
                    self.sendAlert.sendAlertOnSNS(subject,message)

        if 'UniquenessConstraint' in constraint:
            subject = 'Data Quality Check Failure Alert'
            message ='test'
            self.sendAlert.sendAlertOnSNS(subject, message)

