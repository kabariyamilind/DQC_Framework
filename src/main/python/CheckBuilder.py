from src.main.python.MetricsChecks import MetricsChecks


class CheckBuilder():

    def __init__(self,env):
        self.env =env
        self.metricsChecks = MetricsChecks(self.env)
    def getChecksBuild(self, check, param, inputDF):
        chekstypes = param.keys()
        count_of_input_data = inputDF.count()
        for type in chekstypes:
            if type.upper() == "ISCOMPLETE":
                columnsForIsCompelete = param[type]
                for column in columnsForIsCompelete:
                    check = check.isComplete(column)
        for type in chekstypes:
            if type.upper() == "ISUNIQUE":
                columnsForIsUnique = param[type]
                for column in columnsForIsUnique:
                    check = check.isUnique(column)

        for type in chekstypes:
            if type.upper() == "HASSUM":
                columnsForHasSum = param[type]
                dict_condition  = columnsForHasSum[0]
                for column in dict_condition.keys():
                    condition_and_value = dict_condition[column]
                    print(condition_and_value)
                    condition = list(condition_and_value.keys())[0]
                    print(condition)
                    if condition[0] == ">":
                        check = check.hasSum(column, lambda x: x > condition_and_value[condition])
                    elif condition[0] == "<":
                        check = check.hasSum(column, lambda x: x < condition_and_value[condition])
                    elif condition[0] == ">=":
                        check = check.hasSum(column, lambda x: x >= condition_and_value[condition])
                    elif condition[0] == "<=":
                        check = check.hasSum(column, lambda x: x <= condition_and_value[condition])

        for type in chekstypes:
            if type.upper() == "HASCOMPLETENESS":
                columnsForHasCompleteness = param[type]
                dict_condition  = columnsForHasCompleteness[0]
                for column in dict_condition.keys():
                    condition_and_value = dict_condition[column]
                    print(condition_and_value)
                    condition = list(condition_and_value.keys())[0]
                    print(condition)
                    if condition[0] == ">":
                        check = check.hasCompleteness(column, lambda x: x > condition_and_value[condition])
                    elif condition[0] == "<":
                        check = check.hasCompleteness(column, lambda x: x < condition_and_value[condition])
                    elif condition[0] == ">=":
                        check = check.hasCompleteness(column, lambda x: x >= condition_and_value[condition])
                    elif condition[0] == "<=":
                        check = check.hasCompleteness(column, lambda x: x <= condition_and_value[condition])

        for type in chekstypes:
            if type.upper() == "HASSIZE":
                columnsForHasSize = param[type]
                dict_condition = columnsForHasSize[0]
                for column in dict_condition.keys():
                    condition_and_value = dict_condition[column]
                    condition = list(condition_and_value.keys())[0]
                    print(condition_and_value)
                    if condition[0] == ">":
                        check = check.hasSize(lambda x : x > condition_and_value[condition])
                    elif condition[0] == "<":
                        check = check.hasSize(lambda x: x < condition_and_value[condition])
                    elif condition[0] == ">=":
                        check = check.hasSize(lambda x: x >= condition_and_value[condition])
                    elif condition[0] == "<=":
                        check = check.hasSize(lambda x: x <= condition_and_value[condition])

        for type in chekstypes:
            if type.upper() == "HASAPPROXCOUNTDISTINCT":
                columnsForHasApproxCountDistinct = param[type]
                dict_condition  = columnsForHasApproxCountDistinct[0]
                for column in dict_condition.keys():
                    condition_and_value = dict_condition[column]
                    print(condition_and_value)
                    condition = list(condition_and_value.keys())[0]
                    print(condition)
                    if condition[0] == ">":
                        check = check.hasApproxCountDistinct(column, lambda x: x > condition_and_value[condition])
                    elif condition[0] == "<":
                        check = check.hasApproxCountDistinct(column, lambda x: x < condition_and_value[condition])
                    elif condition[0] == ">=":
                        check = check.hasApproxCountDistinct(column, lambda x: x >= condition_and_value[condition])
                    elif condition[0] == "<=":
                        check = check.hasApproxCountDistinct(column, lambda x: x <= condition_and_value[condition])

        for type in chekstypes:
            if type.upper() == "SATISFIESCOUNT":
                columnsForSatisfiesCount = param[type]
                dict_condition  = columnsForSatisfiesCount[0]
                for criteria_to_check in dict_condition.keys():
                    condition_and_value = dict_condition[criteria_to_check]
                    print(condition_and_value)
                    condition = list(condition_and_value.keys())[0]
                    description = list(condition_and_value.keys())[1]
                    print(description)
                    print(condition)
                    print(count_of_input_data,condition_and_value[condition]*count_of_input_data)
                    if condition[0] == ">":
                        check = check.satisfies(criteria_to_check,condition_and_value[description],lambda x: x*count_of_input_data > condition_and_value[condition])
                    elif condition[0] == "<":
                        check = check.satisfies(criteria_to_check,condition_and_value[description],lambda x: x*count_of_input_data < condition_and_value[condition])
                    elif condition[0] == ">=":
                        check = check.satisfies(criteria_to_check, condition_and_value[description],lambda x: x*count_of_input_data >= condition_and_value[condition])
                    elif condition[0] == "<=":
                        check = check.satisfies(criteria_to_check, condition_and_value[description],lambda x: x*count_of_input_data <= condition_and_value[condition])

        for type in chekstypes:
            if type.upper() == "SATISFIES":
                columnsForSatisfies = param[type]
                dict_condition  = columnsForSatisfies[0]
                for criteria_to_check in dict_condition.keys():
                    condition_and_value = dict_condition[criteria_to_check]
                    print(condition_and_value)
                    condition = list(condition_and_value.keys())[0]
                    description = list(condition_and_value.keys())[1]
                    print(description)
                    print(condition)
                    if condition[0] == ">":
                        check = check.satisfies(criteria_to_check,condition_and_value[description],lambda x: x > condition_and_value[condition])
                    elif condition[0] == "<":
                        check = check.satisfies(criteria_to_check,condition_and_value[description],lambda x: x < condition_and_value[condition])
                    elif condition[0] == ">=":
                        check = check.satisfies(criteria_to_check, condition_and_value[description],lambda x: x >= condition_and_value[condition])
                    elif condition[0] == "<=":
                        check = check.satisfies(criteria_to_check, condition_and_value[description],lambda x: x <= condition_and_value[condition])
        print(check)
        return check

    def getMetricsChecked(self,spark, param, inputDF,metricsDF,data_date,check_name):
        print(param)
        chekstypes = param.keys()
        metrics_to_check = []
        for type in chekstypes:
            if type.upper() == "CHECKMETRICS":
                metrics_to_check = param[type]

        # if metrics_to_check ==[]:
        #     return
        # else:
        return self.metricsChecks.getAllMetricsCheckedAndCreateFinalDataframe(spark,metrics_to_check,metricsDF,data_date,check_name)