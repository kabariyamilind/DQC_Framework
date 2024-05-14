
class DataFilter:
    def __init__(self):
        pass

    def getDataFiltered(self, spark, inputDF, startDate, endDate, paramsToFilterData):
        inputDF.printSchema()
        filteredData = inputDF \
            .filter(inputDF[paramsToFilterData['filter_column_name']] >= startDate)\
            .filter(inputDF[paramsToFilterData['filter_column_name']] < endDate)

        return filteredData
