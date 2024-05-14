from datetime import datetime, timedelta,date
class Commons():
    def __init__(self):
        pass

    def getStartDateAndEndDate(self, startDate, endDate):
        if 'CURRENT_DATE' in startDate.upper():
            startDate_minus_days = startDate.split("_")[-1]
            startDate = (datetime.now() - timedelta(int(startDate_minus_days))).replace(hour=0, minute=0, second=0, microsecond=0).strftime('%Y-%m-%d')

        if endDate.upper() =='NONE' or endDate.upper()=='CURRENT_DATE':
            endDate = (datetime.now()).replace(hour=0, minute=0, second=0, microsecond=0).strftime('%Y-%m-%d')
        elif 'CURRENT_DATE' in endDate.upper():
            endDate_minus_days = startDate.split("_")[-1]
            endDate = (datetime.now() - timedelta(int(endDate_minus_days))).replace(hour=0, minute=0, second=0, microsecond=0).strftime('%Y-%m-%d')

        return startDate,endDate

    def getDataDate(self, data_date):
        print(data_date)
        if 'CURRENT_DATE' in data_date.upper():
            data_date_minus_days = data_date.split("_")[-1]
            print(data_date_minus_days)
            data_date = (datetime.now() - timedelta(int(data_date_minus_days))).replace(hour=0, minute=0, second=0, microsecond=0).strftime('%Y-%m-%d')
        elif data_date=='CURRENT_DATE':
            data_date = (datetime.now()).replace(hour=0, minute=0, second=0, microsecond=0).strftime('%Y-%m-%d')
        return data_date