from src.main.python.AthenaTableAsInput import AthenaTableAsInput
from src.main.python.RedshiftTableAsInput import RedshiftTableAsInput
from src.main.python.S3DataAsInput import S3DataAsInput
from src.main.python.TestDataAsInput import TestDataAsInput
from src.main.python.HanaAsInput import HanaAsInput

class InputSourceFactory():
    def __int__(self):
        pass

    def get_input_data_type(self, sourceType):
        """

        :param sourceType:
        :return:
        """
        if sourceType.upper() == 'ATHENA':
            return AthenaTableAsInput() #class object to return
        elif sourceType.upper() == 'REDSHIFT':
            return RedshiftTableAsInput()
        elif sourceType.upper() == 'LOCAL':
            return TestDataAsInput()
        elif sourceType.upper() == 'S3':
            return S3DataAsInput()
        elif sourceType.upper() == 'HANA':
            return HanaAsInput()
        else:
            raise ValueError(sourceType)