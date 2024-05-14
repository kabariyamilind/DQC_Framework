from abc import ABC,abstractmethod


class InputSourceAbstract(ABC):

    @abstractmethod
    def readInputDataCreateSparkDF(self):
        pass