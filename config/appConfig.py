# coding: utf-8


__author__ = 'swissbib - UB Basel, Switzerland, Guenter Hipler'
__copyright__ = "Copyright 2016, swissbib project"
__credits__ = []
__license__ = "??"
__version__ = "0.1"
__maintainer__ = "Guenter Hipler"
__email__ = "guenter.hipler@unibas.ch"
__status__ = "in development"
__description__ = """

                    """

import yaml
import re
import copy

class AppConfig:
    def __init__(self,configpath):
        self.configPath = configpath
        self.loadConfig()
        self.detailedGranularityPattern = re.compile('Thh:mm:ssZ', re.UNICODE | re.DOTALL | re.IGNORECASE)



    def loadConfig(self):

        try:
            configHandle = open(self.configPath, 'r')
            self.config = yaml.load (configHandle)
            configHandle.close()
        except Exception:
            print("error trying to read config File")
            raise Exception

    def getConfig(self):
        return self.config




    def writeConfig(self):
        configHandle = open(self.configPath, 'w')
        yaml.dump(self.config, configHandle,default_flow_style=False)
        configHandle.close()


    def getProcessor(self):
        return self.config['Processing']['processorType']





class MongoConfig(AppConfig):
    def __init__(self,configpath):
        super().__init__(configpath=configpath)

