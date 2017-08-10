from config.appConfig import AppConfig
from pymongo import MongoClient
from bson.objectid import ObjectId
from bson import BSON


__author__ = 'swissbib - UB Basel, Switzerland, Guenter Hipler'
__copyright__ = "Copyright 2016, swissbib project"
__credits__ = []
__license__ = "??"
__version__ = "0.1"
__maintainer__ = "Guenter Hipler"
__email__ = "guenter.hipler@unibas.ch"
__status__ = "in development"
__description__ = """
    kafka producer to load data from Mongo Collections (Kappa - procedure)
"""





class MongoClientWrapper:
    def __init__(self, appConfig : AppConfig = None,
                 userArg = None,
                 passwordArg = None):
        self.config = appConfig.getConfig()

        if not self.config["MONGO"]["HOST"]["user"] is None and not self.config["MONGO"]["HOST"]["password"] is None:
            uri = 'mongodb://{USER}:{PASSWORD}@{SERVER}:{PORT}/{DB}'.format(
                USER=self.config['HOST']['user'],
                PASSWORD=self.config['HOST']['password'],
                SERVER=self.config['HOST']['server'],
                PORT=self.config['HOST']['port'],
                DB=self.config['HOST']['authDB']
            )
        elif not userArg is None and not passwordArg is None:
            uri = 'mongodb://{USER}:{PASSWORD}@{SERVER}:{PORT}/{DB}'.format(
                USER=userArg,
                PASSWORD=passwordArg,
                SERVER=self.config["MONGO"]['HOST']['server'],
                PORT=self.config["MONGO"]['HOST']['port'],
                DB=self.config["MONGO"]['HOST']['authDB'])

        else:

            uri = 'mongodb://{SERVER}:{PORT}'.format(
                SERVER=self.config["MONGO"]['HOST']['server'],
                PORT=self.config ["MONGO"]['HOST']['port'],
            )

        self.client = MongoClient( uri)
        self.database = self.client[self.config["MONGO"]['DB']['dbname']]
        self.queryCollection = self.database[self.config["MONGO"]['DB']['querycollection']]
        self.responseCollection = self.database[self.config["MONGO"]['DB']['responsecollection']]


    def getQueriesCollection(self):
        return self.queryCollection

    def getResponseCollection(self):
        return self.responseCollection

    def getResponseObject(self, docId):

        return  self.responseCollection.find_one({"_id":docId})

    def updateResponsObject(self, idToUpdate, updatePart):
        return self.responseCollection.update_one({"_id": idToUpdate}, {"$set":updatePart }, upsert=True)
        #return self.responseCollection.update_one({"_id": idToUpdate},doc,upsert=True)



    def insertReponseObject(self, doc):
        self.responseCollection.insert_one(doc)


    def closeConnection(self):
        if not self.client is None:
            self.client.close()


