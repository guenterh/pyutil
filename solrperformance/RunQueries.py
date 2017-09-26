
from mongowrapper.mongo import MongoClientWrapper
from config.appConfig import MongoConfig, AppConfig
import requests
import json
from io import StringIO
import time
from datetime import datetime, timedelta
import argparse
import os


class RunQueries():
    def __init__(self,
                 mongoUser = None,
                 mongoPassword = None,
                 time = None,
                 config = None,
                 addparams = "",
                 addnote = ""):


        cTimeUTC =  datetime.utcnow()
        nTList = [str(cTimeUTC.date()),"T",str(cTimeUTC.hour),str(cTimeUTC.minute),str(cTimeUTC.second),"Z"]
        self.currentTime = "".join(nTList)

        self.config = AppConfig(config)
        self.mongoWrapper = MongoClientWrapper(self.config,
                                               mongoUser,
                                               mongoPassword)
        self.addnote = addnote

        self.solrURL = self.config.getConfig()["SOLR"]["host"]

        self.minimumLiveTime = time

        self.timestampFile = open("timestamps.txt","a")

        self.timestampFile.write("testtime_" + self.currentTime + os.linesep)
        self.timestampFile.flush()
        self.timestampFile.close()
        self.additionalparams = addparams



    def startRunning(self):


        if self.minimumLiveTime is None:
            filter = {}
        else:
            filter = {"time": {"$gt": (int)(self.minimumLiveTime)}}

        for doc in self.mongoWrapper.getQueriesCollection().find(filter):
            try:
                query = doc["query"]
                if not self.additionalparams == "":
                    query += "&" + self.additionalparams
                docId = doc["_id"]
                result = requests.get(self.solrURL,params=query.encode("utf-8"))
                io = StringIO(result.text)
                myJson = json.load(io)
                queryTime = myJson["responseHeader"]["QTime"]
                numberHits = myJson["response"]["numFound"]

                responseObject = self.mongoWrapper.getResponseObject(docId)

                if responseObject is None:
                    responseObject = {
                        "_id": docId,
                        "query": query,
                        "liveSystemTime" : doc["time"],
                        "liveHits": doc["hits"],
                        "testtime_" + self.currentTime: (int) (queryTime),
                        "testhits_" + self.currentTime: (int) (numberHits)
                    }
                    if not self.addnote == "":
                        responseObject["note_" + self.currentTime] = self.addnote
                    self.mongoWrapper.insertReponseObject(responseObject)
                else:
                    updatePart = {
                        "testtime_" + self.currentTime: (int)(queryTime),
                        "testhits_" + self.currentTime: (int) (numberHits)
                    }
                    if not self.addnote == "":
                        updatePart["note_" + self.currentTime] = self.addnote

                    #responseObject["testtime_" + self.currentTime] = (int) (queryTime)
                    #responseObject["testhits_" + self.currentTime] = (int)(numberHits)
                    response = self.mongoWrapper.updateResponsObject(docId,updatePart)


                #self.mongoWrapper.getQueriesCollection().save(doc)
                #self.mongoWrapper.getCollection().safe(doc,safe=True)


            except Exception as ex:
                print (ex)




if __name__ == '__main__':

    parser = argparse.ArgumentParser()
    parser.add_argument('-u', '--user', help='user for Mongo DB', type=str, default=None)
    parser.add_argument('-p', '--password', help='password for Mongo DB', type=str, default=None)
    parser.add_argument('-m', '--minimalTime', help='time used to process live query should be $gt then minimaltime', type=str, default=None)
    parser.add_argument('-c', '--config', help='config file', type=str, default="config/files/solrperformance/performance.yaml")
    parser.add_argument('-a', '--addparam', help='query parameters which should be added to the productive query stored in mongo', type=str, default="")
    parser.add_argument('-n', '--note', help='additional note which should be written into the response to identify the character of the testcase', type=str, default="")


    parser.parse_args()
    args = parser.parse_args()

    runner = RunQueries(args.user,
                        args.password,
                        args.minimalTime,
                        args.config,
                        args.addparam,
                        args.note)
    runner.startRunning()




