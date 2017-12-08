
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
                 minTime = None,
                 config = None,
                 addparams = "",
                 addnote = "",
                 solrHost = None):


        cTimeUTC =  datetime.utcnow()
        nTList = [str(cTimeUTC.date()),"T",str(cTimeUTC.hour),str(cTimeUTC.minute),str(cTimeUTC.second),"Z"]
        self.currentTime = "".join(nTList)

        self.config = AppConfig(config)
        self.mongoWrapper = MongoClientWrapper(self.config,
                                               mongoUser,
                                               mongoPassword)
        self.addnote = addnote

        if solrHost is None:
            self.solrURL = self.config.getConfig()["SOLR"]["host"]
        else:
            self.solrURL = solrHost

        if minTime is None:
            self.minimumLiveTime = 0
        else:
            self.minimumLiveTime = minTime

        self.timestampFile = open("solrperformance/timestamps.txt","a")
        self.timestampMongo = "testtime_" + self.currentTime

        self.timestampFile.write(self.timestampMongo + os.linesep)
        self.timestampFile.flush()
        self.timestampFile.close()
        self.additionalparams = addparams


        self.startTime = time.strftime('%H:%M:%S')
        self.totalQueryTime = 0
        self.totalNumberRequests = 0
        self.timeCollection = {}
        self.initializeCollecCurrentQueryTime()



    def getURL(self):
        return self.solrURL

    def getMongoTimeStamp(self):
        return self.timestampMongo

    def getAverTime(self):
        return self.totalQueryTime / self.totalNumberRequests

    def getMinTime(self):
        return self.minimumLiveTime


    def getStartTime(self):
        return self.startTime

    def getDistribution(self):
        distribution = []
        for k, v in self.timeCollection.items():
            distribution.append(" (" + k + "=>" + str(v) + ") ")

        return ''.join(distribution)






    def startRunning(self):



        if self.minimumLiveTime == 0:
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

                self.totalQueryTime += queryTime
                self.totalNumberRequests += 1
                self.collecCurrentQueryTime(queryTime)

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


    def collecCurrentQueryTime(self, queryTime):

        if queryTime >= 0 and queryTime <= 100:
            self.timeCollection['0_100'] += 1
        elif queryTime >= 101 and queryTime <= 200:
            self.timeCollection['100_200'] += 1
        elif queryTime >= 201 and queryTime <= 300:
            self.timeCollection['200_300'] += 1
        elif queryTime >= 301 and queryTime <= 400:
            self.timeCollection['300_400'] += 1
        elif queryTime >= 401 and queryTime <= 500:
            self.timeCollection['400_500'] += 1
        elif queryTime >= 501 and queryTime <= 600:
            self.timeCollection['500_600'] += 1
        elif queryTime >= 601 and queryTime <= 700:
            self.timeCollection['600_700'] += 1
        elif queryTime >= 701 and queryTime <= 800:
            self.timeCollection['700_800'] += 1
        elif queryTime >= 801 and queryTime <= 900:
            self.timeCollection['800_900'] += 1
        elif queryTime >= 901 and queryTime <= 1000:
            self.timeCollection['900_1000'] += 1
        elif queryTime >= 1001 and queryTime <= 1200:
            self.timeCollection['1000_1200'] += 1
        elif queryTime >= 1201 and queryTime <= 1400:
            self.timeCollection['1200_1400'] += 1
        elif queryTime >= 1401 and queryTime <= 1600:
            self.timeCollection['1400_1600'] += 1
        elif queryTime >= 1601 and queryTime <= 1800:
            self.timeCollection['1600_1800'] += 1
        elif queryTime >= 1801 and queryTime <= 2000:
            self.timeCollection['1800_2000'] += 1
        elif queryTime >= 2001 and queryTime <= 2400:
            self.timeCollection['2000_2400'] += 1
        elif queryTime >= 2401 and queryTime <= 2800:
            self.timeCollection['2400_2800'] += 1
        elif queryTime >= 2801 and queryTime <= 3200:
            self.timeCollection['2800_3200'] += 1
        elif queryTime >= 3201 and queryTime <= 3800:
            self.timeCollection['3200_3800'] += 1
        elif queryTime >= 3801 and queryTime <= 4400:
            self.timeCollection['3800_4400'] += 1
        elif queryTime >= 4401 and queryTime <= 5000:
            self.timeCollection['4400_5000'] += 1
        elif queryTime >= 5001 and queryTime <= 6000:
            self.timeCollection['5000_6000'] += 1
        elif queryTime >= 6001 and queryTime <= 7000:
            self.timeCollection['6000_7000'] += 1
        elif queryTime >= 7001 and queryTime <= 8000:
            self.timeCollection['7000_8000'] += 1
        elif queryTime >= 8001 and queryTime <= 9000:
            self.timeCollection['8000_9000'] += 1
        elif queryTime >= 9001 and queryTime <= 10000:
            self.timeCollection['9000_10000'] += 1
        elif queryTime >= 10001 and queryTime <= 15000:
            self.timeCollection['10000_15000'] += 1
        elif queryTime >= 15001 and queryTime <= 20000:
            self.timeCollection['15000_20000'] += 1
        elif queryTime >= 20001:
            self.timeCollection['gt20000'] += 1


    def initializeCollecCurrentQueryTime(self):

        self.timeCollection['0_100'] = 0
        self.timeCollection['100_200'] = 0
        self.timeCollection['200_300'] = 0
        self.timeCollection['300_400'] = 0
        self.timeCollection['400_500'] = 0
        self.timeCollection['500_600'] = 0
        self.timeCollection['600_700'] = 0
        self.timeCollection['700_800'] = 0
        self.timeCollection['800_900'] = 0
        self.timeCollection['900_1000'] = 0
        self.timeCollection['1000_1200'] = 0
        self.timeCollection['1200_1400'] = 0
        self.timeCollection['1400_1600'] = 0
        self.timeCollection['1600_1800'] = 0
        self.timeCollection['1800_2000'] = 0
        self.timeCollection['2000_2400'] = 0
        self.timeCollection['2400_2800'] = 0
        self.timeCollection['2800_3200'] = 0
        self.timeCollection['3200_3800'] = 0
        self.timeCollection['3800_4400'] = 0
        self.timeCollection['4400_5000'] = 0
        self.timeCollection['5000_6000'] = 0
        self.timeCollection['6000_7000'] = 0
        self.timeCollection['7000_8000'] = 0
        self.timeCollection['8000_9000'] = 0
        self.timeCollection['9000_10000'] = 0
        self.timeCollection['10000_15000'] = 0
        self.timeCollection['15000_20000'] = 0
        self.timeCollection['gt20000'] = 0



if __name__ == '__main__':

    parser = argparse.ArgumentParser()
    parser.add_argument('-u', '--user', help='user for Mongo DB', type=str, default=None)
    parser.add_argument('-p', '--password', help='password for Mongo DB', type=str, default=None)
    parser.add_argument('-m', '--minimalTime', help='time used to process live query should be $gt then minimaltime', type=str, default=None)
    parser.add_argument('-c', '--config', help='config file', type=str, default="config/files/solrperformance/performance.yaml")
    parser.add_argument('-a', '--addparam', help='query parameters which should be added to the productive query stored in mongo', type=str, default="")
    parser.add_argument('-n', '--note', help='additional note which should be written into the response to identify the character of the testcase', type=str, default="")
    parser.add_argument('-s', '--solrhost', help='Solr Host URL', type=str, default=None)


    parser.parse_args()
    args = parser.parse_args()

    runner = RunQueries(args.user,
                        args.password,
                        args.minimalTime,
                        args.config,
                        args.addparam,
                        args.note,
                        args.solrhost)

    try:
        runner.startRunning()
    except Exception as ex:
        print (ex)

    endtime = time.strftime('%H:%M:%S')
    summaryFile = open("solrperformance/summary.txt","a")

    stopTime = time.strftime('%H:%M:%S')

    summary = 'summary: startTime:{STARTTIME}   stopTime:{STOPTIME} solrurl:{URL}   timestamp:{TIMESTAMP}   averageTime:{AVERTIME}  mintime:{MINTIME}  distribution:{DISTRIBUTION}'.format(
        STARTTIME=runner.getStartTime(),
        STOPTIME=stopTime,
        URL=runner.getURL(),
        TIMESTAMP=runner.getMongoTimeStamp(),
        AVERTIME=runner.getAverTime(),
        MINTIME=runner.getMinTime(),
        DISTRIBUTION=runner.getDistribution()


    )

    summaryFile.write(summary + os.linesep + os.linesep)
    summaryFile.flush()
    summaryFile.close()





