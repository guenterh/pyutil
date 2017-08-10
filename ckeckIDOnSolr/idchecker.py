
from config.appConfig import AppConfig
import gzip
from os import listdir,linesep
from os.path import isfile, join, isdir
import re
from searchSolr.SolrWrapper import SolrWrapper
from string import Template



class IDChecker:
    def __init__(self, config : AppConfig):
        self.config = config

        self.parentPath = config.getConfig()["ParentPath"]
        #self.solrURL = config.getConfig()["SolrURL"]
        self.regex = re.compile(config.getConfig()["idregex"],re.UNICODE | re.DOTALL | re.IGNORECASE)
        self.solrWrapper = SolrWrapper(config)
        self.outFile = open(config.getConfig()["outFile"],"a")
        self.outline = Template("idnotavailable: $id / file: $file $linebreak")



    def checkIDs(self):

        for dir in sorted(listdir(self.parentPath)):
            subobject = join(self.parentPath, dir)
            #if isdir(subobject):
            #for f in listdir(subobject):
            #assumedFileObject = join(subobject,f)
            try:
                if isfile(subobject):
                    #print(subobject)
                    with gzip.open(subobject,"r") as f:
                        for line in f:
                            strLine = line.decode('utf-8')
                            pId = self.regex.search(strLine)
                            if pId:

                                id = pId.group(1)
                                #self.printId(id,subobject)
                                if not self.solrWrapper.checkId(id):
                                    self.printId(id,subobject)
                            #print(strLine)
            except Exception as ex:
                print(ex)

    def printId(self,currentId, filename):

        self.outFile.write(self.outline.substitute({"id":currentId, "file": filename, "linebreak": linesep}))
        self.outFile.flush()


    def closeResources(self):
        if not self.outFile is None:
            self.outFile.close()



if __name__ == '__main__':

    config = AppConfig("config/files/idchecker/checker.yaml")

    checker = IDChecker(config)
    checker.checkIDs()
    checker.closeResources()
    print("work done")