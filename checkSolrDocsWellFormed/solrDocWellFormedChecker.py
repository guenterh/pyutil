
from ckeckIDOnSolr.idchecker import SolrWrapper, IDChecker, AppConfig
import gzip
from os import listdir,linesep
from os.path import isfile, join, isdir
import re
from lxml import etree
from string import Template


class SolrDocWellFormedChecker:
    pass

    def __init__(self, idChecker : IDChecker, config: AppConfig):
        self.config = config
        self.idChecker = idChecker

        self.parentPath = config.getConfig()["ParentPath"]
        self.outFileNotWellformed = open(config.getConfig()["outFileNotWellformed"],"a")
        self.outFileIdNotAvailable = open(config.getConfig()["outFileIdNotAvailable"],"a")
        self.recordRegex = re.compile(config.getConfig()["recordRegex"], re.UNICODE | re.DOTALL | re.IGNORECASE)
        self.idRegEx = re.compile(config.getConfig()["idRegex"], re.UNICODE | re.DOTALL | re.IGNORECASE)
        self.outline = Template("idnotavailable: $id / file: $file $linebreak")
        self.outlineNWF = Template("idnotwellformed: $id / file: $file $linebreak")



    def checkDocs(self):
        for dir in sorted(listdir(self.parentPath)):
            subobject = join(self.parentPath, dir)
            for file in listdir(subobject):
                try:
                    if isfile(join(subobject,file)):

                        tfile = gzip.open(join(subobject,file),"r")
                        contentSingleFile = tfile.read()
                        contentSingleFile = contentSingleFile.decode('utf-8')
                        tfile.close()
                        iterator = self.recordRegex.finditer(contentSingleFile)


                        for pMatchRecord in iterator:

                            record = pMatchRecord.group(1)
                            pId = self.idRegEx.search(record)

                            if pId:
                                id = pId.group(1)
                                if self.idChecker.solrWrapper.checkId(id):
                                    try:
                                        #etree.fromstring('<doc>sllsls</doc>')
                                        #check xml record structure being well formed
                                        etree.fromstring(record)
                                        #print('XML well formed, syntax ok.')


                                    except Exception as e:
                                        print(e)
                                        print(record)
                                        self.printId(id,file)

                                else:
                                    self.printId(id,file)



                except Exception as ex:
                    print(ex)


    def closeResources(self):
        if not self.outFileIdNotAvailable is None:
            self.outFileIdNotAvailable.close()
        if not self.outFileNotWellformed is None:
            self.outFileNotWellformed.close()

    def printId(self,currentId, filename):

        self.outFileIdNotAvailable.write(self.outline.substitute({"id":currentId, "file": filename, "linebreak": linesep}))
        self.outFileIdNotAvailable.flush()

    def printnotWellFormed(self,currentId, filename):

        self.outFileNotWellformed.write(self.outlineNWF.substitute({"id":currentId, "file": filename, "linebreak": linesep}))
        self.outFileNotWellformed.flush()







if __name__ == '__main__':

    config = AppConfig("config/files/idchecker/checker.yaml")

    checker = IDChecker(config)

    configWellFormed = AppConfig("config/files/idchecker/solrWellFormed.yaml")
    checkDocWellFormed = SolrDocWellFormedChecker(checker,configWellFormed)

    checkDocWellFormed.checkDocs()
    checker.closeResources()
    checkDocWellFormed.closeResources()
    print("work done")