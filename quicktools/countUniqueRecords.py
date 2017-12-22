
import gzip
from os import listdir,linesep, sep
from os.path import isfile, join, isdir
import re


number = 0

directory = '/swissbib_index/lsbPlatform/data/createFolderTemp'
pattern = re.compile('<controlfield tag="001">(.*?)</controlfield>')
idMap = {}
multipleEntries = {}

for f in listdir(directory):
    try:
        with open( directory + sep + f, "r") as handle:
            contentSingleFile = handle.read()
            #contentSingleFile = contentSingleFile.decode('utf-8')

            matched = pattern.search(contentSingleFile)
            if matched:
                id = matched.group(1)
                if id in idMap.keys():
                    idMap[id] += 1
                else:
                    idMap[id] = 1

    except Exception as exc:
        print(exc)
print ('number of documents in dictionary: ' + str(len(idMap)) + linesep + linesep)

for key, value in idMap.items():
    if value > 1:
        if value in multipleEntries.keys():
            multipleEntries[value] += 1
        else:
            multipleEntries[value] = 1

for key, value in multipleEntries.items():
    print("key / count " + str(key) + " " + str(value))
