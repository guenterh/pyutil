

import gzip
import re


if __name__ == '__main__':

    recordRegex = re.compile("(<doc>.*?</doc>)", re.UNICODE | re.DOTALL | re.IGNORECASE)
    tfile = gzip.open("solrout000151.xml.gz", "r")

    contentSingleFile = tfile.read()
    contentSingleFile = contentSingleFile.decode('utf-8')
    tfile.close()
    iterator = recordRegex.finditer(contentSingleFile)

    i = 0
    searchDoc = re.compile("<doc>", re.UNICODE | re.DOTALL | re.IGNORECASE)
    for pMatchRecord in iterator:
        i += 1
        record = pMatchRecord.group(1)
        #pId = self.idRegEx.search(record)
        #ergebnis = record.find("<doc>")
        #pm =  searchDoc.search(record)
        anzahl = record.count("<doc>")
        if anzahl > 1:
            print (record)
        #if pm:
        #    print(record)
        #print (ergebnis)

    #print (i)