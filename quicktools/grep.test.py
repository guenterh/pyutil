
import gzip
from os import listdir,linesep
from os.path import isfile, join, isdir
import re


number = 0
for dir in sorted(listdir("all")):
    subobject = join("all", dir)
    # if isdir(subobject):
    # for f in listdir(subobject):
    # assumedFileObject = join(subobject,f)
    try:
        # print(subobject)
        for f in sorted(listdir(subobject)):
            file = join(subobject, f)
            print(file)
            with gzip.open(file, "r") as handle:
                contentSingleFile = handle.read()
                contentSingleFile = contentSingleFile.decode('utf-8')

                # txt = open("ffixDocCheck.py").read()
                # print (len(txt))
                rexp = re.compile(r"<record>")
                match = rexp.findall(contentSingleFile)
                numberrecs = len(match)
                print(numberrecs)
                number += numberrecs
                # print(strLine)
    except Exception as ex:
        print(ex)

print (number)