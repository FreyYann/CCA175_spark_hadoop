"""
pyspark --master yarn \
--conf spark.ui.port=12789 \
--num-executors 6 \
--executor-cores 2 \
--executor-memory 3G
"""
import re
from operator import add
rawtext = sc.textFile('/public/randomtextwriter')
rawtextRDD = rawtext.flatMap(lambda row: re.split("r[^\w]|.|[ ]+", row))
rawtextRDDNotNull = rawtextRDD.filter(lambda word: len(word) > 0)
rawtextRDDNotNullNoByte = rawtextRDDNotNull.\
    filter(lambda word: re.search('[0-9]', word))
rawtextRDDNotNullNoByteMap = rawtextRDDNotNullNoByte.\
    map(lambda word: (word, 1))
rawtextRDDNotNullNoByteMapCount = rawtextRDDNotNullNoByteMap.\
    reduceByKey(add)
rawtextRDDNotNullNoByteMapCountSorted = rawtextRDDNotNullNoByteMapCount.\
    # map(lambda row: (row[0], -row[1])).\
    sortByValue(False)
rawtextRDDNotNullNoByteMapCountSorted.\
    saveAsTextFile('/user/xinzhouyan/solutions/solution05/wordcount')
