"""
pyspark --master yarn \
--conf spark.ui.port=12789 \
--num-executors 6 \
--executor-cores 2 \
--executor-memory 3G
"""
from pyspark.sql.types import StructType
from pyspark.sql.types import StructField
from pyspark.sql.types import StringType

crimeData = sc.textFile("/public/crime/csv")
for row in crimeData.take(5):
    print(row)

crimeDataHead = crimeData.first()
crimeDataNoHead = crimeData.filter(lambda row: row != crimeDataHead)
crimeDataNoHeadMap = crimeDataNoHead.map(lambda row: row.split(','))

"""
'ID,Case Number,Date,Block,IUCR,Primary Type,
Description,Location Description,Arrest,Domestic,Beat,District,
Ward,Community Area,FBI Code,X Coordinate,Y Coordinate,Year,Updated On,
Latitude,Longitude,Location'
"""
schema = StructType([StructField(col, StringType(), True)
                     for col in crimeDataHead.split(',')])
from spark.sql.functions import *
data = crimeDataNoHeadMap.\
    map(lambda row: ((int(row[2].split()[0][-4:] + row[2].split()[0][0:2]), row[5]), 1))
# take(2)
from operator import *
countByKey = data.reduceByKey(add)
# df = sqlContext.createDataFrame(crimeDataNoHeadMap, schema)
MonthValueKey = countByKey.map(lambda row:
                               ((row[0][0], -row[1]), (str(row[0][0]) + '\t' + str(row[1]) + '\t' + row[0][1])))\
    .sortByKey()

monthlyCrimeCountedByTypeSorted = MonthValueKey.map(lambda row: row[1])

monthlyCrimeCountedByTypeSorted.\
    saveAsTextFile("/user/xinzhouyan/solutions/sulution01/crimes_by_type_by_month",
                   compressionCodecClass='org.apache.hadoop.io.compress.GzipCodec')
