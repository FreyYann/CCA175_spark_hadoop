"""
pyspark --master yarn \
--conf spark.ui.port=12789 \
--num-executors 6 \
--executor-cores 2 \
--executor-memory 3G
"""

crimeData = sc.textFile('/public/crime/csv')
crimehead = crimeData.first()  # .split(',')
crimeDataNoHead = crimeData.filter(lambda row: row != crimehead)
crimeDataNoHeadMap = crimeDataNoHead.map(lambda row: row.split(','))

oldColumns = crimeData.columns
newColumns = 'ID,Case Number,Date,Block,IUCR,Primary Type,\
Description,Location Description,Arrest,Domestic,Beat,District,\
Ward,Community Area,FBI Code,X Coordinate,Y Coordinate,Year,Updated On,\
Latitude,Longitude,Location'.split(',')

crimeDataLocationType = crimeDataNoHeadMap.\
    map(lambda row: ((row[5], row[7]), 1))

from operator import add
crimeDataLocationTypeCount = crimeDataLocationType.\
    reduceByKey(add)

crimeDataLocationTypeCountSorted = crimeDataLocationTypeCount.\
    map(lambda row: (-row[1], row[0])).\
    sortByKey().\
    map(lambda row: (row[1], -row[0]))

crimeTop3Df = crimeDataLocationTypeCountSorted.\
    toDF(schema=['crime_type_area', 'count']).coalesce(1)

crimeTop3Df.write.\
    json("/user/xinzhouyan/solutions1/solution03/RESIDENCE_AREA_CRIMINAL_TYPE_DATA")
