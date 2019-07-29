"""
pyspark --master yarn \
--conf spark.ui.port=12789 \
--num-executors 6 \
--executor-cores 2 \
--executor-memory 3G
"""

import os
path = '/data/nyse'
files = os.listdir(path)
for f in files:
	        temp_path = os.path.join(path, f)
        with open(temp_path) as file:
            rawfile = file.readlines()
            rawfile = [x[:-2].split(',') for x in rawfile]
            rawRDD = sc.parallelize(rawfile)
            allRDDDF = rawRDD.toDF(schema=['stockticker', 'transactiondate',
                                           'openprice', 'highprice', 'lowprice', 'closeprice', 'volume']).coalesce(1)
	break
for f in files[1:]:
    if '.txt' in f:
        temp_path = os.path.join(path, f)
        with open(temp_path) as file:
            rawfile = file.readlines()
            rawfile = [x[:-2].split(',') for x in rawfile]
            rawRDD = sc.parallelize(rawfile)
            rawRDDDF = rawRDD.toDF(schema=['stockticker', 'transactiondate',
                                           'openprice', 'highprice', 'lowprice', 'closeprice', 'volume']).coalesce(1)
            allRDDDF.unionAll(rawRDDDF)
allRDDDF.write.format('parquet').save('/user/xinzhouyan/nyse_parquet')
