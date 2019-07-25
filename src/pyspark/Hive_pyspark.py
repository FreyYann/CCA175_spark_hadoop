"pyspark --master yarn --conf spark.ui.port=12562\
  --executor-memory 2G --num-executors 1"


"""
SparkContext available as sc, HiveContext available as sqlContext
"""
sqlContext.sql("show databases")
sqlContext.sql("use hornay_indian")
sqlContext.sql("show tables")
sqlContext.sql("describe formatted orders")
sqlContext.sql("select * from orders limit 10")
sqlContext.sql("select length('hello world')")
