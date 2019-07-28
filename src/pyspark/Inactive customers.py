from pyspark.sql import *

spark = SparkSession.\
    builder.\
    master('local').\
    appName('CSV Example').\
    getOrCreate()

orders = spark.read.format("csv").\
    option("header", "false").\
    load("/data/retail_db/orders/part-00000")

customers = spark.read.format("csv").\
    option("header", "false").\
    load("/data/retail_db/customers/part-00000")

oldColumns = orders.columns
newColumns = 'order_id, order_date, order_customer_id, order_status'\
    .split(',')
newColumns = [x.strip() for x in newColumns]
orders = reduce(lambda data, idx:
                data.withColumnRenamed(oldColumns[idx],
                                       newColumns[idx]), xrange(len(oldColumns)), orders)
oldColumns = customers.columns
newColumns = ['customer_id', 'customer_fname', 'customer_lname',
              'SSN', 'customer_NO', 'customer_address', 'customer_city', 'customer_state',
              'customer_zip_code']
customers = reduce(lambda data, idx:
                   data.withColumnRenamed(oldColumns[idx],
                                          newColumns[idx]), xrange(len(oldColumns)), customers)

customersOrder = customers.\
    join(orders, orders.order_customer_id == customers.customer_id, 'left').\
    select(customers.customer_lname, customers.customer_fname, orders.order_customer_id).\
    where(orders.order_customer_id.isNull()).\
    orderBy(customers.customer_lname, customers.customer_fname)

customersOrder.\
    write.format('com.databricks.spark.csv').\
    save('/home/xinzhouyan/mycsv.csv')
