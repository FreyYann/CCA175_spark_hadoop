"""
pyspark --master yarn \
  --conf spark.ui.port=12890 \
  --num-executors 2 \
  --executor-memory 512M \
  --packages com.databricks:spark-avro_2.10:2.0.1

"""

from pyspark import SparktConf, SparkContext
conf = SparkConf().\
    setAppName('Daily Ravenue Per Product').\
    setMaster('yarn-client')
sc = SparkContext(conf=conf)

# `filter all order is closed/complete
orders = sc.textFile("/public/retail_db/orders")
orderItems = sc.textFile("/public/retail_db/order_items")

ordersFiltered = orders. \
    filter(lambda o: o.split(",")[3] in ["COMPLETE", "CLOSED"])

# check condiction
orders.map(lambda o: o.split(',')[3]).distinct().collect()

orderMap = orders.\
    map(lambda o: (int(o.split(',')[0]), o.split(',')[1]))

orderItemsMap = orderItems.\
    map(lambda oi: (int(oi.split(',')[1]),
                    (int(oi.split(',')[2]), float(oi.split(',')[4]))))

orderJoin = orderMap.join(orderItemsMap)

orderJoinMap = orderJoin.\
    map(lambda o: ((o[1][0], o[1][1][0]), o[1][1][1]))

from operator import add
dailyRevenuePerProductID = orderJoinMap.reduceByKey(add)

dailyRevenuePerProductIDMap = dailyRevernuePerProductID.\
    map(lambda r: (r[0][1], (r[0][0], r[1])))

productsRaw = open("/data/retail_db/products/part-00000"). \
    read(). \
    splitlines()
products = sc.parallelize(productsRaw)

productsMap = products. \
    map(lambda p: (int(p.split(",")[0]), p.split(",")[2]))
dailyRevenuePerProductIDMap = dailyRevenuePerProductID. \
    map(lambda r: (r[0][1], (r[0][0], r[1])))

dailyRevernuePerProductJoin = dailyRevernuePerProductIDMap.join(productsMap)
dailyRevernuePerProduct = dailyRevernuePerProductJoin.\
    map(lambda t:
        ((t[1][0][0], -t[1][0][1]), t[1][0][0] + "," + str(t[1][0][1]) + ',' +
         t[1][1])
        )
dailyRevernuePerProductSorted = dailyRevernuePerProduct.sortByKey()
dailyRevernuePerProductName = dailyRevernuePerProductSorted.\
    map(lambda r: r[1])
# save to the HDFS
dailyRevernuePerProductName.saveAsTextFile('/user/xinzhouyan/daily_revenue')
# save two split
dailyRevernuePerProductName.\
    coalesce(2).\
    saveAsTextFile('/user/xinzhouyan/daily_revenue')
