from pyspark.sql import *
spark = SparkSession.\
    builder.\
    master('local').\
    appName('CSV Example').\
    getOrCreate()

orders = spark.read.\
    format('csv').\
    schema('order_id int, order_date string, order_customer_id int,\
		 order_status string').\
    load('/Users/itversity/Research/data/retail_db/orders')

orderItems = spark.read.\
    format('csv').\
    schema(""" order_item_id int,
	order_item_order_id int,
	order_item_product_id int,
	order_item_quantity int,
	order_item_subtotal int,
	order_item_product_price float
	""").\
    load('/Users/itversity/Research/data/retail_db/orders_items')

orderItems.printSchema()
orderItems.show()

ordersCSV = spark.read.\
    csv('/Users/itversity/Research/data/retail_db/orders').\
    toDF('order_id int, order_date string, order_customer_id int,\
		 order_status string')


ordersItemsCSV = spark.read.\
    csv('/Users/itversity/Research/data/retail_db/orders_items').\
    toDF("""
    	order_item_id int,
	order_item_order_id int,
	order_item_product_id int,
	order_item_quantity int,
	order_item_subtotal int,
	order_item_product_price float
    	""")

from pyspark.type import IntegerType, FloatType
orders = ordersCSV.\
    withColulmn('order_id', orderCSV.order_id.cast(IntegerType))

orders.filter((orders.order_status == 'COMPLETE') |
              (orders.order_status == 'CLOSED'))

orders.filter(orders.order_status.isin('COMPLETE', 'CLOSED'))

orders.filter("orders.order_status in('COMPLETE', 'CLOSED')")

orders.filter((orders.order_status.isin('COMPLETE', 'CLOSED')).__and__
              (orders.order_date like('2013-08%'))).show()


from pyspark.sql.functions import round

orderItems.\
    select('order_item_subtotal', 'order_item_quantity', 'order_item_product_price').\
    where(orderItems.order_item_subtotal != round(orderItems.order_item_quantity *
                                                  orderItems.order_item_product_price, 2)).show()

from pyspark.sql.functions import countDistinct

order.select(countDistinct('order_status')).show()

orderItems.\
    filter('order_item_order_id'=2).\
    select(round(sum('order_item_subtotal'))).\
    show()

orderItems.\
    groupBy('order_items_order_id').\
    agg(round(sum('order_item_subtotal')).alias('order_revenue')).
    show()

orders.groupBy('order_status').\
    count().\
    show()
