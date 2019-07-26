from spark.sql.functions import sum
from spark.sql.functions import round
from spark.sql.window import *

employee.select('employee_id', 'department_id', 'salary').\
    groupBy('department_id').\
    sum('salary').\
    show()

spec = Window.partitionBy(orderItems.order_item_order_id)

orderItems.\
    withColumn('order_revenue', round(sum('order_item_subtotal').over(spec), 2)).\
    select('order_itme_id', 'order_item_order_id', 'order_item_subtotal', 'order_revenue').\
    show()

dailyProductRevenue = orders.\
    where(orders.order_status.isin('COMPLETED', 'CLOSED')).\
    join(orderItems, orders.order_id == orderItems.order_item_order_id).\
    groupBy('order_data', 'order_item_product_id').\
    .agg(round(sum(orderItems.order_item_subtotal), 2)).\
    alias('revenue').\
    show()

employee.\
    select('employee_id', 'department_id', 'salary').\
    orderBy('department_id', 'salary').\
    show()

spect = Window.partitionBy('department_id').\
    orderBy(employee.salary)

employee.\
    select('employee_id', 'department_id', 'salary').\
    withColumn('rank', rank().over(spec))  # jumpy
    withColumn('dense_rank', dense_rank().over(spec))  # no jump
    withColumn('row_num', row_number().over(spec))
    orderBy('department_id', 'salary').\
        show()
