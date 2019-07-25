"
spark-sql --master yarn --conf spark.ui.port=12567
"
set hive.metastore.warehouse.dir;
dfs -ls /apps/hive/warehouse;

create table orders (
      order_id int,
      order_date string,
      order_customer_id int,
      order_status string
) row format delimited fields terminated by ','
stored as textfile;

load data local inpath '/data/retail_db/orders' into table orders;

create table order_items (
  order_item_id int,
  order_item_order_id int,
  order_item_product_id int,
  order_item_quality int,
  order_item_subtotal float,
  order_item_product_price float
  ) row format delimited fields terminated by ','
stored as textfile;

"
pyspark --master yarn --conf spark.ui.port=12562  --executor-memory 2G --num-executors 1
"

select substr("Hello World, How are you",14)
# begin with the exact idx
select substr("Hello World, How are you",7,5)
# like rlike upper lower trim ltrim rtrim lpad rpad initcap
select cast("12" as int)
select split("fjla jfsl dsjfl",' ')
select index(split("fjla jfsl dsjfl",' '),2)
select current_timestamp/current_date
select date_format(current_date,'y')
hive> select date_format(current_date,'M');
hive> select date_format(current_date,'d');
select day(current_date);
select day("2019-7-9")
select to_date("2019-7-9")
























