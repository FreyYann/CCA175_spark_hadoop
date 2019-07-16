# Use retail_db data set

## Problem Statement
- Get daily revenue by product considering completed and closed orders.
- Data need to be sorted in ascending order by date and then descending
- order by revenue computed for each product for each day.
- Data for orders and order_items is available in HDFS  
*/public/retail_db/orders and /public/retail_db/order_items*

## Data for products is available locally under 
*/data/retail_db/products*

## Final output need to be stored under
- HDFS location – avro format
*/user/YOUR_USER_ID/daily_revenue_avro_python*
- HDFS location – text format
*/user/YOUR_USER_ID/daily_revenue_txt_python*
- Local location */home/YOUR_USER_ID/daily_revenue_python*
Solution need to be stored under
*/home/YOUR_USER_ID/daily_revenue_python.txt*
