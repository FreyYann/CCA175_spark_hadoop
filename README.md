# Projects of CCA175


## Task I: Insight of retail_db 
- Get daily revenue by product considering completed and closed orders.
- Data need to be sorted in ascending order by date and then descending
- order by revenue computed for each product for each day.
- Data for orders and order_items is available in HDFS  
*/public/retail_db/orders and /public/retail_db/order_items*

### Data for products is available locally under 
*/data/retail_db/products*

### Final output need to be stored under
- HDFS location – avro format
*/user/YOUR_USER_ID/daily_revenue_avro_python*
- HDFS location – text format
*/user/YOUR_USER_ID/daily_revenue_txt_python*
- Local location */home/YOUR_USER_ID/daily_revenue_python*
Solution need to be stored under
*/home/YOUR_USER_ID/daily_revenue_python.txt*

## Task II
- Get daily product revenue
- Orders eg. order_id, order_date, order_customer_id, order_status
- Order_items eg. order_items_id, order_items_order_id, order_items_product_id,
order_items_quantity, order_items_subtotal, order_items_product_price
- Data is comma separated
- fetch data using spark.read.csv
- apply type cast function


## Excersice 1 monthly crime count by type
- Details - Duration 40 minutes
- Data set URL 596
- Choose language of your choice Python or Scala
- Data is available in HDFS file system under /public/crime/csv
You can check properties of files using hadoop fs -ls -h /public/crime/csv
Structure of data (ID,Case Number,Date,Block,IUCR,Primary Type,Description,Location
- Description,Arrest,Domestic,Beat,District,Ward,Community Area,FBI Code,X Coordinate,Y Coordinate,Year,Updated On,Latitude,Longitude,Location)
- File format - text file
- Delimiter - “,”
- Get monthly count of primary crime type, sorted by month in ascending and number of crimes per type in descending order
Store the result in HDFS path /user/<YOUR_USER_ID>/solutions/solution01/crimes_by_type_by_month
- Output File Format: TEXT
- Output Columns: Month in YYYYMM format, crime count, crime type
- Output Delimiter: \t (tab delimited)
- Output Compression: gzip

## Excersice 2 Get details of inactive customers
- Source directories: /data/retail_db/orders and /data/retail_db/customers
- Source delimiter: comma (“,”)
- Source Columns - orders - order_id, order_date, order_customer_id, order_status
Source Columns - customers - customer_id, customer_fname, customer_lname and many more
- Get the customers who have not placed any orders, sorted by customer_lname and then customer_fname
- Target Columns: customer_lname, customer_fname
- Target Directory: /user/<YOUR_USER_ID>/solutions/solutions02/inactive_customers
- Target File Format: TEXT
Target Delimiter: comma (“, ”)
- Compression: N/A

## Excersice 3 Get top 3 crime 
Data is available in HDFS file system under /public/crime/csv
Structure of data (ID,Case Number,Date,Block,IUCR,Primary Type,Description,Location Description,Arrest,Domestic,Beat,District,Ward,Community Area,FBI Code,X Coordinate,Y Coordinate,Year,Updated On,Latitude,Longitude,Location)
File format - text file
Delimiter - “,” (use regex while splitting split(",(?=(?:[^\"]*\"[^\"]*\")*[^\"]*$)", -1), as there are some fields with comma and enclosed using double quotes.
Get top 3 crime types based on number of incidents in RESIDENCE area using “Location Description”
Store the result in HDFS path /user/<YOUR_USER_ID>/solutions/solution03/RESIDENCE_AREA_CRIMINAL_TYPE_DATA
Output Fields: Crime Type, Number of Incidents
Output File Format: JSON
Output Delimiter: N/A
Output Compression: No

## Exercise 04 - Convert nyse data to parquet