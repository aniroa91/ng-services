****
 Install scala version 2.13.0, sbt version 0.13.15
***

1. Spark process read json and store parquet:
 - Install JDK 1.8.0, Spark (2.4.4), Elastisearch (6.8.6) and Kibana local
 - Run main in Class ParquetService
 - Change sources matching your local disk
 
2. Dashboard Visualization on Kibana:
 - Run main in class Init Data: calculate and group by data to store Elastisearch
 - Add data on Kibana to show heatmap chart 