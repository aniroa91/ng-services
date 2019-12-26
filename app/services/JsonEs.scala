package services

/*
 Author: Hoangnh
 Date: 2019/12/23
 Description: Read json file and convert to Parquet
*/

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.elasticsearch.spark.sql._
import utils.CommonUtil

object JsonES{

  val spark = SparkSession.builder().appName("Spark parquet").config("spark.master", "local").getOrCreate()
  val outSrcLocation = "/home/hoangnh44/Desktop/data/outbound/locations"

  def main(args: Array[String]) {
    val t0 = System.currentTimeMillis()
    val locationDF = spark.read.parquet(s"$outSrcLocation/*")
    //locationDF.printSchema()
    val date =  udf { s: String => CommonUtil.create(s.toLong).toString(CommonUtil.YMD) }
    val merge = udf( (first: Double, second: Double) => { CommonUtil.formatDecimalByDigit(first, 4) + " => " + CommonUtil.formatDecimalByDigit(second, 4) } )
    val df = locationDF.withColumn("day", date(col("created_time")))
        .withColumn("address", merge(col("location.lat"),col("location.long"))).drop("location")
    val dfGroup = df.groupBy("day","address").agg(count("userId")).withColumnRenamed("count(userId)", "numUser")
    dfGroup.saveToEs("loc-date/docs")
    /*df.printSchema()
    df.createTempView("location")
    val sql = df.sparkSession.sql("select * from location limit 1")
    sql.show(1)*/
    //df.saveToEs("locDate/docs")
    println("Time job: "+(System.currentTimeMillis()-t0))
  }
}
