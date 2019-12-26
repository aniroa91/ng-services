package services

import utils.CommonUtil
import utils.FileUtil
import org.apache.spark.sql.SparkSession
import scala.util.matching.Regex

object ParquetService{
  val spark = SparkSession.builder().appName("Spark parquet").config("spark.master", "local").getOrCreate()
  val inSrcLocation = "/home/hoangnh44/Desktop/data/inbound/locations"
  val outSrcLocation = "/home/hoangnh44/Desktop/data/outbound/locations"
  val inSrcCandies = "/home/hoangnh44/Desktop/data/inbound/surpriseCandies"
  val outSrcCandies = "/home/hoangnh44/Desktop/data/outbound/surpriseCandies"
  val arrDays = CommonUtil.getRangeDay("2019-11")

  def readParquetFile() = {
    val locationDF = spark.read.parquet(s"$outSrcLocation/*")
    val viewName = "location"
    locationDF.createTempView(viewName)
    val dfNewLoc = spark.sql(s"SELECT userId,created_time,count(*) FROM $viewName GROUP BY userId,created_time")
    dfNewLoc.rdd.collect().map(row => (row(0),CommonUtil.create(row(1).toString.toLong).toString("yyyy-MM-dd"),row(2)))
      .groupBy(x=> x._2 -> x._1).map(x=> (x._1._1, x._1._2.toString, x._2.map(y=> y._3.toString.toLong).sum)).toArray.sorted
  }

  def main(args: Array[String]) {
    // get list days of month
    for (i <- 0 until arrDays.length){
      val date = arrDays(i)
      val inPathLoc = s"$inSrcLocation/locations-$date*.json"
      val outPathLoc = s"$outSrcLocation/locations-$date.parquet"
      val inPathCandies = s"$inSrcCandies/surpriseCandies-$date"
      val outPathCandies = s"$outSrcCandies/surpriseCandies-$date.parquet"
      // check exists files in each date
      val fileLocs = FileUtil.getFilesMatchingRegex(inSrcLocation, new Regex(s"locations-$date.*.json"))
      val fileCandies = FileUtil.getFilesMatchingRegex(inSrcCandies, new Regex(s"surpriseCandies-$date.*"))
      if(fileLocs.length > 0 && fileCandies.length >0 ){
        /* BEGIN read LOCATION folder */
        val locationDF = spark.read.textFile(inPathLoc)
        // check exists file to write
        if(!FileUtil.isExist(outPathLoc)) locationDF.write.parquet(outPathLoc)
        /* END read LOCATION folder */

        /* BEGIN read CANDIES folder */
        val candiesDF = spark.read.textFile(inPathCandies)
        if(!FileUtil.isExist(outPathCandies)) candiesDF.write.parquet(outPathCandies)
        /* END read CANDIES folder */
      }
    }
  }
}
