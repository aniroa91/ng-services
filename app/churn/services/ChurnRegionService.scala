package service

import scala.collection.immutable.Map
import com.sksamuel.elastic4s.ElasticsearchClientUri
import com.sksamuel.elastic4s.TcpClient
import com.sksamuel.elastic4s.http.ElasticDsl.{RichFuture, RichString, SearchHttpExecutable, SearchShow, percentilesAggregation, query, rangeAggregation, search, termsAgg, termsAggregation, _}
import com.sksamuel.elastic4s.http.search.SearchResponse
import play.api.libs.json.JsArray
import play.api.libs.json.JsNumber
import play.api.libs.json.JsObject
import play.api.libs.json.JsString
import play.api.libs.json.JsValue
import play.api.libs.json.Json
import churn.utils.{AgeGroupUtil, CommonUtil}
import play.api.mvc.{AnyContent, Request}

import scalaj.http.Http
import services.Configure
import services.domain.CommonService

object  ChurnRegionService{

  val client = Configure.client

  def getChurnGroupMonth(queryString: String) ={
    val request = search(s"profile-internet-contract-*" / "docs") query(queryString +" AND !(Region:0)") aggregations (
      termsAggregation("month")
        .field("month")
        .subaggs(
          termsAggregation("Status")
            .field("Status")
            .subaggs(
              rangeAggregation("Age")
                .field("Age")
                .range("6", 0, 6.0001)
                .range("12", 6.001, 12.001)
                .range("18", 12.001, 18.001)
                .range("24", 18.001, 24.001)
                .range("30", 24.001, 30.001)
                .range("36", 30.001, 36.001)
                .range("42", 36.001, 42.001)
                .range("48", 42.001, 48.001)
                .range("54", 48.001, 54.001)
                .range("60", 54.001, 60.001)
                .range("66", 60.001, Double.MaxValue)
            ) size 1000
        ) size 13
      )
    val rs = client.execute(request).await
    CommonService.getMultiAggregations(rs.aggregations.get("month"),"Status" ,"Age").flatMap(x => x._2.map(y => x._1 -> y))
      .map(x => (x._1 , x._2._1) -> x._2._2)
      .flatMap(x => x._2.map(y => x._1 -> y))
      .map(x => (x._1._1, x._1._2 , x._2._1, x._2._2))
  }

  def getChurnGroupbyStatus(queryString: String, field: String) ={
    val request = search(s"profile-internet-contract-*" / "docs") query(queryString +" AND !(Region:0)") aggregations (
      termsAggregation("Status")
        .field("Status")
        .subaggs(
          termsAggregation(s"$field")
            .field(s"$field") size 1000
        ) size 1000
      )
    val rs = client.execute(request).await
    getSecondAggregations(rs.aggregations.get("Status"), field).flatMap(x=> x._3.map(y=> (x._1,x._2) -> y))
      .map(x=> (x._1._1, x._2._1, x._2._2, x._1._2))
  }

  def getTrendRegionMonth() ={
    val request = search(s"profile-internet-contract-*" / "docs") query("!(Region:0)") aggregations (
      termsAggregation("month")
        .field("month")
        .subaggs(
          termsAggregation("Status")
            .field("Status")
            .subaggs(
              termsAggregation("Region")
                .field("Region")
            ) size 1000
        ) size 13
      )
   // println(client.show(request))
    val rs = client.execute(request).await
    getChurnRateAndPercentage(rs,"month","Status" ,"Region").map(x=> (x._1, x._2, x._3, x._5, x._6))
  }

  private def getSecondAggregations(aggr: Option[AnyRef],secondField: String):  Array[(String,Long, Array[(String, Long)])] = {
    aggr.getOrElse("buckets", Map[String, AnyRef]()).asInstanceOf[Map[String, AnyRef]]
      .getOrElse("buckets", List).asInstanceOf[List[AnyRef]]
      .map(x => x.asInstanceOf[Map[String, AnyRef]])
      .map(x => {
        val key = x.getOrElse("key", "0L").toString
        val count = x.getOrElse("doc_count",0L).toString.toLong
        val map = x.getOrElse(s"$secondField",Map[String,AnyRef]()).asInstanceOf[Map[String,AnyRef]]
          .getOrElse("buckets",List).asInstanceOf[List[AnyRef]]
          .map(x => x.asInstanceOf[Map[String,AnyRef]])
          .map(x => {
            val keyCard = x.getOrElse("key","0L").toString
            val count = x.getOrElse("doc_count",0L).toString.toLong
            (keyCard,count)
          }).toArray
        (key, count, map)
      })
      .toArray
  }

  private def getChurnRateAndPercentage(response: SearchResponse, term1: String, term2: String, term3: String): Array[(String, String, String, Int, Int, Int)] = {
    if (response.aggregations != null) {
      response.aggregations
        .getOrElse(term1, Map[String, AnyRef]()).asInstanceOf[Map[String, AnyRef]]
        .getOrElse("buckets", List).asInstanceOf[List[AnyRef]]
        .map(x => x.asInstanceOf[Map[String, AnyRef]])
        .flatMap(x => {
          val term1Key = x.getOrElse("key", "key").toString()
          val term1Count = x.getOrElse("doc_count", "0").toString().toInt
          x.getOrElse(term2, Map[String, AnyRef]()).asInstanceOf[Map[String, AnyRef]]
            .getOrElse("buckets", List).asInstanceOf[List[AnyRef]]
            .map(y => y.asInstanceOf[Map[String, AnyRef]])
            .flatMap(y => {
              val term2Key = y.getOrElse("key", "key").toString()
              val term2Count = y.getOrElse("doc_count", "0").toString().toInt
              y.getOrElse(term3, Map[String, AnyRef]()).asInstanceOf[Map[String, AnyRef]]
                .getOrElse("buckets", List).asInstanceOf[List[AnyRef]]
                .map(z => z.asInstanceOf[Map[String, AnyRef]])
                .map(z => {
                  val term3Key = z.getOrElse("key", "key").toString()
                  val term3Count = z.getOrElse("doc_count", "0").toString().toInt
                  (term1Key, term2Key, term3Key, term1Count, term2Count, term3Count)
                })
            })
        }).toArray
    } else {
      Array[(String, String, String, Int, Int, Int)]()
    }
  }

  def calChurnRateAndPercentageByStatus(array: Array[(String, String, Long, Long)], status: Int, _type: String) = {
    var sumAll = array.filter(x=> x._1.toInt == status).map(x=> x._3).sum
    if(_type.equals("Profile")) sumAll =  array.filter(x=> x._1.toInt == status).slice(0,1).map(x=> x._4).sum
    val sumByStatus    = array.filter(x=> x._1.toInt == status).groupBy(x=> x._2).map(x=> x._1 -> x._2.map(y=> y._3).sum)
    val sumByStatusAll = array.groupBy(x=> x._2).map(x=>x._1 -> x._2.map(y=> y._3).sum)

    // month, age, rate, percentage
    sumByStatus.map(x=> (x._1, x._2, sumByStatusAll.filter(y=> y._1 == x._1).map(x=> x._2).sum))
      .map(x => (x._1, CommonService.format2Decimal(x._2  * 100.0/ x._3), CommonService.format2Decimal(x._2 * 100.0 / sumAll) ) ).toArray.sorted
  }

  def calChurnRateAndPercentageForRegionMonth(array: Array[(String, String, String, Int, Int)], status: Int) = {
    val rs = array.filter(x => x._2.toInt == status)
    //rs.foreach(println)
    val sumByRegionMonth       = rs.map(x=> (x._1, x._3, x._5, CommonService.format2Decimal(x._5 * 100.0 / x._4)))
    val sumByRegionMonthStatus = array.groupBy(x=> x._1-> x._3).map(x=> (x._1._1,x._1._2, x._2.map(y=> y._5).sum))
    sumByRegionMonth.map(x=> (x._2.toInt, x._1, CommonService.format2Decimal(x._3 * 100.0 / sumByRegionMonthStatus.filter(y=> y._1 == x._1).filter(y=> y._2 == x._2)
      .map(y=> y._3).sum), x._4))
  }

  def getInternet(request: Request[AnyContent]) = {
    var status = 1 // Huy dich vu default
    var age = "*"
    var region = "*"
    var month = "2018-07"
    if(request != null) {
      status = request.body.asFormUrlEncoded.get("status").head.toInt
      age = if(request.body.asFormUrlEncoded.get("age").head != "") request.body.asFormUrlEncoded.get("age").head else "*"
      region = if(request.body.asFormUrlEncoded.get("region").head != "") request.body.asFormUrlEncoded.get("region").head else "*"
      month = request.body.asFormUrlEncoded.get("month").head
    }

    val churnRegionMonth = calChurnRateAndPercentageForRegionMonth(getTrendRegionMonth(), status).filter(x=> x._2 != CommonService.getCurrentMonth()).sorted
    //println(s"month:$month AND Region:$region")
    val churnRegion      = calChurnRateAndPercentageByStatus(getChurnGroupbyStatus(s"month:$month", "Region"), status, "Region")
    val churnProfile     = calChurnRateAndPercentageByStatus(getChurnGroupbyStatus(s"month:$month", "Profile"), status, "Profile")
      .map(x=> (x._1, x._2, x._3, 2 * x._2 * x._3 * 0.01/(x._2 + x._3))).sortWith((x, y) => x._4 > y._4)
      .slice(0, 8).map(x=> (x._1, x._2, x._3))

    (churnRegion, churnRegionMonth, churnProfile)
  }
}