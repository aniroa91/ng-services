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

case class Bubble(x: Double, y: Double, z: Double, name: String)

object  ChurnAgeService{

  val client = Configure.client

  def getChurnGroupMonth(queryString: String) ={
    val request = search(s"profile-internet-contract-*" / "docs") query(queryString) aggregations (
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

  def getChurnGroupAge(queryString: String) ={
    val request = search(s"profile-internet-contract-*" / "docs") query(queryString) aggregations (
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
      )
    val rs = client.execute(request).await
    CommonService.getTerm1(rs, "Status", "Age").map(x => (x._1, x._2, x._3))
  }

  def getChurn(month: String) ={
    val request = search(s"profile-internet-contract-${month}" / "docs") aggregations (
      termsAggregation("Region")
        .field("Region")
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
        )
      )
    val rs = client.execute(request).await
    getChurnRateAndPercentage(rs,"Region","Status" ,"Age").map(x=> (x._1, x._2, x._3, x._5, x._6))
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

   def calChurnRateAndPercentageByAge(array: Array[(String, String, Long)], status: Int) = {
     val sumAll = array.filter(x=> x._1.toInt == status).map(x=> x._3).sum
     val sumByStatus    = array.filter(x=> x._1.toInt == status).groupBy(x=> x._2).map(x=> x._1 -> x._2.map(y=> y._3).sum)
     val sumByStatusAll = array.groupBy(x=> x._2).map(x=>x._1 -> x._2.map(y=> y._3).sum)

     // month, age, rate, percentage
     sumByStatus.map(x=> (x._1, x._2, sumByStatusAll.filter(y=> y._1 == x._1).map(x=> x._2).sum))
       .map(x => (x._1.toInt, CommonService.format2Decimal(x._2  * 100.0/ x._3), CommonService.format2Decimal(x._2 * 100.0 / sumAll) ) ).toArray.sorted
  }

   def calChurnRateAndPercentageByMonth(array: Array[(String, String, String, Long)], status: Int) = {
    val sumByMonth     = array.filter(x=> x._1 != CommonService.getCurrentMonth())
    val sumByStatus    = sumByMonth.filter(x=> x._2.toInt == status).groupBy(x=> x._1).map(x=> x._1 -> x._2.map(y=> y._4).sum).toArray.sortWith((x, y) => x._1 > y._1).sorted
    val sumByStatusAll = sumByMonth.groupBy(x=> x._1).map(x=> x._1 -> x._2.map(y=> y._4).sum).toArray.sortWith((x, y) => x._1 > y._1).sorted
    // month, age, rate, percentage
     sumByStatus.map(x=> (x._1, x._2, sumByStatusAll.filter(y=> y._1 == x._1).map(x=> x._2).sum))
       .map(x => (x._1, CommonService.format2Decimal(x._2  * 100.0/ x._3), CommonService.format2Decimal(x._3 * 100.0 / x._3) ) ).sorted
  }

   def calChurnRateAndPercentageForChurnbyStatus(array: Array[(String, String, String, Int, Int)], status: Int) = {
    val rs = array.filter(x => x._2.toInt == status).filter(x=> x._1.toInt != 0)
    val sumByRegionAge       = rs.map(x=> (x._1, x._3, x._5, CommonService.format2Decimal(x._5 * 100.0 / x._4)))
    val sumByRegionAgeStatus = array.groupBy(x=> x._1-> x._3).map(x=> (x._1._1,x._1._2, x._2.map(y=> y._5).sum))
     sumByRegionAge.map(x=> (x._1.toInt, x._2.toInt, CommonService.format2Decimal(x._3 * 100.0 / sumByRegionAgeStatus.filter(y=> y._1 == x._1).filter(y=> y._2 == x._2)
      .map(y=> y._3).sum), x._4))
  }

  def getInternet(request: Request[AnyContent]) = {
    var status = 1 // Huy dich vu default
    var age = "*"
    var region = "*"
    var month = CommonService.getPrevMonth()
    if(request != null) {
       status = request.body.asFormUrlEncoded.get("status").head.toInt
       age = if(request.body.asFormUrlEncoded.get("age").head != "") request.body.asFormUrlEncoded.get("age").head else "*"
       region = if(request.body.asFormUrlEncoded.get("region").head != "") request.body.asFormUrlEncoded.get("region").head else "*"
       month = request.body.asFormUrlEncoded.get("month").head
    }

    val rsStatus = calChurnRateAndPercentageForChurnbyStatus(getChurn(month), status)
    val mapAge = AgeGroupUtil.AGE.map(y=> y._2).toArray
    val mapRegion = CommonUtil.REGION.map(x=> x._2).toArray
    var mapGroup = Array[(Int, Int)]()
    val arrEmpty = Array(0.0 -> 0.0)
    for(i <- 0 until mapRegion.size){
      for(j <- 0 until mapAge.size){
        mapGroup :+= (mapRegion(i) -> mapAge(j))
      }
    }
    val churnStatus = mapGroup.map(x=> (x._1, x._2, if(rsStatus.filter(y=> y._1 == x._1).filter(y=> y._2 == x._2).length ==0) arrEmpty else rsStatus.filter(y=> y._1 == x._1).filter(y=> y._2 == x._2).map(x=> x._3 -> x._4)))
      .flatMap(x=> x._3.map(y=> (x._1, x._2) -> y))
      .map(x=> (x._1._1, x._1._2, x._2._1, x._2._2))

    //println(s"month:$month AND Region:$region")
    val churnAge = calChurnRateAndPercentageByAge(getChurnGroupAge(s"month:$month AND Region:$region"), status)
    val churnMonth = calChurnRateAndPercentageByMonth(getChurnGroupMonth(s"Age:$age AND Region:$region"), status)

    (churnStatus, churnAge, churnMonth)
  }
}