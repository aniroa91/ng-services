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
import org.elasticsearch.search.sort.SortOrder
import play.api.mvc.{AnyContent, Request}
import churn.models.CallogResponse
import scalaj.http.Http
import services.Configure
import services.domain.CommonService

object  ChurnCallogService{

  val client = Configure.client

  def getNumberOfContracts(indexs: String, queryString: String) ={
    val request = search(s"$indexs-*" / "docs") query(queryString + " AND !(TenGoi: \"FTTH - TV Only\") AND !(TenGoi: \"ADSL - TV Only\") AND !(TenGoi: \"ADSL - TV Gold\") AND !(TenGoi: \"FTTH - TV Gold\") ") aggregations (
      termsAggregation("month")
        .field("month") size 24
      ) sortBy( fieldSort("month") order SortOrder.DESC)
    val rs = client.execute(request).await
    CommonService.getAggregationsSiglog(rs.aggregations.get("month")).filter(x=> x._1 != CommonService.getCurrentMonth()).sortWith((x, y) => x._1 > y._1).slice(0,12).sorted
  }

  def getChurnByCates(queryString: String) ={
    val request = search(s"churn-calllog-*" / "docs") query(queryString + " AND !(region:0) AND !(TenGoi: \"FTTH - TV Only\") AND !(TenGoi: \"ADSL - TV Only\") AND !(TenGoi: \"ADSL - TV Gold\") AND !(TenGoi: \"FTTH - TV Gold\") ") aggregations (
      termsAggregation("callCategory")
        .field("callCategory")
        .subaggs(
          termsAggregation("status")
            .field("status") size 1000
        ) size 1000
      )

    /*val request1 = search(s"churn-calllog-2018-07" / "docs") query(queryString + " AND !(region:0) AND !(TenGoi: \"FTTH - TV Only\") AND !(TenGoi: \"ADSL - TV Only\") AND !(TenGoi: \"ADSL - TV Gold\") AND !(TenGoi: \"FTTH - TV Gold\") ") aggregations (
      nestedAggregation("calllog", "calllog")

      )

    println(client.show(request1))*/

    val rs = client.execute(request).await
    ChurnRegionService.getSecondAggregations(rs.aggregations.get("callCategory"), "status")
      .flatMap(x=> x._3.map(y=> (x._1,x._2) -> y))
      .map(x=> (x._1._1, x._2._1, x._2._2, x._1._2))
  }

  def getTrendCallIn(queryString: String) ={
    val request = search(s"churn-calllog-*" / "docs") query(queryString + " AND !(region:0) AND !(TenGoi: \"FTTH - TV Only\") AND !(TenGoi: \"ADSL - TV Only\") AND !(TenGoi: \"ADSL - TV Gold\") AND !(TenGoi: \"FTTH - TV Gold\") ") aggregations (
      termsAggregation("month")
        .field("month")
        .subaggs(
          termsAggregation("status")
            .field("status") size 1000
        ) size 24
      ) sortBy( fieldSort("month") order SortOrder.DESC)
    val rs = client.execute(request).await
    ChurnRegionService.getSecondAggregations(rs.aggregations.get("month"), "status")
      .flatMap(x=> x._3.map(y=> (x._1,x._2) -> y))
      .map(x=> (x._1._1, x._2._1, x._2._2, x._1._2))
  }

  def getChurnCallInbyRegion(indexs: String, queryString: String, region: String) ={
    val request = search(s"$indexs-*" / "docs") query(queryString + " AND !(TenGoi: \"FTTH - TV Only\") AND !(TenGoi: \"ADSL - TV Only\") AND !(TenGoi: \"ADSL - TV Gold\") AND !(TenGoi: \"FTTH - TV Gold\") ") aggregations (
      termsAggregation("month")
        .field("month")
        .subaggs(
          termsAggregation(region)
            .field(region) size 1000
        ) size 24
      ) sortBy( fieldSort("month") order SortOrder.DESC)
    val rs = client.execute(request).await
    CommonService.getSecondAggregations(rs.aggregations.get("month"), region)
      .flatMap(x=> x._2.map(y=> x._1 -> y))
      .map(x=> (x._1, x._2._1, x._2._2)).filter(x=> x._1 != CommonService.getCurrentMonth()).sortWith((x, y) => x._1 > y._1)
  }

  def getChurnByRegionAge(indexs:String, queryString: String, age: String, region: String) ={
    val request = search(s"$indexs" / "docs") query(queryString+" AND !(TenGoi: \"FTTH - TV Only\") AND !(TenGoi: \"ADSL - TV Only\") AND !(TenGoi: \"ADSL - TV Gold\") AND !(TenGoi: \"FTTH - TV Gold\") ") aggregations (
      rangeAggregation("age")
        .field(s"$age")
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
        .subaggs(
          termsAggregation("region")
            .field(s"$region") size 1000
        )
      )
    val rs = client.execute(request).await
    CommonService.getSecondAggregations(rs.aggregations.get("age"), "region")
      .flatMap(x=> x._2.map(y=> x._1 -> y))
      .map(x=> (x._1, x._2._1, x._2._2))
  }

  def getCallInRegionMonth(queryString: String) = {
    val request = search(s"churn-calllog-*" / "docs") query(queryString+" AND !(region:0) AND !(TenGoi: \"FTTH - TV Only\") AND !(TenGoi: \"ADSL - TV Only\") AND !(TenGoi: \"ADSL - TV Gold\") AND !(TenGoi: \"FTTH - TV Gold\") ") aggregations (
      termsAggregation("month")
        .field("month")
        .subaggs(
          termsAggregation("status")
            .field("status")
            .subaggs(
              termsAggregation("region")
                .field("region") size 1000
            ) size 1000
        ) size 24
      )
    val rs = client.execute(request).await
    ChurnRegionService.getChurnRateAndPercentage(rs,"month","status" , "region").map(x=> (x._1, x._2, x._3, x._5, x._6))
  }

  def getCallInRegionAge(month: String, queryString: String) = {
    val request = search(s"churn-calllog-${month}" / "docs") query(queryString+" AND !(region:0) AND !(TenGoi: \"FTTH - TV Only\") AND !(TenGoi: \"ADSL - TV Only\") AND !(TenGoi: \"ADSL - TV Gold\") AND !(TenGoi: \"FTTH - TV Gold\") ") aggregations (
      termsAggregation("region")
        .field("region")
        .subaggs(
          termsAggregation("status")
            .field("status")
            .subaggs(
              termsAggregation("lifeGroup")
                .field("lifeGroup") size 100
            ) size 1000
        )
      )
    val rs = client.execute(request).await
    ChurnAgeService.getChurnRateAndPercentage(rs,"region","status" ,"lifeGroup").map(x=> (x._1, x._2, x._3, x._5, x._6))
  }


  def calChurnRateAndPercentageCategory(array: Array[(String, String, Long, Long)], status: Int) ={
    val sumHuydv  = array.filter(x=> x._2.toInt == status).map(x=> x._3).sum
    val rsHuydv   = array.filter(x=> x._2.toInt == status).map(x=> (x._1, CommonService.format3Decimal(x._3 * 100.00 / x._4), CommonService.format3Decimal(x._3 * 100.00 / sumHuydv)))
    val top9Cates = rsHuydv.map(x=> (x._1, x._2, x._3, 2*x._2*x._3*1.00/(x._2+x._3))).sortWith((x, y) => x._4 > y._4).slice(0,9).map(x=> (x._1,x._2,x._3))
    val others    = array.filter(x=> x._2.toInt == status).filter(x=> top9Cates.map(x=> x._1).indexOf(x._1) < 0)
      .groupBy(x=> x._2).map(x=> (x._1 , x._2.map(y=> y._3).sum, x._2.map(y=> y._4).sum))
      .map(x=> ("Others", CommonService.format3Decimal(x._2 * 100.00 / x._3), CommonService.format3Decimal(x._2 * 100.00 / sumHuydv))).toArray

    top9Cates ++ others
  }

  def calTrendCallinRateAndPercentage(callInArray: Array[(String, String, Long, Long)], allChurn: Array[(String, Long)], status: Int) ={
    val rs  = callInArray.filter(x=> x._2.toInt == status).filter(x=> allChurn.map(y=> y._1).indexOf(x._1) >=0)
    rs.map(x=> (x._1, x._3, x._4, allChurn.toMap.get(x._1).get)).map(x=>(x._1, CommonService.format3Decimal(x._2 * 100.00 / x._3), CommonService.format3Decimal(x._2 * 100.00 / x._4)))
      .sortWith((x, y) => x._1 > y._1).slice(0, 12).sorted
  }

  def calChurnCallinRateAndPercentagebyRegion(callInRegion: Array[(String, String, Long)], allRegion: Array[(String, String, Long)]) = {
    val catesMonth  = allRegion.map(x=> x._1).distinct.sortWith((x, y) => x > y).slice(0,12)
    val rs = callInRegion.filter(x=> catesMonth.indexOf(x._1) >= 0).map(x=> (x._1, x._2, x._3, allRegion.filter(y=> y._1 == x._1).filter(y=> y._2 == x._2).map(y=> y._3).sum))
        .map(x=> (x._1, x._2, CommonService.format2Decimal(x._3 * 100.00 / x._4), x._3)).sorted

    (catesMonth.sorted, rs)
  }

  def calCallInRateAndPercentageRegionAge(array: Array[(String, String, String, Int, Int)], status: Int) = {
    val rs = array.filter(x => x._2.toInt == status)
    //rs.foreach(println)
    val sumByRegionAge       = rs.map(x=> (x._1, x._3, x._5, CommonService.format2Decimal(x._5 * 100.0 / x._4)))
    val sumByRegionAgeStatus = array.groupBy(x=> x._1-> x._3).map(x=> (x._1._1,x._1._2, x._2.map(y=> y._5).sum))
    sumByRegionAge.map(x=> (x._1.toInt, if(x._2 == "6-12") "06-12" else x._2, CommonService.format2Decimal(x._3 * 100.0 / sumByRegionAgeStatus.filter(y=> y._1 == x._1).filter(y=> y._2 == x._2)
      .map(y=> y._3).sum), x._4, x._3))
  }

  def getInternet(request: Request[AnyContent]) = {
    var status = 1 // Huy dich vu default
    var ageCallIn = "*"
    var ageAll = "*"
    var region = "*"
    var cate = "*"
    var month = "2018-07"
    if(request != null) {
      status = request.body.asFormUrlEncoded.get("status").head.toInt
      if(request.body.asFormUrlEncoded.get("age").head != "" && request.body.asFormUrlEncoded.get("age").head == "12"){
        ageCallIn =  "\"6-12\""
      }
      else if(request.body.asFormUrlEncoded.get("age").head != "" && request.body.asFormUrlEncoded.get("age").head != "12"){
        ageCallIn = "\""+AgeGroupUtil.getAgeById(request.body.asFormUrlEncoded.get("age").head.toInt) +"\""
      }
      ageAll = if(request.body.asFormUrlEncoded.get("age").head != "")  AgeGroupUtil.getCalAgeByName(AgeGroupUtil.getAgeById(request.body.asFormUrlEncoded.get("age").head.toInt)) else "*"
      region = if(request.body.asFormUrlEncoded.get("region").head != "") request.body.asFormUrlEncoded.get("region").head else "*"
      month = request.body.asFormUrlEncoded.get("month").head
      request.body.asFormUrlEncoded.get("cateCurr").head match {
        case "" => {
          cate = "*"
        }
        case "Others" =>  {
          val topCates = request.body.asFormUrlEncoded.get("topCate").head
          cate = topCates.split(",").filter(!_.contains("Others")).map(x=> "!(callCategory:\""+x+"\")").mkString(" AND ")
        }
        case _ => {
          cate = request.body.asFormUrlEncoded.get("cateCurr").head
        }
      }
    }
    // Number of Contracts Who Call in and Number of Inbound Calls
    val contractAll = getNumberOfContracts("profile-internet-contract", s"Region:$region AND $ageAll AND !(Region:0)").slice(0,12).sorted
    val whoCallIn   = getNumberOfContracts("churn-calllog", "* AND !(region:0)").filter(x=> contractAll.map(y=> y._1).indexOf(x._1) >=0)
      .map(x=> (x._1, x._2, CommonService.format2Decimal(x._2 * 100.00 / contractAll.toMap.get(x._1).get))).sorted

    // Churn Rate and Churn Percentage by Call Category
    val churnCates = calChurnRateAndPercentageCategory(getChurnByCates(s"month:$month"), status)

    // Number of Contracts Who Call in by Region by Contract Age
    val numOfCallIn   = getChurnByRegionAge(s"churn-calllog-${month}", "* AND !(region:0)", "age", "region")
    val numOfContract = getChurnByRegionAge(s"profile-internet-contract-${month}", "* AND !(Region:0)", "Age", "Region")
    val callInRegionAge = numOfCallIn.map(x=> (x._1, x._2, CommonService.format2Decimal(x._3 * 100.00 / numOfContract.filter(y=> y._1 == x._1).filter(y=> y._2 == x._2).map(y=> y._3).sum), x._3))

    // For Contracts Who Call in: Trend in Churn Rate and Churn Percentage
    val allChurn_count  = getNumberOfContracts("profile-internet-contract", s"Status:$status AND !(Region:0)")
    val trendCallIn = calTrendCallinRateAndPercentage(getTrendCallIn("*"), allChurn_count ,status)

    //  Number of Contracts Who Call In by Region
    val ctAllRegion  = getChurnCallInbyRegion("profile-internet-contract", "* AND !(Region:0)", "Region")
    val callInRegion = calChurnCallinRateAndPercentagebyRegion(getChurnCallInbyRegion("churn-calllog", "* AND !(region:0)", "region"), ctAllRegion)

    // region and month trends
    val trendRegionMonth = ChurnRegionService.calChurnRateAndPercentageForRegionMonth(getCallInRegionMonth("*"), status).filter(x=> x._2 != CommonService.getCurrentMonth()).sorted
    val top12monthRegion = trendRegionMonth.map(x=> x._2).distinct.sortWith((x, y) => x > y).filter(x=> x != CommonService.getCurrentMonth()).slice(0,12).sorted
    val topMonth = if(top12monthRegion.length >= 12) 13 else top12monthRegion.length+1
    val mapMonthRegion   = if(top12monthRegion.length >0) (1 until topMonth).map(x=> top12monthRegion(x-1) -> x).toMap else Map[String, Int]()
    val rsRegionMonth    = if(top12monthRegion.length >0) trendRegionMonth.filter(x=> top12monthRegion.indexOf(x._2) >=0).map(x=> (x._1, mapMonthRegion.get(x._2).get, x._3, x._4, x._5))
    else Array[(Int, Int, Double, Double, Int)]()
    var arrGroup = Array[(Int, Int)]()
    val mapRegion = CommonUtil.REGION.map(x=> x._2).toArray
    val mapMonths = mapMonthRegion.map(x=> x._2).toArray
    val arrEmpty = Array((0.0, 0.0, 0))
    for(i <- 0 until mapRegion.size){
      for(j <- 0 until mapMonths.size){
        arrGroup :+= (mapRegion(i) -> mapMonths(j))
      }
    }
    val callRegionMonth = arrGroup.map(x=> (x._1, x._2, if(rsRegionMonth.filter(y=> y._1 == x._1).filter(y=> y._2 == x._2).length ==0) arrEmpty else rsRegionMonth.filter(y=> y._1 == x._1).filter(y=> y._2 == x._2)
      .map(x=> (x._3, x._4, x._5)))).flatMap(x=> x._3.map(y=> (x._1, x._2) -> y))
      .map(x=> (x._1._1, x._1._2, x._2._1, x._2._2, x._2._3))

    // region and age trends
    val trendRegionAge = calCallInRateAndPercentageRegionAge(getCallInRegionAge(month, "*"), status)
    val mapAge = AgeGroupUtil.AGE.map(y=> y._2).toArray
    val mapRegion1 = CommonUtil.REGION.map(x=> x._2).toArray
    var mapGroup1 = Array[(Int, Int)]()
    val arrEmpty1 = Array((0.0, 0.0, 0))
    for(i <- 0 until mapRegion1.size){
      for(j <- 0 until mapAge.size){
        mapGroup1 :+= (mapRegion1(i) -> mapAge(j))
      }
    }
    val callRegionAge = mapGroup1.map(x=> (x._1, x._2, if(trendRegionAge.filter(y=> y._1 == x._1).filter(y=> x._2 == AgeGroupUtil.getAgeIdByName(y._2)).length ==0) arrEmpty1 else trendRegionAge.filter(y=> y._1 == x._1)
      .filter(y=> x._2 == AgeGroupUtil.getAgeIdByName(y._2)).map(x=> (x._3, x._4, x._5))))
      .flatMap(x=> x._3.map(y=> (x._1, x._2) -> y))
      .map(x=> (x._1._1, x._1._2, x._2._1, x._2._2, x._2._3))

    CallogResponse(whoCallIn, churnCates, callInRegionAge, trendCallIn, callInRegion, (mapMonthRegion, callRegionMonth), callRegionAge)
  }
}