package service

import java.util

import churn.models.RegionResponse

import scala.collection.immutable.Map
import com.sksamuel.elastic4s.ElasticsearchClientUri
import com.sksamuel.elastic4s.TcpClient
import com.sksamuel.elastic4s.http.ElasticDsl.{RichFuture, RichString, SearchHttpExecutable, SearchShow, percentilesAggregation, query, rangeAggregation, search, termsAgg, termsAggregation, _}
import com.sksamuel.elastic4s.http.search.SearchResponse
import churn.utils.{AgeGroupUtil, CommonUtil}
import play.api.Logger
import play.api.mvc.{AnyContent, Request}
import services.Configure
import services.domain.CommonService
import org.joda.time.format.DateTimeFormat
import org.joda.time.DateTime

object  ChurnRegionService{

  val client = Configure.client

  val logger = Logger(this.getClass())

  def getChurnGroupMonth(queryString: String) ={
    val request = search(s"churn-contract-info-*" / "docs") query(queryString +" AND "+CommonUtil.filterCommon("package_name")) aggregations (
      termsAggregation("month")
        .field("month")
        .subaggs(
          termsAggregation("status")
            .field("status")
            .subaggs(
              rangeAggregation("age")
                .field("age")
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
    CommonService.getMultiAggregations(rs.aggregations.get("month"),"status" ,"age").flatMap(x => x._2.map(y => x._1 -> y))
      .map(x => (x._1 , x._2._1) -> x._2._2)
      .flatMap(x => x._2.map(y => x._1 -> y))
      .map(x => (x._1._1, x._1._2 , x._2._1, x._2._2))
  }

  def getChurnGroupbyStatusRegion(queryString: String, profile: String) ={
    val profileQuery = if(profile == "*") "" else s" AND package_name: $profile"
    val request = search(s"churn-contract-info-*" / "docs") query(queryString + profileQuery +" AND "+CommonUtil.filterCommon("package_name")) aggregations (
      termsAggregation("status")
        .field("status")
        .subaggs(
          termsAggregation("region")
            .field("region") size 1000
        ) size 1000
      ) size 0
    val rs = client.execute(request).await
    getSecondAggregations(rs.aggregations.get("status"), "region").flatMap(x=> x._3.map(y=> (x._1,x._2) -> y))
      .map(x=> (x._1._1, x._2._1, x._2._2, x._1._2))
  }

  def getChurnGroupbyStatusProfile(queryString: String) ={
    val request = search(s"churn-contract-info-*" / "docs") query(queryString + " AND "+CommonUtil.filterCommon("package_name")) aggregations (
      termsAggregation("status")
        .field("status")
        .subaggs(
          termsAggregation("tenGoi")
            .field("package_name") size 1000
        ) size 1000
      ) size 0
    val rs = client.execute(request).await
    getSecondAggregations(rs.aggregations.get("status"), "tenGoi").flatMap(x=> x._3.map(y=> (x._1,x._2) -> y))
      .map(x=> (x._1._1, x._2._1, x._2._2, x._1._2))
  }

  def getTrendProfileMonth(queryString: String) ={
    val request = search(s"churn-contract-info-*" / "docs") query(queryString + " AND "+CommonUtil.filterCommon("package_name")) aggregations (
      termsAggregation("month")
        .field("month")
        .subaggs(
          termsAggregation("status")
            .field("status")
            .subaggs(
              termsAggregation("tenGoi")
                .field("package_name") size 1000
            ) size 1000
        ) size 13
      ) size 0
    val rs = client.execute(request).await
    getChurnRateAndPercentage(rs, "month", "status" , "tenGoi").map(x=> (x._1, x._2, x._3, x._5, x._6))
  }

  def getTrendRegionMonth(queryString: String, profile: String) ={
    val profileQuery = if(profile == "*") "" else s" AND package_name: $profile"
    val request = search(s"churn-contract-info-*" / "docs") query(queryString + profileQuery + " AND "+CommonUtil.filterCommon("package_name")) aggregations (
      termsAggregation("month")
        .field("month")
        .subaggs(
          termsAggregation("status")
            .field("status")
            .subaggs(
              termsAggregation("region")
                .field("region") size 1000
            ) size 1000
        ) size 13
      ) size 0
    val rs = client.execute(request).await
    getChurnRateAndPercentage(rs,"month","status" , "region").map(x=> (x._1, x._2, x._3, x._5, x._6))
  }

  def getTrendAgeProfile(month: String, region: String) = {
    val request = search(s"churn-contract-info-${month}" / "docs") query(region +" AND "+CommonUtil.filterCommon("package_name")) aggregations (
      rangeAggregation("age")
        .field("age")
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
          termsAggregation("status")
            .field("status")
            .subaggs(
              termsAggregation("tenGoi")
                .field("package_name") size 1000
            ) size 1000
        )
      ) size 0
    val rs = client.execute(request).await
    getChurnRateAndPercentage(rs,"age", "status" , "tenGoi").map(x=> (x._1, x._2, x._3, x._5, x._6))
  }

  def getSecondAggregations(aggr: Option[AnyRef],secondField: String):  Array[(String,Long, Array[(String, Long)])] = {
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

  def getChurnRateAndPercentage(response: SearchResponse, term1: String, term2: String, term3: String): Array[(String, String, String, Int, Int, Int)] = {
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
    if(_type.equals("profile")) sumAll =  array.filter(x=> x._1.toInt == status).slice(0,1).map(x=> x._4).sum
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
      .map(y=> y._3).sum), x._4, x._3))
  }

  def calChurnRateAndPercentageForProfileMonth(array: Array[(String, String, String, Int, Int)], status: Int, month: String) = {
    val rs = array.filter(x => x._2.toInt == status)
    //rs.foreach(println)
    val sumByProfileMonth       = rs.map(x=> (x._1, x._3, x._5, CommonService.format2Decimal(x._5 * 100.0 / x._4)))
    val sumByProfileMonthStatus = array.groupBy(x=> x._1-> x._3).map(x=> (x._1._1,x._1._2, x._2.map(y=> y._5).sum))
    val sumAll = sumByProfileMonth.map(x=> (x._2, x._1, CommonService.format2Decimal(x._3 * 100.0 / sumByProfileMonthStatus.filter(y=> y._1 == x._1).filter(y=> y._2 == x._2)
      .map(y=> y._3).sum), x._4, x._3))
    val top8Profile = sumAll.filter(x=> x._2 == month).map(x=> (x._1, x._2, x._3, x._4, 2 * x._3 * x._4 * 0.01 / (x._3 + x._4)))
      .sortWith((x, y) => x._5 > y._5).slice(0, 8).map(x=> x._1)
    //sumAll.foreach(println)
    sumAll.filter(x=> top8Profile.indexOf(x._1) >= 0)
  }

  def getTop8ProfileByMonth(array: Array[(String, String, String, Int, Int)], status: Int, month: String) = {
    val rs = array.filter(x => x._2.toInt == status)
    //rs.foreach(println)
    val sumByProfileMonth       = rs.map(x=> (x._1, x._3, x._5, CommonService.format2Decimal(x._5 * 100.0 / x._4)))
    val sumByProfileMonthStatus = array.groupBy(x=> x._1-> x._3).map(x=> (x._1._1,x._1._2, x._2.map(y=> y._5).sum))
    val sumAll = sumByProfileMonth.map(x=> (x._2, x._1, CommonService.format2Decimal(x._3 * 100.0 / sumByProfileMonthStatus.filter(y=> y._1 == x._1).filter(y=> y._2 == x._2)
      .map(y=> y._3).sum), x._4, x._3))
    val top8Profile = sumAll.filter(x=> x._2 == month).map(x=> (x._1, x._2, x._3, x._4, 2 * x._3 * x._4 * 0.01 / (x._3 + x._4)))
      .sortWith((x, y) => x._5 > y._5).slice(0, 8).map(x=> x._1)
    top8Profile
  }

  def calChurnRateAndPercentageForProfileAge(array: Array[(String, String, String, Int, Int)], status: Int, top8Profile: Array[String]) = {
    val rs = array.filter(x => x._2.toInt == status)
    val sumByProfileAge       = rs.map(x=> (x._1, x._3, x._5, CommonService.format2Decimal(x._5 * 100.0 / rs.filter(y=> y._3 == x._3).map(y=> y._5).sum)))
    val sumByProfileAgeStatus = array.groupBy(x=> x._1 -> x._3).map(x=> (x._1._1,x._1._2, x._2.map(y=> y._5).sum))
    val sumAll = sumByProfileAge.map(x=> (x._2, x._1, CommonService.format2Decimal(x._3 * 100.0 / sumByProfileAgeStatus.filter(y=> y._1 == x._1).filter(y=> y._2 == x._2)
      .map(y=> y._3).sum), x._4, x._3))
    sumAll.filter(x=> top8Profile.indexOf(x._1) >= 0)
  }

  def calNumberofContractTopProfile(array:Array[(String, String, String, Int, Int)], mapProfile: Map[String, Int]) = {
     /*val mapProfile = (0 until profiles.length).map(x=> profiles(x) -> x).sorted.toMap*/
     val mapAge     = AgeGroupUtil.AGE_INDEX
     val groupAgeProfile = array.groupBy(x=> x._1 -> x._3).map(x=> (x._1._1.toInt, x._1._2, x._2.map(y=> y._5).sum))
     groupAgeProfile.filter(x=> mapProfile.map(y=> y._1).toArray.indexOf(x._2) >= 0).map(x=> (mapAge.get(x._1).get, mapProfile.get(x._2).get - 1, x._3)).toArray.sortWith((x, y) => x._1 < y._1)
  }

  def getInternet(request: Request[AnyContent]) = {
    logger.info("========START REGION SERVICE=========")
    var status = 1 // Huy dich vu default
    var age = "age:*"
    var region = "*"
    var profile = "*"
    var changeDate = "*"
    var month = CommonService.getPrevMonth()
    if(request != null) {
      status = request.body.asFormUrlEncoded.get("status").head.toInt
      age = if(request.body.asFormUrlEncoded.get("age").head != "") AgeGroupUtil.getCalAgeByName(request.body.asFormUrlEncoded.get("age").head) else "age:*"
      region = if(request.body.asFormUrlEncoded.get("region").head != "") request.body.asFormUrlEncoded.get("region").head else "*"
      profile = if(request.body.asFormUrlEncoded.get("profile").head != "") "\""+request.body.asFormUrlEncoded.get("profile").head +"\"" else "*"
      month = request.body.asFormUrlEncoded.get("month").head
    }
    val stpChangeDate = "["+new DateTime(month+"-01").getMillis+" TO "+new DateTime(CommonService.getCurrentMonth()+"-01").getMillis+"]"
    println("timestamp:"+stpChangeDate)
    if(status == 4 || status == 4 || status == 6) changeDate = s"change_date:$stpChangeDate"
    val t0 = System.currentTimeMillis()
    // region and month trends
    val trendRegionMonth = calChurnRateAndPercentageForRegionMonth(getTrendRegionMonth(s"$age AND $changeDate", profile), status).filter(x=> x._2 != CommonService.getCurrentMonth()).sorted
    val top12monthRegion = trendRegionMonth.map(x=> x._2).distinct.sortWith((x, y) => x > y).filter(x=> x != CommonService.getCurrentMonth()).slice(0,12).sorted
    val topMonth = if(top12monthRegion.length >= 12) 13 else top12monthRegion.length+1
    val mapMonthRegion   = if(top12monthRegion.length >0) (1 until topMonth).map(x=> top12monthRegion(x-1) -> x).toMap else Map[String, Int]()
    val rsRegion    = if(top12monthRegion.length >0) trendRegionMonth.filter(x=> top12monthRegion.indexOf(x._2) >=0).map(x=> (x._1, mapMonthRegion.get(x._2).get, x._3, x._4, x._5))
                           else Array[(Int, Int, Double, Double, Int)]()
    /*var arrGroup = Array[(Int, Int)]()
    val mapRegion = CommonUtil.REGION.map(x=> x._2).toArray
    val mapMonths = mapMonthRegion.map(x=> x._2).toArray
    val arrEmpty = Array((0.0, 0.0, 0))
    for(i <- 0 until mapRegion.size){
      for(j <- 0 until mapMonths.size){
        arrGroup :+= (mapRegion(i) -> mapMonths(j))
      }
    }
    val rsRegion = arrGroup.map(x=> (x._1, x._2, if(rsRegionMonth.filter(y=> y._1 == x._1).filter(y=> y._2 == x._2).length ==0) arrEmpty else rsRegionMonth.filter(y=> y._1 == x._1).filter(y=> y._2 == x._2)
      .map(x=> (x._3, x._4, x._5)))).flatMap(x=> x._3.map(y=> (x._1, x._2) -> y))
      .map(x=> (x._1._1, x._1._2, x._2._1, x._2._2, x._2._3))*/
    logger.info("t0: "+(System.currentTimeMillis()- t0))
    val t1 = System.currentTimeMillis()
    // churn region
    val churnRegion      = calChurnRateAndPercentageByStatus(getChurnGroupbyStatusRegion(s"month:$month AND $age AND $changeDate", profile), status, "region")
    logger.info("t1: "+(System.currentTimeMillis()- t1))
    val t2 = System.currentTimeMillis()
    // churn profile
    val churnProfile     = calChurnRateAndPercentageByStatus(getChurnGroupbyStatusProfile(s"month:$month AND $age AND region:$region AND $changeDate"), status, "profile")
      .map(x=> (x._1, x._2, x._3, 2 * x._2 * x._3 * 0.01/(x._2 + x._3))).sortWith((x, y) => x._4 > y._4)
      .slice(0, 8).map(x=> (x._1, x._2, x._3))
    logger.info("t2: "+(System.currentTimeMillis()- t2))
    val t3 = System.currentTimeMillis()
    // profile and month trends
    val rsProMonth = getTrendProfileMonth(s"$age AND region:$region AND $changeDate")
    val trendProfileMonth = calChurnRateAndPercentageForProfileMonth(rsProMonth, status, month).filter(x=> x._2 != CommonService.getCurrentMonth()).sorted
    //rsProMonth.foreach(println)
        /* get top profile*/
    val top8Profile = getTop8ProfileByMonth(rsProMonth, status, month)
    val sizeTopProfile = if(top8Profile.length >= 8 ) 9 else top8Profile.length +1
    val mapProfileMonth = (1 until sizeTopProfile).map(x=> top8Profile(sizeTopProfile-x-1) -> x).toMap
        /* get top month*/
    val topLast12month = trendProfileMonth.map(x=> x._2).distinct.sortWith((x, y) => x > y).filter(x=> x != CommonService.getCurrentMonth()).slice(0,12).sorted
    val topMonthProf = if(topLast12month.length >= 12) 13 else topLast12month.length+1
    val mapMonth   = (1 until topMonthProf).map(x=> topLast12month(x-1) -> x).toMap
    val rsProfileMonth = trendProfileMonth.filter(x=> topLast12month.indexOf(x._2) >=0).map(x=> (mapProfileMonth.get(x._1).get, mapMonth.get(x._2).get, x._3, x._4, x._5))
    logger.info("t3: "+(System.currentTimeMillis()- t3))
    val t4 = System.currentTimeMillis()
    // profile and age trends
    val trendAgeProfile = calChurnRateAndPercentageForProfileAge(getTrendAgeProfile(month, s"region:$region AND $changeDate"), status, mapProfileMonth.map(x=> x._1).toArray)
    val rsProfileAge    = trendAgeProfile.map(x=> (mapProfileMonth.get(x._1).get, x._2.toInt, x._3, x._4, x._5))
    var arrGroup = Array[(Int, Int)]()
    val arrEmptyProf = Array((0.0 ,0.0, 0))
    val arrAge = AgeGroupUtil.AGE.map(x=> x._2).toArray
    val arrProfiles = mapProfileMonth.map(x=> x._2).toArray
    for(i <- 0 until arrProfiles.size){
      for(j <- 0 until arrAge.size){
        arrGroup :+= (arrProfiles(i) -> arrAge(j))
      }
    }
    val arrProfileAge = arrGroup.map(x=> (x._1, x._2, if(rsProfileAge.filter(y=> y._1 == x._1).filter(y=> y._2 == x._2).length ==0) arrEmptyProf else rsProfileAge.filter(y=> y._1 == x._1).filter(y=> y._2 == x._2)
      .map(x=> (x._3, x._4, x._5)))).flatMap(x=> x._3.map(y=> (x._1, x._2) -> y))
      .map(x=> (x._1._1, x._1._2, x._2._1, x._2._2,x._2._3))
    logger.info("t4: "+(System.currentTimeMillis()- t4))
    val t5 = System.currentTimeMillis()
    // number of contracts top8 profile
    val contractProfiles = calNumberofContractTopProfile(getTrendAgeProfile(month, s"region:$region AND $changeDate"), mapProfileMonth)
    val xAxisAgeGroup    = AgeGroupUtil.AGE_INDEX.map(x=> AgeGroupUtil.getAgeById(x._1)).toArray.sorted
    val yAxisProfile     = mapProfileMonth.toArray.sortWith((x, y) => x._2 < y._2)

    logger.info("t5: "+(System.currentTimeMillis()- t5))
    logger.info("Time: "+(System.currentTimeMillis()- t0))
    logger.info("========END REGION SERVICE=========")
    RegionResponse(churnRegion, (mapMonthRegion, rsRegion), churnProfile, (mapProfileMonth, mapMonth, rsProfileMonth), (mapProfileMonth, arrProfileAge), (xAxisAgeGroup, yAxisProfile, contractProfiles))
  }
}