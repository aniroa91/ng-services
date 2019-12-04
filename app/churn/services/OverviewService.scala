package service

import churn.models.{ContractNumber, OverviewResponse, RateNumber}
import com.sksamuel.elastic4s.http.ElasticDsl.{RichFuture, RichString, SearchHttpExecutable, SearchShow, percentilesAggregation, query, rangeAggregation, search, termsAgg, termsAggregation, _}
import com.sksamuel.elastic4s.http.search.SearchResponse
import churn.utils.{CommonUtil, ProvinceUtil}
import com.ftel.bigdata.utils.DateTimeUtil
import org.elasticsearch.action.support.WriteRequest.RefreshPolicy
import org.joda.time.DateTime
import org.joda.time.format.DateTimeFormat
import play.api.Logger
import play.api.mvc.{AnyContent, Request}
import services.Configure
import services.domain.CommonService

import scala.collection.immutable.Map

object OverviewService{

  val client = Configure.client

  val logger = Logger(this.getClass())

  def rangeMonth(month: String) = CommonService.getRangeDateByLimit(month, 15, "month")

  def checkExistsIndex(name: String) = {
    val isExists = client.execute{
      indexExists(name)
    }.await
    isExists.isExists
  }

  def getRegionProvince(month: String) = {
    val request = search(s"churn-contract-info-${month}" / "docs") aggregations (
      termsAggregation("region")
        .field("region")
        .subaggs(
          termsAgg("province", "province") size 100) size 1000) size 0
    val response = client.execute(request).await
    CommonService.getTerm1(response, "region", "province").map(x=> (x._1, ProvinceUtil.getProvince(x._2.toInt).toLowerCase)).sorted
  }

  def getListPackage(month: String) = {
    val request = search(s"churn-contract-info-${month}" / "docs") query(CommonUtil.filterCommon("package_name")) aggregations (
      termsAggregation("package_name")
        .field("package_name") size 1000
      ) size 0
    val response = client.execute(request).await
    CommonService.getAggregationsSiglog(response.aggregations.get("package_name")).sorted
  }

  private def getContractByStatus(month: String, status: Int, commonFilter: String) = {
    if(!checkExistsIndex(s"churn-contract-info-$month")){
      0
    }
    else{
      val indexs = if(status == 0) s"churn-contract-info-$month" else "churn-contract-info-*"
      val queryStr = if(status == 0) s"status:$status" else s"status:$status AND month:<=$month AND month:>="+month.substring(0, month.indexOf("-"))+"-01"
      val request = search(indexs / "docs") query(queryStr +" AND "+commonFilter) size 0
      val response = client.execute(request).await
      response.hits.total
    }
  }

  def getTrendRegionMonth(month: String, queries: String, province: String) ={
    val location = checkLocation(province)
    val request = search(s"churn-contract-info-*" / "docs") query(rangeMonth(month) +" AND "+ queries) aggregations (
      termsAggregation("month")
        .field("month")
        .subaggs(
          termsAggregation("status")
            .field("status")
            .subaggs(
              termsAggregation(s"$location")
                .field(s"$location") size 1000
            ) size 1000
        ) size 15
      ) size 0
    val rs = client.execute(request).await
    CommonUtil.getChurnRateAndPercentage(rs,"month","status" , s"$location").map(x=> (x._1, x._2, x._3, x._5, x._6))
  }

  private def getContractChurnRate(month: String, _type: String, commonFilter: String) = {
    val monthQuery = if(_type == "rate") s"month:<=$month AND month:>="+month.substring(0, month.indexOf("-"))+"-01" else rangeMonth(month)
    val queryStr = s"$monthQuery AND "+commonFilter
    val request = search("churn-contract-info-*" / "docs") query(queryStr) aggregations (
      termsAggregation("month")
        .field("month")
        .subaggs(
          termsAgg("status", "status")) size 1000) size 0
    val response = client.execute(request).await
    CommonService.getTerm1(response, "month", "status")
  }

  private def calRateAndPercentByMonth(array: Array[(String, String, Long)], arrayAll: Array[(String, String, Long)], status: String) = {
    var fitlerArray = new Array[(String, String , Long)](0)
    var arrPercent  = new Array[(String, String , Long)](0)
    status match {
      case "1"  => {
        fitlerArray = array.filter(x=> x._2.toInt == 1)
        arrPercent  = arrayAll.filter(x=> x._2.toInt == 1)
      }
      case "3" => {
        fitlerArray = array.filter(x=> x._2.toInt == 3)
        arrPercent  = arrayAll.filter(x=> x._2.toInt == 3)
      }
      case _ => {
        fitlerArray = array.filter(x=> x._2.toInt == 1 ||  x._2.toInt == 3)
        arrPercent  = arrayAll.filter(x=> x._2.toInt == 1 ||  x._2.toInt == 3)
      }
    }
    val sumByStatus    = fitlerArray.groupBy(x=> x._1).map(x=> x._1 -> x._2.map(y=> y._3).sum)
    val sumByStatusAll = array.groupBy(x=> x._1).map(x=>x._1 -> x._2.map(y=> y._3).sum)
    // month, rate, percentage
    sumByStatus.map(x=> (x._1, x._2, sumByStatusAll.filter(y=> y._1 == x._1).map(x=> x._2).sum, arrPercent.filter(y=> y._1 == x._1).map(x=> x._3).sum))
      .map(x => (x._1, CommonService.format3Decimal(x._2  * 100.0/ x._3), CommonService.format3Decimal(x._2 * 100.0 / x._4)) )
        .map(x=> (x._1, x._2, x._3, CommonService.format3Decimal(2* x._2 * x._3 /(x._2 + x._3)))).toArray.sorted
  }

  private def calChurnRatebyMonth(array: Array[(String, String, Long)], status: Int) = {
    val allStatus = array.groupBy(x => x._1).map(x => x._1 -> x._2.map(y => y._3).sum).toMap
    val rs = if (status != 13) array.filter(x => x._2.toInt == status).map(x => x._1 -> x._3)
    else array.filter(x => x._2.toInt == 1 || x._2.toInt == 3).groupBy(x => x._1).map(x => x._1 -> x._2.map(y => y._3).sum).toArray
    rs.map(x => x._1 -> CommonService.format3Decimal(x._2 * 100.0 / allStatus.get(x._1).get))
  }

  def calChurnRateAndPercentageForRegionMonth(array: Array[(String, String, String, Int, Int)], status: String, province: String) = {
    val rsArr = if(status == "" || status == "13") array.filter(x=> x._2.toInt == 1 || x._2.toInt == 3).groupBy(x=> x._1 -> x._3).map(x=> (x._1._1, 111, x._1._2, x._2.map(y=> y._4).sum, x._2.map(y=> y._5).sum)).toArray.sorted
                else array.filter(x => x._2.toInt == status.toInt).sorted
    val sumByRegionMonth       = rsArr.map(x=> (x._1, x._3, x._5, CommonService.format2Decimal(x._5 * 100.0 / x._4)))
    val sumByRegionMonthStatus = array.groupBy(x=> x._1-> x._3).map(x=> (x._1._1, x._1._2, x._2.map(y=> y._5).sum)).toArray
    val res = sumByRegionMonth.map(x=> (x._2, x._1, CommonService.format2Decimal(x._3 * 100.0 / sumByRegionMonthStatus.filter(y=> y._1 == x._1).filter(y=> y._2 == x._2)
      .map(y=> y._3).sum), x._4, x._3))
    val intRegex = """(\d+)""".r
    province match {
      case ""   => res.map(x=> ("Vung "+x._1, x._2, x._3, x._4, x._5))
      case intRegex(province) => res.map(x=> (CommonService.toTitleCase(ProvinceUtil.getProvince(x._1.toInt)), x._2, x._3, x._4, x._5))
      case _       => res
    }
  }

  def getFilterGroup(age:String, province:String, packages:String, combo:String): String = {
    val ageFilter = if(age == "") "*" else age.split(",").map(x=> if(x == "06-12") "6-12" else x).map(x=> "lifeGroup:"+x.trim()).mkString(" OR ")
    val packageFilter = if(packages == "") "*" else packages.split(",").map(x=> "package_name:\""+x.trim()+"\"").mkString(" OR ")
    // filter combo group
    val comboFitler = combo match {
      case "int"   => CommonUtil.filterCommon("package_name") + " AND int_info:have_info AND !(ptv_info:have_info)"
      case "combo" => CommonUtil.filterCommon("package_name") + " AND int_info:have_info AND ptv_info:have_info"
      case _       => CommonUtil.filterCommon("package_name")
    }
    val intRegex = """(\d+)""".r
    val filterCN = province match {
      case ""   => "*"
      case intRegex(province) => s"region:$province"
      case _       => "province:"+ProvinceUtil.getProvinceCode(province.toUpperCase())
    }
    s"($ageFilter) AND ($packageFilter) AND $filterCN AND ($comboFitler)"
  }

  def getTop10OltByMonth(array: Array[(String, String, String, Int, Int)], status: String, month: String, province: String, sortBy: String) = {
    val limit = if(checkLocation(province) == "olt_name") 10 else array.map(x=> x._3).distinct.length
    val rs = if(status == "" || status == "13") array.filter(x=> x._2.toInt == 1 || x._2.toInt == 3).groupBy(x=> x._1 -> x._3).map(x=> (x._1._1, 111, x._1._2, x._2.map(y=> y._4).sum, x._2.map(y=> y._5).sum)).toArray.sorted
             else array.filter(x => x._2.toInt == status.toInt).sorted
    val sumByOltMonth       = rs.map(x=> (x._1, x._3, x._5, CommonService.format2Decimal(x._5 * 100.0 / x._4)))
    val sumByOltMonthStatus = array.groupBy(x=> x._1-> x._3).map(x=> (x._1._1,x._1._2, x._2.map(y=> y._5).sum))
    val sumAll = sumByOltMonth.map(x=> (x._2, x._1, CommonService.format2Decimal(x._3 * 100.0 / sumByOltMonthStatus.filter(y=> y._1 == x._1).filter(y=> y._2 == x._2)
      .map(y=> y._3).sum), x._4, x._3))
    val top10Olt = sumAll.filter(x=> x._2 == month).map(x=> (x._1, x._2, x._3, x._4, 2 * x._3 * x._4 * 0.01 / (x._3 + x._4)))
    val sortByTop = if(sortBy == "f1") top10Olt.sortWith((x, y) => x._5 > y._5).slice(0, limit).map(x=> x._1)
                    else top10Olt.sortWith((x, y) => x._4 > y._4).slice(0, limit).map(x=> x._1)
    val intRegex = """(\d+)""".r
    province match {
      case ""   => sortByTop.map(x=> "Vung "+x)
      case intRegex(province) => sortByTop.map(x=> (CommonService.toTitleCase(ProvinceUtil.getProvince(x.toInt))))
      case _       => sortByTop
    }
  }

  def checkLocation(location: String) = {
    val intRegex = """(\d+)""".r
    val filterCN = location match {
      case ""   => "region"
      case intRegex(location) => s"province"
      case _       => "olt_name"
    }
    filterCN
  }

  def getCommentChart(user: String, pageId: String) = {
    val comments = CommonService.getCommentByUser(user, pageId)
    val cmtChart = if(comments.length >0) comments.sortWith((x, y) => x._1 > y._1)(0)._2 else ""
    cmtChart
  }

  def getInternet(user: String, request: Request[AnyContent]) = {
    logger.info("========START OVERVIEW SERVICE=========")
    val t0 = System.currentTimeMillis()
    var age = "*"
    var province = ""
    var combo = "*"
    var packages = "*"
    var status = ""
    var month = CommonService.getCurrentMonth()
    var queries = CommonUtil.filterCommon("package_name")
    if(!checkExistsIndex(s"churn-contract-info-$month")) month = CommonService.getPrevMonth(2)

    // get list province of region for filter
    val lstProvince = getRegionProvince(CommonService.getPrevMonth(2))
    // get list package for filter
    val lstPackage = getListPackage(month).sortWith((x, y) => x._2 > y._2).map(x=> x._1)

    if(request != null) {
      age = request.body.asFormUrlEncoded.get("age").head
      province = request.body.asFormUrlEncoded.get("province").head
      packages = request.body.asFormUrlEncoded.get("package").head
      combo = request.body.asFormUrlEncoded.get("combo").head
      month = request.body.asFormUrlEncoded.get("month").head
      status = request.body.asFormUrlEncoded.get("status").head
      queries = getFilterGroup(age, province, packages, combo)
      //println(queries)
    }
    logger.info("t0: "+(System.currentTimeMillis() - t0))
    val t1 = System.currentTimeMillis()

    // get contract by status
    val currActive = getContractByStatus(month, 0, queries)
    val currHuydv  = getContractByStatus(month, 1, queries)
    val currCtbdv  = getContractByStatus(month, 3, queries)
    val prevActive = getContractByStatus(CommonService.getPrevYYYYMM(month), 0, queries)
    val prevHuydv  = getContractByStatus(CommonService.getPrevYYYYMM(month), 1, queries)
    val prevCtbdv  = getContractByStatus(CommonService.getPrevYYYYMM(month), 3, queries)
    logger.info("t1: "+(System.currentTimeMillis() - t1))
    val t2 = System.currentTimeMillis()

    // calculate churn rate for Status HUYDV + CTBDV(1 vs 3)
    val ctMonthStatus = getContractChurnRate(month, "rate", queries)
    val avgRateHuydv  = if(calChurnRatebyMonth(ctMonthStatus, 1).length > 0) CommonService.format3Decimal(calChurnRatebyMonth(ctMonthStatus, 1).map(x=> x._2).sum / calChurnRatebyMonth(ctMonthStatus, 1).length)
                        else 0
    val currRateHuydv = calChurnRatebyMonth(ctMonthStatus, 1).filter(x=> x._1 == month).toMap.get(month).getOrElse(0.0)
    val avgRateCTBDV  = if(calChurnRatebyMonth(ctMonthStatus, 3).length > 0) CommonService.format3Decimal(calChurnRatebyMonth(ctMonthStatus, 3).map(x=> x._2).sum / calChurnRatebyMonth(ctMonthStatus, 3).length)
                        else 0
    val currRateCTBDV = calChurnRatebyMonth(ctMonthStatus, 3).filter(x=> x._1 == month).toMap.get(month).getOrElse(0.0)
    val avgRateAll    = if(calChurnRatebyMonth(ctMonthStatus, 13).length > 0) CommonService.format3Decimal(calChurnRatebyMonth(ctMonthStatus, 13).map(x=> x._2).sum / calChurnRatebyMonth(ctMonthStatus, 13).length)
                        else 0
    val currRateAll   = calChurnRatebyMonth(ctMonthStatus, 13).filter(x=> x._1 == month).toMap.get(month).getOrElse(0.0)
    logger.info("t2: "+(System.currentTimeMillis() - t2))
    val t3 = System.currentTimeMillis()

    val rsStatus = if(status == "" || status == "13") " AND (status:1 OR status:3)" else s" AND (status:$status)"
    // Chart trend Number Contract by month
    val numOfMonth    = getContractChurnRate(month, "month", queries+rsStatus).sorted
    logger.info("t3: "+(System.currentTimeMillis() - t3))
    val t4 = System.currentTimeMillis()

    // Churn Rate & Percent HUYDV and CTBDV
    val trendRatePert = calRateAndPercentByMonth(getContractChurnRate(month, "month", queries), getContractChurnRate(month, "month", CommonUtil.filterCommon("package_name")), status)
    logger.info("t4: "+(System.currentTimeMillis() - t4))
    val t5 = System.currentTimeMillis()

    // Trend region and month
    val arrMonth = getTrendRegionMonth(month, queries, province)
    val trendRegionMonth = calChurnRateAndPercentageForRegionMonth(arrMonth, status, province).sorted
    /* get top location */
    val topLocationByPert = getTop10OltByMonth(arrMonth, status, month, province, "percent")
    val sizeTopLocation = topLocationByPert.length +1
    val mapRegionMonth = (1 until sizeTopLocation).map(x=> topLocationByPert(sizeTopLocation-x-1) -> x).toMap
    /* get top month */
    val topLast12month = trendRegionMonth.map(x=> x._2).distinct.sortWith((x, y) => x > y).slice(0,15).sorted
    val topMonthRegion = if(topLast12month.length >= 15) 16 else topLast12month.length+1
    val mapMonth   = (1 until topMonthRegion).map(x=> topLast12month(x-1) -> x).toMap
    val rsRegionMonth = trendRegionMonth.filter(x=> topLast12month.indexOf(x._2) >=0).filter(x=> topLocationByPert.indexOf(x._1) >=0)
      .map(x=> (mapRegionMonth.get(x._1).get, mapMonth.get(x._2).get, x._3, x._4, x._5))

    logger.info("t5: "+(System.currentTimeMillis() - t5))
    val t6 = System.currentTimeMillis()
    // sparkline table
    val topLocationByF1 = getTop10OltByMonth(arrMonth, status, month, province, "f1")
    val tbChurn = trendRegionMonth.filter(x=> topLast12month.indexOf(x._2) >=0).filter(x=> topLocationByF1.indexOf(x._1) >=0).map(x=> (x._1, x._2, x._3,
      CommonService.format2Decimal(2*x._3*x._4/(x._3+x._4)), x._5))
    logger.info("t6: "+(System.currentTimeMillis() - t6))

    // comments content
    val cmtChart = getCommentChart(user, CommonUtil.PAGE_ID.get(0).get+"_tabOverview")

    logger.info("Time: "+(System.currentTimeMillis() - t0))
    logger.info("========END OVERVIEW SERVICE=========")
    OverviewResponse((ContractNumber(currActive, prevActive), ContractNumber(currHuydv, prevHuydv), ContractNumber(currCtbdv, prevCtbdv)),
      (RateNumber(avgRateHuydv, currRateHuydv), RateNumber(avgRateCTBDV, currRateCTBDV), RateNumber(avgRateAll, currRateAll)), numOfMonth, trendRatePert,
      lstProvince, lstPackage, (mapRegionMonth, mapMonth,rsRegionMonth), (topLocationByF1, tbChurn), cmtChart, month)
  }
}