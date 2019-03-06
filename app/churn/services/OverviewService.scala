package service

import churn.models.{RateNumber, ContractNumber, OverviewResponse}
import com.sksamuel.elastic4s.http.ElasticDsl.{RichFuture, RichString, SearchHttpExecutable, SearchShow, percentilesAggregation, query, rangeAggregation, search, termsAgg, termsAggregation, _}
import com.sksamuel.elastic4s.http.search.SearchResponse
import churn.utils.{CommonUtil, ProvinceUtil}
import play.api.Logger
import play.api.mvc.{AnyContent, Request}
import services.Configure
import services.domain.CommonService

object OverviewService{

  val client = Configure.client

  val logger = Logger(this.getClass())

  def rangeMonth(month: String) = CommonService.getRangeDateByLimit(month, 12, "month")

  def checkExistsIndex(name: String) = {
    val isExists = client.execute{
      indexExists(name)
    }.await
    isExists.isExists
  }

  private def getRegionProvince(month: String) = {
    val request = search(s"churn-contract-info-${month}" / "docs") aggregations (
      termsAggregation("region")
        .field("region")
        .subaggs(
          termsAgg("province", "province")) size 1000) size 0
    val response = client.execute(request).await
    CommonService.getTerm1(response, "region", "province").map(x=> (x._1, ProvinceUtil.getProvince(x._2.toInt))).sorted
  }

  private def getListPackage(month: String) = {
    val request = search(s"churn-contract-info-${month}" / "docs") aggregations (
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

  private def calRateAndPercentByMonth(array: Array[(String, String, Long)], status: Int) = {
    val fitlerArray = status match {
      case 1  => array.filter(x=> x._2.toInt == 1)
      case 3  => array.filter(x=> x._2.toInt == 3)
      case 13 => array.filter(x=> x._2.toInt == 1 ||  x._2.toInt == 3)
    }
    val sumAll = fitlerArray.map(x=> x._3).sum
    val sumByStatus    = fitlerArray.groupBy(x=> x._1).map(x=> x._1 -> x._2.map(y=> y._3).sum)
    val sumByStatusAll = array.groupBy(x=> x._1).map(x=>x._1 -> x._2.map(y=> y._3).sum)
    // month, rate, percentage
    sumByStatus.map(x=> (x._1, x._2, sumByStatusAll.filter(y=> y._1 == x._1).map(x=> x._2).sum))
      .map(x => (x._1, CommonService.format3Decimal(x._2  * 100.0/ x._3), CommonService.format3Decimal(x._2 * 100.0 / sumAll)) )
        .map(x=> (x._1, x._2, x._3, CommonService.format3Decimal(2* x._2 * x._3 /(x._2 + x._3)))).toArray.sorted
  }

  private def calChurnRatebyMonth(array: Array[(String, String, Long)], status: Int) = {
    val allStatus = array.groupBy(x=> x._1).map(x=> x._1 -> x._2.map(y=> y._3).sum).toMap
    val rs = if(status != 13) array.filter(x=> x._2.toInt == status).map(x=> x._1 -> x._3)
    else array.filter(x=> x._2.toInt == 1 || x._2.toInt == 3).groupBy(x=> x._1).map(x=> x._1 -> x._2.map(y=> y._3).sum).toArray
    rs.map(x=> x._1 -> CommonService.format3Decimal(x._2 * 100.0 / allStatus.get(x._1).get))
  }

  private def getFilterGroup(age:String, region:String, province:String, packages:String, combo:String, lstProvince: Array[(String, String)]): String = {
    val ageFilter = if(age == "") "*" else age.split(",").map(x=> if(x == "06-12") "6-12" else x).map(x=> "lifeGroup:"+x.trim()).mkString(" OR ")
    val packageFilter = if(packages == "") "*" else packages.split(",").map(x=> "package_name:\""+x.trim()+"\"").mkString(" OR ")
    // filter combo group
    val comboFitler = combo match {
      case "int"   => CommonUtil.filterCommon("package_name") + " AND int_info:have_info AND !(ptv_info:have_info)"
      case "combo" => CommonUtil.filterCommon("package_name") + " AND int_info:have_info AND ptv_info:have_info"
      case _       => CommonUtil.filterCommon("package_name")
    }
    // filter Region or Province
    val arrRegion = region.split(",").map(x=> x.trim() -> lstProvince.filter(y=> y._1 == x.trim())).flatMap(x=> x._2.map(y=> x._1->y)).map(x=> x._2._1 -> x._2._2)
    val provinces = province.split(",").map(x=> x.trim())
    val diffProvince = provinces diff arrRegion.map(x=> x._2)
    val regionFilter = if(region == "") "*" else arrRegion.map(x=> x._1).distinct.map(x=> s"region:$x").mkString(" OR ")
    var provinceFilter = diffProvince.filter(x=> x != "" && x != "All").map(x=> "province:"+ProvinceUtil.getProvinceCode(x)).mkString(" OR ")
    if(provinceFilter != "") provinceFilter = s"OR $provinceFilter"
    return s"($ageFilter) AND ($packageFilter) AND ($regionFilter $provinceFilter) AND ($comboFitler)"
  }

  def getInternet(request: Request[AnyContent]) = {
    logger.info("========START OVERVIEW SERVICE=========")
    val t0 = System.currentTimeMillis()
    var age = "*"
    var region = "*"
    var province = "*"
    var combo = "*"
    var packages = "*"
    var month = CommonService.getPrevMonth()
    var queries = CommonUtil.filterCommon("package_name")

    if(!checkExistsIndex(s"churn-contract-info-$month")) month = CommonService.getPrevMonth(2)

    // get list province of region for filter
    val lstProvince = getRegionProvince(month)
    // get list package for filter
    val lstPackage = getListPackage(month).sortWith((x, y) => x._2 > y._2).map(x=> x._1)

    if(request != null) {
      age = request.body.asFormUrlEncoded.get("age").head
      region = request.body.asFormUrlEncoded.get("region").head
      province = request.body.asFormUrlEncoded.get("province").head
      packages = request.body.asFormUrlEncoded.get("package").head
      combo = request.body.asFormUrlEncoded.get("combo").head
      month = request.body.asFormUrlEncoded.get("month").head
      queries = getFilterGroup(age, region, province, packages, combo, lstProvince)
      println(queries)
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
    val avgRateHuydv  = CommonService.format3Decimal(calChurnRatebyMonth(ctMonthStatus, 1).map(x=> x._2).sum / calChurnRatebyMonth(ctMonthStatus, 1).length)
    val currRateHuydv = calChurnRatebyMonth(ctMonthStatus, 1).filter(x=> x._1 == month).toMap.get(month).get
    val avgRateCTBDV  = CommonService.format3Decimal(calChurnRatebyMonth(ctMonthStatus, 3).map(x=> x._2).sum / calChurnRatebyMonth(ctMonthStatus, 3).length)
    val currRateCTBDV = calChurnRatebyMonth(ctMonthStatus, 3).filter(x=> x._1 == month).toMap.get(month).get
    val avgRateAll    = CommonService.format3Decimal(calChurnRatebyMonth(ctMonthStatus, 13).map(x=> x._2).sum / calChurnRatebyMonth(ctMonthStatus, 13).length)
    val currRateAll   = calChurnRatebyMonth(ctMonthStatus, 13).filter(x=> x._1 == month).toMap.get(month).get
    logger.info("t2: "+(System.currentTimeMillis() - t2))
    val t3 = System.currentTimeMillis()

    // Chart trend Number Contract by month
    val numOfMonth    = getContractChurnRate(month, "month", queries).filter(x=> x._2.toInt == 1 || x._2.toInt == 3).sorted
    logger.info("t4: "+(System.currentTimeMillis() - t3))
    val t4 = System.currentTimeMillis()

    // Churn Rate & Percent HUYDV and CTBDV
    val trendRatePert = calRateAndPercentByMonth(getContractChurnRate(month, "month", queries), 13)
    val huyDvRatePert = calRateAndPercentByMonth(getContractChurnRate(month, "month", queries), 1)
    val ctbdvRatePert = calRateAndPercentByMonth(getContractChurnRate(month, "month", queries), 3)
    logger.info("t5: "+(System.currentTimeMillis() - t4))

    logger.info("Time: "+(System.currentTimeMillis() - t0))
    logger.info("========END OVERVIEW SERVICE=========")
    OverviewResponse((ContractNumber(currActive, prevActive), ContractNumber(currHuydv, prevHuydv), ContractNumber(currCtbdv, prevCtbdv)),
      (RateNumber(avgRateHuydv, currRateHuydv), RateNumber(avgRateCTBDV, currRateCTBDV), RateNumber(avgRateAll, currRateAll)), numOfMonth, (trendRatePert, huyDvRatePert, ctbdvRatePert),
      lstProvince, lstPackage, month)
  }
}