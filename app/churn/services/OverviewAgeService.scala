package service

import churn.models.AgeResponse
import com.sksamuel.elastic4s.http.ElasticDsl.{RichFuture, RichString, SearchHttpExecutable, SearchShow, percentilesAggregation, query, rangeAggregation, search, termsAgg, termsAggregation, _}
import churn.utils.{AgeGroupUtil, CommonUtil, ProvinceUtil}
import play.api.Logger
import play.api.mvc.{AnyContent, Request}
import service.OverviewService.{checkLocation, getCommentChart}
import services.Configure
import services.domain.CommonService

object OverviewAgeService{

  val client = Configure.client

  val logger = Logger(this.getClass())

  def calChurnRateAndPercentForAgeMonth(array: Array[(String, String, String, Int, Int)], status: String, province: String) = {
    val rsArr = if(status == "" || status == "13") array.filter(x=> x._2.toInt == 1 || x._2.toInt == 3).groupBy(x=> x._1 -> x._3).map(x=> (x._1._1, 111, x._1._2, x._2.map(y=> y._4).sum, x._2.map(y=> y._5).sum)).toArray.sorted
    else array.filter(x => x._2.toInt == status.toInt).sorted
    val sumByRegionMonth       = rsArr.map(x=> (x._1, x._3, x._5, CommonService.format2Decimal(x._5 * 100.0 / x._4)))
    val sumByRegionMonthStatus = array.groupBy(x=> x._1-> x._3).map(x=> (x._1._1, x._1._2, x._2.map(y=> y._5).sum)).filter(x=> x._3 != 0).filter(x=> x._3 !=0).toArray
    val res = sumByRegionMonth.map(x=> (x._2, x._1, x._3 * 100.0 / sumByRegionMonthStatus.filter(y=> y._1 == x._1).filter(y=> y._2 == x._2)
      .map(y=> y._3).sum, x._4, x._3)).filter(x=> x._5 != 0).map(x=> (x._1, x._2, CommonService.format2Decimal(x._3), x._4, x._5))
    val intRegex = """(\d+)""".r
    province match {
      case ""   => res.map(x=> (x._1, "Vung "+x._2, x._3, x._4, x._5))
      case intRegex(province) => res.map(x=> (x._1, CommonService.toTitleCase(ProvinceUtil.getProvince(x._2.toInt)), x._3, x._4, x._5))
      case _       => res
    }
  }

  def getTrendAgeMonth(month: String, queries: String, province: String) ={
    val field = if(province == "No") "month" else checkLocation(province)
    val request = search(s"churn-contract-info-*" / "docs") query(OverviewService.rangeMonth(month) +" AND "+ queries) aggregations (
      termsAggregation(s"$field")
        .field(s"$field")
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
        ) size 15
      ) size 0
    val rs = client.execute(request).await
    CommonUtil.getChurnRateAndPercentage(rs,s"$field","status" , "age").map(x=> (x._1, x._2, AgeGroupUtil.AGE.find(_._2 == x._3.toInt).get._1, x._5, x._6))
  }

  def getTopAgeByMonth(array: Array[(String, String, String, Int, Int)], status: String, month: String, sortBy: String, limit: Int) = {
    val rs = if(status == "" || status == "13") array.filter(x=> x._2.toInt == 1 || x._2.toInt == 3).groupBy(x=> x._1 -> x._3).map(x=> (x._1._1, 111, x._1._2, x._2.map(y=> y._4).sum, x._2.map(y=> y._5).sum)).toArray.sorted
             else array.filter(x => x._2.toInt == status.toInt).sorted
    val sumByOltMonth       = rs.map(x=> (x._1, x._3, x._5, CommonService.format2Decimal(x._5 * 100.0 / x._4)))
    val sumByOltMonthStatus = array.groupBy(x=> x._1-> x._3).map(x=> (x._1._1, x._1._2, x._2.map(y=> y._5).sum))
    val sumAll = sumByOltMonth.map(x=> (x._2, x._1, x._3 * 100.0 / sumByOltMonthStatus.filter(y=> y._1 == x._1).filter(y=> y._2 == x._2)
      .map(y=> y._3).sum, x._4, x._3))
    val top10Olt = sumAll.filter(x=> x._2 == month).filter(x=> x._3 != 0).filter(x=> x._4 != 0).map(x=> (x._1, x._2, x._3, x._4, 2 * x._3 * x._4 * 0.01 / (x._3 + x._4)))
    val sortByTop = if(sortBy == "f1") top10Olt.sortWith((x, y) => x._5 > y._5).slice(0, limit).map(x=> x._1)
                    else top10Olt.sortWith((x, y) => x._4 > y._4).slice(0, limit).map(x=> x._1)
    sortByTop
  }

  def getTopOltByAge(rs: Array[(String, String, Double)]) = {
    val ageGroup = rs.map(x=> x._1).distinct
    val top10Olt = ageGroup.map(x=> rs.filter(y=> x == y._1).sortBy(y=> y._3) -> x).map(x=> x._1)
        .flatMap{
          x=> x.zipWithIndex.map(y=> (y._2, y._1._2, y._1._3))
        }.groupBy(x=> x._2).map(x=> x._1 -> x._2.map(y=> y._1).sum).toArray
    top10Olt.sortWith((x, y) => x._2 > y._2).map(x=> x._1).distinct.slice(0,10)
  }

  def getInternet(user: String, request: Request[AnyContent]) = {
    logger.info("========START AGE SERVICE=========")
    val t0 = System.currentTimeMillis()
    val age = ""
    val province = request.body.asFormUrlEncoded.get("province").head
    val packages = request.body.asFormUrlEncoded.get("package").head
    val combo = request.body.asFormUrlEncoded.get("combo").head
    val month = request.body.asFormUrlEncoded.get("month").head
    val status = request.body.asFormUrlEncoded.get("status").head
    // get list province of region for filter
    val lstProvince = OverviewService.getRegionProvince(month)
    val queries = OverviewService.getFilterGroup(age, province, packages, combo)
   // println(queries)
    logger.info("t0: "+(System.currentTimeMillis() - t0))
    val t1 = System.currentTimeMillis()
    // Trend Age and location
    val arrAgeLoc = getTrendAgeMonth(month, queries, province)
    val trendRegionAge = calChurnRateAndPercentForAgeMonth(arrAgeLoc, status, province).filter(x=> x._2 != CommonService.getCurrentMonth()).sorted
    /* get top location */
    val topLocationByPert = if(checkLocation(province) == "olt_name") getTopOltByAge(trendRegionAge.map(x=> (x._1, x._2, x._4))) else trendRegionAge.map(x=> x._2).distinct
    val sizeTopLocation = topLocationByPert.length +1
    val mapLocation = (1 until sizeTopLocation).map(x=> topLocationByPert(sizeTopLocation-x-1) -> x).toMap
    /* get top Age Location */
    val topAgeLocByPert = trendRegionAge.map(x=> x._1).distinct
    val sizeTopAgeLoc = topAgeLocByPert.length +1
    val mapAge = (1 until sizeTopAgeLoc).map(x=> topAgeLocByPert(sizeTopAgeLoc-x-1) -> x).toMap

    val rsLocationAge = trendRegionAge.filter(x=> topAgeLocByPert.indexOf(x._1) >=0).filter(x=> topLocationByPert.indexOf(x._2) >=0)
      .map(x=> (mapLocation.get(x._2).get, mapAge.get(x._1).get, x._3, x._4, x._5))
    logger.info("t1: "+(System.currentTimeMillis() - t1))
    val t2 = System.currentTimeMillis()

    // Trend Age and month
    val arrMonth = getTrendAgeMonth(month, queries, "No")
    val trendAgeMonth = calChurnRateAndPercentForAgeMonth(arrMonth, status, "No").filter(x=> x._2 != CommonService.getCurrentMonth()).sorted
    /* get top Age */
    val topAgeByPert = getTopAgeByMonth(arrMonth, status, month, "percent", arrMonth.map(x=> x._3).distinct.length)
    val sizeTopAge = topAgeByPert.length +1
    val mapAgeMonth = (1 until sizeTopAge).map(x=> topAgeByPert(sizeTopAge-x-1) -> x).toMap
    /* get top month */
    val topLast12month = trendAgeMonth.map(x=> x._2).distinct.sortWith((x, y) => x > y).filter(x=> x != CommonService.getCurrentMonth()).slice(0,15).sorted
    val topMonthAge = if(topLast12month.length >= 15) 16 else topLast12month.length+1
    val mapMonth   = (1 until topMonthAge).map(x=> topLast12month(x-1) -> x).toMap
    val rsAgeMonth = trendAgeMonth.filter(x=> topLast12month.indexOf(x._2) >=0).filter(x=> topAgeByPert.indexOf(x._1) >=0)
      .map(x=> (mapAgeMonth.get(x._1).get, mapMonth.get(x._2).get, x._3, x._4, x._5))
    logger.info("t2: "+(System.currentTimeMillis() - t2))
    val t3 = System.currentTimeMillis()

    // sparkline table
    val topAgeByF1 = getTopAgeByMonth(arrMonth, status, month, "f1", arrMonth.map(x=> x._3).distinct.length)
    val tbAge = trendAgeMonth.filter(x=> topLast12month.indexOf(x._2) >=0).filter(x=> topAgeByF1.indexOf(x._1) >=0).map(x=> (x._1, x._2, x._3,
      CommonService.format2Decimal(2*x._3*x._4/(x._3+x._4)), x._5))

    // comments content
    val cmtChart = getCommentChart(user, CommonUtil.PAGE_ID.get(0).get+"_tabAge")
    logger.info("t3: "+(System.currentTimeMillis() - t3))

    logger.info("Time: "+(System.currentTimeMillis() - t0))
    logger.info("========END AGE SERVICE=========")
    AgeResponse((mapAgeMonth, mapMonth,rsAgeMonth), (topAgeByF1, tbAge), (mapLocation, mapAge, rsLocationAge), cmtChart, month)
  }
}