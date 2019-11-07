package service

import churn.models.PackageResponse
import com.sksamuel.elastic4s.http.ElasticDsl.{RichFuture, RichString, SearchHttpExecutable, SearchShow, percentilesAggregation, query, rangeAggregation, search, termsAgg, termsAggregation, _}
import churn.utils.{AgeGroupUtil, CommonUtil, ProvinceUtil}
import play.api.Logger
import play.api.mvc.{AnyContent, Request}
import service.OverviewService.{checkLocation, getCommentChart}
import service.ChurnAgeService.{calChurnRateAndPercentForAgeMonth, client, getTopAgeByMonth, getTopOltByAge}
import services.Configure
import services.domain.CommonService

object ChurnPackageService{

  val client = Configure.client

  val logger = Logger(this.getClass())

  def getTrendPkgMonth(month: String, queries: String, province: String) ={
    val field = if(province == "No") "month" else checkLocation(province)
    val request = search(s"churn-contract-info-*" / "docs") query(OverviewService.rangeMonth(month) +" AND "+ queries) aggregations (
      termsAggregation(s"$field")
        .field(s"$field")
        .subaggs(
          termsAggregation("status")
            .field("status")
            .subaggs(
              termsAggregation("package_name")
                .field("package_name")
            ) size 1000
        ) size 15
      ) size 0
    val rs = client.execute(request).await
    CommonUtil.getChurnRateAndPercentage(rs,s"$field","status" , "package_name").map(x=> (x._1, x._2, x._3, x._5, x._6))
  }

  def getInternet(user: String, request: Request[AnyContent]) = {
    logger.info("========START PACKAGE SERVICE=========")
    val t0 = System.currentTimeMillis()
    val packages = ""
    val province = request.body.asFormUrlEncoded.get("province").head
    val age = request.body.asFormUrlEncoded.get("age").head
    val combo = request.body.asFormUrlEncoded.get("combo").head
    val month = request.body.asFormUrlEncoded.get("month").head
    val status = request.body.asFormUrlEncoded.get("status").head
    // get list province of region for filter
    val lstProvince = OverviewService.getRegionProvince(month)
    val queries = OverviewService.getFilterGroup(age, province, packages, combo, lstProvince)
    println(queries)
    logger.info("t0: "+(System.currentTimeMillis() - t0))
    val t1 = System.currentTimeMillis()

    // Trend Package and location
    val arrPkgLoc = getTrendPkgMonth(month, queries, province)
    val trendRegionAge = calChurnRateAndPercentForAgeMonth(arrPkgLoc, status, province).filter(x=> x._2 != CommonService.getCurrentMonth()).sorted
    /* get top location */
    val topLocationByPert = if(checkLocation(province) == "olt_name") getTopOltByAge(trendRegionAge) else trendRegionAge.map(x=> x._2).distinct
    val sizeTopLocation = topLocationByPert.length +1
    val mapLocation = (1 until sizeTopLocation).map(x=> topLocationByPert(sizeTopLocation-x-1) -> x).toMap
    /* get top Package Location */
    val topPkgLocByPert = trendRegionAge.map(x=> x._1).distinct
    val sizeTopPkgLoc = topPkgLocByPert.length +1
    val mapPkg = (1 until sizeTopPkgLoc).map(x=> topPkgLocByPert(sizeTopPkgLoc-x-1) -> x).toMap
    val rsLocationPkg = trendRegionAge.filter(x=> topPkgLocByPert.indexOf(x._1) >=0).filter(x=> topLocationByPert.indexOf(x._2) >=0)
      .map(x=> (mapLocation.get(x._2).get, mapPkg.get(x._1).get, x._3, x._4, x._5))
    logger.info("t1: "+(System.currentTimeMillis() - t1))
    val t2 = System.currentTimeMillis()

    // Trend Package and month
    val arrMonth = getTrendPkgMonth(month, queries, "No")
    val trendPkgMonth = calChurnRateAndPercentForAgeMonth(arrMonth, status, "No").filter(x=> x._2 != CommonService.getCurrentMonth()).sorted
    /* get top Package */
    val topPkgByPert = getTopAgeByMonth(arrMonth, status, month, "percent")
    val sizeTopPkg = topPkgByPert.length +1
    val mapPkgMonth = (1 until sizeTopPkg).map(x=> topPkgByPert(sizeTopPkg-x-1) -> x).toMap
    /* get top month */
    val topLast12month = trendPkgMonth.map(x=> x._2).distinct.sortWith((x, y) => x > y).filter(x=> x != CommonService.getCurrentMonth()).slice(0,15).sorted
    val topMonthPkg = if(topLast12month.length >= 15) 16 else topLast12month.length+1
    val mapMonth   = (1 until topMonthPkg).map(x=> topLast12month(x-1) -> x).toMap
    val rsPkgMonth = trendPkgMonth.filter(x=> topLast12month.indexOf(x._2) >=0).filter(x=> topPkgByPert.indexOf(x._1) >=0)
      .map(x=> (mapPkgMonth.get(x._1).get, mapMonth.get(x._2).get, x._3, x._4, x._5))
    logger.info("t2: "+(System.currentTimeMillis() - t2))
    val t3 = System.currentTimeMillis()

    // sparkline table
    val topPkgByF1 = getTopAgeByMonth(arrMonth, status, month, "f1")
    val tbPkg = trendPkgMonth.filter(x=> topLast12month.indexOf(x._2) >=0).filter(x=> topPkgByF1.indexOf(x._1) >=0).map(x=> (x._1, x._2, x._3,
      CommonService.format2Decimal(2*x._3*x._4/(x._3+x._4)), x._5))
    tbPkg.filter(x=> topLast12month.indexOf(x._2) >=0).filter(x=> topPkgByF1.indexOf(x._1) >=0).foreach(println)

    // comments content
    val cmtChart = getCommentChart(user, "tabPackage")
    logger.info("t3: "+(System.currentTimeMillis() - t3))

    logger.info("Time: "+(System.currentTimeMillis() - t0))
    logger.info("========END PACKAGE SERVICE=========")
    PackageResponse((mapPkgMonth, mapMonth,rsPkgMonth), (topPkgByF1, tbPkg), (mapLocation, mapPkg, rsLocationPkg), cmtChart, month)
  }
}