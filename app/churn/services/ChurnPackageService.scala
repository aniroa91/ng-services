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

  def getTrendPkgMonth(month: String, queries: String, province: String, colName: String) ={
    val field = if(province == "No") s"$colName" else checkLocation(province)
    val request = search(s"churn-contract-info-*" / "docs") query(OverviewService.rangeMonth(month) +" AND "+ queries) aggregations (
      termsAggregation(s"$field")
        .field(s"$field")
        .subaggs(
          termsAggregation("status")
            .field("status")
            .subaggs(
              termsAggregation("package_name")
                .field("package_name") size 1000
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
    val arrPkgLoc = getTrendPkgMonth(month, queries, province, "")
    val trendRegionPkg = calChurnRateAndPercentForAgeMonth(arrPkgLoc, status, province).filter(x=> x._2 != CommonService.getCurrentMonth()).sorted
    /* get top location */
    val topLocationByPert = if(checkLocation(province) == "olt_name") getTopOltByAge(trendRegionPkg) else trendRegionPkg.map(x=> x._2).distinct
    val sizeTopLocation = topLocationByPert.length +1
    val mapLocation = (1 until sizeTopLocation).map(x=> topLocationByPert(sizeTopLocation-x-1) -> x).toMap
    /* get top Package Location */
    val topPkgLocByPert = getTopOltByAge(trendRegionPkg.map(x=> (x._2, x._1, x._3, x._4, x._5)))
    val sizeTopPkgLoc = topPkgLocByPert.length +1
    val mapPkg = (1 until sizeTopPkgLoc).map(x=> topPkgLocByPert(sizeTopPkgLoc-x-1) -> x).toMap
    val rsLocationPkg = trendRegionPkg.filter(x=> topPkgLocByPert.indexOf(x._1) >=0).filter(x=> topLocationByPert.indexOf(x._2) >=0)
      .map(x=> (mapLocation.get(x._2).get, mapPkg.get(x._1).get, x._3, x._4, x._5))
    logger.info("t1: "+(System.currentTimeMillis() - t1))
    val t2 = System.currentTimeMillis()

    // Trend Package and month
    val arrMonth = getTrendPkgMonth(month, queries, "No", "month")
    val trendPkgMonth = calChurnRateAndPercentForAgeMonth(arrMonth, status, "No").filter(x=> x._2 != CommonService.getCurrentMonth()).sorted
    /* get top Package */
    val topPkgByPert = getTopAgeByMonth(arrMonth, status, month, "percent", 10)
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
    val topPkgByF1 = getTopAgeByMonth(arrMonth, status, month, "f1", 10)
    val tbPkg = trendPkgMonth.filter(x=> topLast12month.indexOf(x._2) >=0).filter(x=> topPkgByF1.indexOf(x._1) >=0).map(x=> (x._1, x._2, x._3,
      CommonService.format2Decimal(2*x._3*x._4/(x._3+x._4)), x._5))

    // comments content
    val cmtChart = getCommentChart(user, "tabPackage")
    logger.info("t3: "+(System.currentTimeMillis() - t3))
    val t4 = System.currentTimeMillis()

    // Trend Package and Age
    val arrPkgAge = getTrendPkgMonth(month, queries, "No", "lifeGroup")
    val trendPkgAge = calChurnRateAndPercentForAgeMonth(arrPkgAge, status, "No").filter(x=> x._2 != CommonService.getCurrentMonth()).sorted
    /* get top age */
    val topAgeByPert = trendPkgAge.map(x=> x._2).distinct
    val sizeTopAge = topAgeByPert.length +1
    val mapPkgAge = (1 until sizeTopAge).map(x=> topAgeByPert(sizeTopAge-x-1) -> x).toMap
    /* get top Package Age */
    val topPkgAgeByPert = getTopOltByAge(trendPkgAge.map(x=> (x._2, x._1, x._3, x._4, x._5)))
    val sizeTopPkgAge = topPkgAgeByPert.length +1
    val mapPackage = (1 until sizeTopPkgAge).map(x=> topPkgAgeByPert(sizeTopPkgAge-x-1) -> x).toMap
    val rsPkgAge = trendPkgAge.filter(x=> topPkgAgeByPert.indexOf(x._1) >=0).filter(x=> topAgeByPert.indexOf(x._2) >=0)
      .map(x=> (mapPkgAge.get(x._2).get, mapPackage.get(x._1).get, x._3, x._4, x._5))
    logger.info("t4: "+(System.currentTimeMillis() - t4))

    logger.info("Time: "+(System.currentTimeMillis() - t0))
    logger.info("========END PACKAGE SERVICE=========")
    PackageResponse((mapPkgMonth, mapMonth,rsPkgMonth), (topPkgByF1, tbPkg), (mapLocation, mapPkg, rsLocationPkg), (mapPkgAge, mapPackage, rsPkgAge), cmtChart, month)
  }
}