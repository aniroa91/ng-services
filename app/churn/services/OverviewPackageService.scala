package service

import churn.models.PackageResponse
import com.sksamuel.elastic4s.http.ElasticDsl.{RichFuture, RichString, SearchHttpExecutable, search, termsAggregation, _}
import churn.utils.CommonUtil
import play.api.Logger
import play.api.mvc.{AnyContent, Request}
import service.OverviewService.{checkLocation, getCommentChart}
import service.OverviewAgeService.{calChurnRateAndPercentForAgeMonth, getTopAgeByMonth, getTopOltByAge}
import services.Configure
import services.domain.CommonService
import scala.concurrent.ExecutionContext.Implicits.global
import scala.async.Async.{async, await}
import scala.concurrent.{Await, Future}
import scala.concurrent.duration.Duration

object OverviewPackageService{

  val client = Configure.client

  val logger = Logger(this.getClass())

  def getPkgMonth(month: String, queries: String, province: String, colName: String) ={
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

  def getTrendPackageLoc(month: String, queries: String, province:String, status: String) = {
    val arrPkgLoc = getPkgMonth(month, queries, province, "")
    val trendRegionPkg = calChurnRateAndPercentForAgeMonth(arrPkgLoc, status, province).sorted
    /* get top location */
    val topLocationByPert = if(checkLocation(province) == "olt_name") getTopOltByAge(trendRegionPkg.map(x=> (x._1, x._2, x._4))) else trendRegionPkg.map(x=> x._2).distinct
    val sizeTopLocation = topLocationByPert.length +1
    val mapLocation = (1 until sizeTopLocation).map(x=> topLocationByPert(sizeTopLocation-x-1) -> x).toMap
    /* get top Package Location */
    val topPkgLocByPert = getTopOltByAge(trendRegionPkg.map(x=> (x._2, x._1, x._4)))
    val sizeTopPkgLoc = topPkgLocByPert.length +1
    val mapPkg = (1 until sizeTopPkgLoc).map(x=> topPkgLocByPert(sizeTopPkgLoc-x-1) -> x).toMap
    val rsLocationPkg = trendRegionPkg.filter(x=> topPkgLocByPert.indexOf(x._1) >=0).filter(x=> topLocationByPert.indexOf(x._2) >=0)
      .map(x=> (mapLocation.get(x._2).get, mapPkg.get(x._1).get, x._3, x._4, x._5))
    Future{
      (mapLocation, mapPkg, rsLocationPkg)
    }
  }

  def getTrendPackageAge(month:String, queries:String, status:String) = {
    val arrPkgAge = getPkgMonth(month, queries, "No", "lifeGroup")
    val trendPkgAge = calChurnRateAndPercentForAgeMonth(arrPkgAge, status, "No").sorted
    /* get top age */
    val topAgeByPert = trendPkgAge.map(x=> x._2).distinct
    val sizeTopAge = topAgeByPert.length +1
    val mapPkgAge = (1 until sizeTopAge).map(x=> topAgeByPert(sizeTopAge-x-1) -> x).toMap
    /* get top Package Age */
    val topPkgAgeByPert = getTopOltByAge(trendPkgAge.map(x=> (x._2, x._1, x._4)))
    val sizeTopPkgAge = topPkgAgeByPert.length +1
    val mapPackage = (1 until sizeTopPkgAge).map(x=> topPkgAgeByPert(sizeTopPkgAge-x-1) -> x).toMap
    val rsPkgAge = trendPkgAge.filter(x=> topPkgAgeByPert.indexOf(x._1) >=0).filter(x=> topAgeByPert.indexOf(x._2) >=0)
      .map(x=> (mapPkgAge.get(x._2).get, mapPackage.get(x._1).get, x._3, x._4, x._5))
    Future{
      (mapPkgAge, mapPackage, rsPkgAge)
    }
  }

  def getTrendPackageMonth(month: String, queries: String, status: String) = {
    val arrMonth = getPkgMonth(month, queries, "No", "month")
    val trendPkgMonth = calChurnRateAndPercentForAgeMonth(arrMonth, status, "No").sorted
    /* get top Package */
    val topPkgByPert = getTopAgeByMonth(arrMonth, status, month, "percent", 10)
    val sizeTopPkg = topPkgByPert.length +1
    val mapPkgMonth = (1 until sizeTopPkg).map(x=> topPkgByPert(sizeTopPkg-x-1) -> x).toMap
    /* get top month */
    val topLast12month = trendPkgMonth.map(x=> x._2).distinct.sortWith((x, y) => x > y).slice(0,15).sorted
    val topMonthPkg = if(topLast12month.length >= 15) 16 else topLast12month.length+1
    val mapMonth   = (1 until topMonthPkg).map(x=> topLast12month(x-1) -> x).toMap
    val rsPkgMonth = trendPkgMonth.filter(x=> topLast12month.indexOf(x._2) >=0).filter(x=> topPkgByPert.indexOf(x._1) >=0)
      .map(x=> (mapPkgMonth.get(x._1).get, mapMonth.get(x._2).get, x._3, x._4, x._5))

    // sparkline table
    val topPkgByF1 = getTopAgeByMonth(arrMonth, status, month, "f1", 10)
    val tbPkg = trendPkgMonth.filter(x=> topLast12month.indexOf(x._2) >=0).filter(x=> topPkgByF1.indexOf(x._1) >=0).map(x=> (x._1, x._2, x._3,
      CommonService.format2Decimal(2*x._3*x._4/(x._3+x._4)), x._5))
    Future{
      (mapPkgMonth, mapMonth,rsPkgMonth, topPkgByF1, tbPkg)
    }
  }

  def getInternet(user: String, request: Request[AnyContent]) = async{
    logger.info("========START PACKAGE SERVICE=========")
    val t0 = System.currentTimeMillis()
    val packages = ""
    val province = request.body.asFormUrlEncoded.get("province").head
    val age = request.body.asFormUrlEncoded.get("age").head
    val combo = request.body.asFormUrlEncoded.get("combo").head
    val month = request.body.asFormUrlEncoded.get("month").head
    val status = request.body.asFormUrlEncoded.get("status").head
    val queries = OverviewService.getFilterGroup(age, province, packages, combo)
    //println(queries)
    logger.info("t0: "+(System.currentTimeMillis() - t0))
    val t1 = System.currentTimeMillis()

    // Trend Package and location
    val trendPackLoc = Await.result(getTrendPackageLoc(month, queries, province, status), Duration.Inf)
    logger.info("t1: "+(System.currentTimeMillis() - t1))
    val t2 = System.currentTimeMillis()

    // Trend Package and month
    val trendPackMonth = Await.result(getTrendPackageMonth(month, queries, status), Duration.Inf)
    logger.info("t2:"+(System.currentTimeMillis() - t2))
    val t3  = System.currentTimeMillis()

    // comments content
    val cmtChart = Await.result(Future{ getCommentChart(user, CommonUtil.PAGE_ID.get(0).get+"_tabPackage") }, Duration.Inf)
    logger.info("t3: "+(System.currentTimeMillis() - t3))
    val t4 = System.currentTimeMillis()

    // Trend Package and Age
    val trendPackAge = Await.result(getTrendPackageAge(month, queries, status), Duration.Inf)
    logger.info("t4: "+(System.currentTimeMillis() - t4))

    logger.info("Time: "+(System.currentTimeMillis() - t0))
    logger.info("========END PACKAGE SERVICE=========")
    await(
      Future {
        PackageResponse((trendPackMonth._1, trendPackMonth._2, trendPackMonth._3), (trendPackMonth._4, trendPackMonth._5), trendPackLoc, trendPackAge, cmtChart, month)
      }
    )
  }
}