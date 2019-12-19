package churn.controllers

import churn.services.CacheService
import churn.utils.DateTimeUtil
import javax.inject.Inject
import javax.inject.Singleton
import play.api.mvc._
import play.api.mvc.AbstractController
import play.api.mvc.ControllerComponents
import services.domain.CommonService
import controllers.Secured
import play.api.libs.json.Json
import service.{OverviewMonthService, OverviewService}
import scala.util.Random

/**
  * This controller creates an `Action` to handle HTTP requests to the
  * application's home page.
  */

@Singleton
class OverviewController @Inject() (cc: ControllerComponents) extends AbstractController(cc) with Secured{
  def index() = withAuth { username => implicit request: Request[AnyContent] =>
    try {
      val rs = CacheService.getOverviewResponse(null, username)
      Ok(churn.views.html.overview.index(rs, rs.month, username, rs.trendRegionMonth._2.map(x=> x._1).toArray.sorted, churn.controllers.routes.OverviewController.index()))
    }
    catch {
      case e: Exception => Ok(churn.views.html.overview.index(null, CommonService.getCurrentMonth(), username, null, churn.controllers.routes.OverviewController.index()))
    }
  }

  def getJsonOverview() = withAuth {username => implicit request: Request[AnyContent] =>
    try{
      val rs = CacheService.getOverviewResponse(request, username)
      // num contract
      var perct = if(rs.numContract._1.prev.toLong == 0) "NaN" else CommonService.percent(rs.numContract._1.curr.toLong, rs.numContract._1.prev.toLong).toString
      val activeCt = Json.obj(
        "curr"       -> CommonService.formatPattern(rs.numContract._1.curr),
        "prev"       -> CommonService.formatPattern(rs.numContract._1.prev),
        "perct"      -> perct
      )
      perct = if(rs.numContract._2.prev.toLong == 0) "NaN" else CommonService.percent(rs.numContract._2.curr.toLong, rs.numContract._2.prev.toLong).toString
      val huydvCt = Json.obj(
        "curr"       -> CommonService.formatPattern(rs.numContract._2.curr),
        "prev"       -> CommonService.formatPattern(rs.numContract._2.prev),
        "perct"      -> perct
      )
      perct = if(rs.numContract._3.prev.toLong == 0) "NaN" else CommonService.percent(rs.numContract._3.curr.toLong, rs.numContract._3.prev.toLong).toString
      val ctbdvCt = Json.obj(
        "curr"       -> CommonService.formatPattern(rs.numContract._3.curr),
        "prev"       -> CommonService.formatPattern(rs.numContract._3.prev),
        "perct"      -> perct
      )
      val numContract = Json.obj(
        "actives"     -> activeCt,
        "ctbdv"       -> ctbdvCt,
        "huydv"       -> huydvCt
      )
      // churn rate
      val huydvRate = Json.obj(
        "avg"       -> rs.churnRate._1.avg,
        "curr"      -> rs.churnRate._1.curr
      )
      val ctbdvRate = Json.obj(
        "avg"       -> rs.churnRate._2.avg,
        "curr"      -> rs.churnRate._2.curr
      )
      val totalRate = Json.obj(
        "avg"       -> rs.churnRate._3.avg,
        "curr"      -> rs.churnRate._3.curr
      )
      val churnRate = Json.obj(
        "totalRate"       -> totalRate,
        "huydvRate"       -> huydvRate,
        "ctbdvRate"       -> ctbdvRate
      )
      // number of contract by month
      val ctHuyDV = rs.numofMonth.filter(x=> x._2.toInt == 1).map(x=> x._1 -> x._3).sorted.toMap
      val ctCTBDV = rs.numofMonth.filter(x=> x._2.toInt == 3).map(x=> x._1 -> x._3).sorted.toMap
      val cates   = rs.numofMonth.map(x=> x._1).distinct.map(x=> (x, ctHuyDV.get(x).getOrElse(0.toLong), ctCTBDV.get(x).getOrElse(0.toLong)))
      val numofMonth = Json.obj(
        "cates"      -> cates.map(x=> x._1).distinct.sorted,
        "huydv"      -> cates.map(x=> x._2),
        "titSeries1" -> "HSSD",
        "titSeries2" -> "CTBDV",
        "ctbdv"      -> cates.map(x=> x._3)
      )
      // trend churn rate by status
      val maxPertAll = if(rs.trendRatePert.length >0) rs.trendRatePert.map(x=> x._3).max else 0
      val totalTrend = Json.obj(
        "cates"       -> rs.trendRatePert.map(x=> x._1).distinct.sorted,
        "rate"        -> rs.trendRatePert.map(x=> x._2),
        "perct"       -> rs.trendRatePert.map(x=> x._3),
        "f1"          -> rs.trendRatePert.map(x=> x._4),
        "maxPercent"  -> maxPertAll
      )
      val trendRatePert = Json.obj(
        "totalTrend"       -> totalTrend
      )
      // sparkline table
      val tbChurn = Json.obj(
        "name" -> rs.tbChurn._1,
        "data" -> rs.tbChurn._2,
        "arrMonth" -> rs.trendRegionMonth._2.map(x=> x._1).toArray.sorted
      )
      // bubble charts
      val minPercBubble = if(rs.trendRegionMonth._3.length > 0) CommonService.format2Decimal(rs.trendRegionMonth._3.map(x=>x._4).min) else 0
      val maxPercBubble = if(rs.trendRegionMonth._3.length > 0) CommonService.format2Decimal(rs.trendRegionMonth._3.map(x=>x._4).max) else 0
      val isNullBubble  = if(rs.trendRegionMonth._3.length > 0 && rs.trendRegionMonth._3.map(x=>x._4).min == 0 && rs.trendRegionMonth._3.map(x=>x._3).min == 0) 1 else 0
      val churnBubble = Json.obj(
        "catesRegion" -> rs.trendRegionMonth._1,
        "catesMonth"  -> rs.trendRegionMonth._2,
        "data"       -> rs.trendRegionMonth._3,
        "minPercent" -> minPercBubble,
        "maxPercent" -> maxPercBubble,
        "isNull"     -> isNullBubble
      )
      val json = Json.obj(
        "numContract"    -> numContract,
        "churnRate"      -> churnRate,
        "numofMonth"     -> numofMonth,
        "trendRatePert"  -> trendRatePert,
        "churnBubble"    -> churnBubble,
        "tbChurn"        -> tbChurn,
        "lastYYYY"       -> CommonService.getPrevYYYY(rs.month),
        "lastYYYYmm"     -> CommonService.getPrevYYYYMM(rs.month),
        "location"       -> OverviewService.checkLocation(request.body.asFormUrlEncoded.get("province").head),
        "cmtChart"       -> rs.comments
      )
      Ok(Json.toJson(json))
    }
    catch{
      case e: Exception => Ok("Error")
    }
  }

  def getJsonByTab() = withAuth {username => implicit request: Request[AnyContent] =>
    try{
      val tabName = request.body.asFormUrlEncoded.get("tabName").head
      tabName match {
        // tab Age trending
        case "tabAge" => {
          val rs = CacheService.getOverviewAgeResponse(request, username)
          // bubble charts Age & Month
          val minPercAge = if(rs.trendAgeMonth._3.length > 0) CommonService.format2Decimal(rs.trendAgeMonth._3.map(x=>x._4).min) else 0
          val maxPercAge = if(rs.trendAgeMonth._3.length > 0) CommonService.format2Decimal(rs.trendAgeMonth._3.map(x=>x._4).max) else 0
          val isNullAge  = if(rs.trendAgeMonth._3.length > 0 && rs.trendAgeMonth._3.map(x=>x._4).min == 0 && rs.trendAgeMonth._3.map(x=>x._3).min == 0) 1 else 0
          val churnAge = Json.obj(
            "catesRegion" -> rs.trendAgeMonth._1,
            "catesMonth"  -> rs.trendAgeMonth._2,
            "data"       -> rs.trendAgeMonth._3,
            "minPercent" -> minPercAge,
            "maxPercent" -> maxPercAge,
            "isNull"     -> isNullAge
          )
          // bubble charts Age & Location
          val minPercLocation = if(rs.trendAgeLocation._3.length > 0) CommonService.format2Decimal(rs.trendAgeLocation._3.map(x=>x._4).min) else 0
          val maxPercLocation = if(rs.trendAgeLocation._3.length > 0) CommonService.format2Decimal(rs.trendAgeLocation._3.map(x=>x._4).max) else 0
          val isNullLocation  = if(rs.trendAgeLocation._3.length > 0 && rs.trendAgeLocation._3.map(x=>x._4).min == 0 && rs.trendAgeLocation._3.map(x=>x._3).min == 0) 1 else 0
          val churnLocation = Json.obj(
            "catesRegion" -> rs.trendAgeLocation._1,
            "catesMonth"  -> rs.trendAgeLocation._2,
            "data"       -> rs.trendAgeLocation._3,
            "minPercent" -> minPercLocation,
            "maxPercent" -> maxPercLocation,
            "isNull"     -> isNullLocation
          )
          // sparkline table
          val tbAge = Json.obj(
            "name" -> rs.tbAge._1,
            "data" -> rs.tbAge._2,
            "arrMonth" -> rs.trendAgeMonth._2.map(x=> x._1).toArray.sorted
          )
          val json = Json.obj(
            "churnAgeLocation"    -> churnLocation,
            "churnAgeMonth"   -> churnAge,
            "tbAge"     -> tbAge,
            "cmtChart"  -> rs.comments
          )
          Ok(Json.toJson(json))
        }
        case "tabPackage" => {
          val rs = CacheService.getOverviewPackageResponse(request, username)
          // bubble charts Package & Month
          val minPercPkg = if(rs.trendPkgMonth._3.length > 0) CommonService.format2Decimal(rs.trendPkgMonth._3.map(x=>x._4).min) else 0
          val maxPercPkg = if(rs.trendPkgMonth._3.length > 0) CommonService.format2Decimal(rs.trendPkgMonth._3.map(x=>x._4).max) else 0
          val isNullPkg  = if(rs.trendPkgMonth._3.length > 0 && rs.trendPkgMonth._3.map(x=>x._4).min == 0 && rs.trendPkgMonth._3.map(x=>x._3).min == 0) 1 else 0
          val churnPkg = Json.obj(
            "catesRegion" -> rs.trendPkgMonth._1,
            "catesMonth"  -> rs.trendPkgMonth._2,
            "data"       -> rs.trendPkgMonth._3,
            "minPercent" -> minPercPkg,
            "maxPercent" -> maxPercPkg,
            "isNull"     -> isNullPkg
          )
          // bubble charts Package & Location
          val minPercLocation = if(rs.trendPkgLocation._3.length > 0) CommonService.format2Decimal(rs.trendPkgLocation._3.map(x=>x._4).min) else 0
          val maxPercLocation = if(rs.trendPkgLocation._3.length > 0) CommonService.format2Decimal(rs.trendPkgLocation._3.map(x=>x._4).max) else 0
          val isNullLocation  = if(rs.trendPkgLocation._3.length > 0 && rs.trendPkgLocation._3.map(x=>x._4).min == 0 && rs.trendPkgLocation._3.map(x=>x._3).min == 0) 1 else 0
          val churnLocation = Json.obj(
            "catesRegion" -> rs.trendPkgLocation._1,
            "catesMonth"  -> rs.trendPkgLocation._2,
            "data"       -> rs.trendPkgLocation._3,
            "minPercent" -> minPercLocation,
            "maxPercent" -> maxPercLocation,
            "isNull"     -> isNullLocation
          )
          // bubble charts Package & Age
          val minPercPkgAge = if(rs.trendPkgAge._3.length > 0) CommonService.format2Decimal(rs.trendPkgAge._3.map(x=>x._4).min) else 0
          val maxPercPkgAge = if(rs.trendPkgAge._3.length > 0) CommonService.format2Decimal(rs.trendPkgAge._3.map(x=>x._4).max) else 0
          val isNullPkgAge  = if(rs.trendPkgAge._3.length > 0 && rs.trendPkgAge._3.map(x=>x._4).min == 0 && rs.trendPkgAge._3.map(x=>x._3).min == 0) 1 else 0
          val churnPkgAge = Json.obj(
            "catesRegion" -> rs.trendPkgAge._1,
            "catesMonth"  -> rs.trendPkgAge._2,
            "data"       -> rs.trendPkgAge._3,
            "minPercent" -> minPercPkgAge,
            "maxPercent" -> maxPercPkgAge,
            "isNull"     -> isNullPkgAge
          )
          // sparkline table
          val tbPkg = Json.obj(
            "name" -> rs.tbPkg._1,
            "data" -> rs.tbPkg._2,
            "arrMonth" -> rs.trendPkgMonth._2.map(x=> x._1).toArray.sorted
          )
          val json = Json.obj(
            "churnPkgLocation"    -> churnLocation,
            "churnPkgMonth"   -> churnPkg,
            "tbPkg"     -> tbPkg,
            "cmtChart"  -> rs.comments,
            "churnPkgAge"  -> churnPkgAge
          )
          Ok(Json.toJson(json))
        }
        case "tabMonth" => {
          val rs = CacheService.getOverviewMonthResponse(request, username)
          val plan = OverviewMonthService.getDataPoint(DateTimeUtil.getDaysOfMonth(request.body.asFormUrlEncoded.get("month").head)
            .map(x=> (x, Random.nextInt(300).toLong, Random.nextInt(300).toLong, Random.nextInt(300).toLong)).sorted,
            request.body.asFormUrlEncoded.get("month").head, request.body.asFormUrlEncoded.get("dataPoint").head.toInt)

          val arrTbTrend = Json.obj(
            "quarter" -> rs.arrTbTrend._1,
            "data" -> rs.arrTbTrend._2.map(x=> (x._1, CommonService.formatPattern(x._2), CommonService.formatPattern(x._3), CommonService.formatPattern(x._4),
              CommonService.formatPattern(x._5), CommonService.formatPattern(x._6), CommonService.formatPattern(x._7))),
            "total" -> rs.arrTbTrend._3.map(x=> (CommonService.formatPattern(x._1), CommonService.formatPattern(x._2), CommonService.formatPattern(x._3),
              CommonService.formatPattern(x._4), CommonService.formatPattern(x._5), CommonService.formatPattern(x._6)))
          )
          val chartDaily = Json.obj(
            "cateX" -> plan.map(x=> x._1).sorted,
            "planDaily" -> plan.map(x=> x._1 -> x._2).sorted,
            "realDaily" -> rs.rangeDaily.map(x=> x._1 -> x._2).sorted,
            "rangeDaily" -> rs.rangeDaily.map(x=> (x._1, x._3, x._4)).sorted
          )
          val json = Json.obj(
            "arrTbTrend"  -> arrTbTrend,
            "chartDaily"     -> chartDaily,
            "cmtChart"  -> rs.comments
          )
          Ok(Json.toJson(json))
        }
      }
    }
    catch{
      case e: Exception => Ok("Error")
    }

  }

  def pushComment() = withAuth { username => implicit request: Request[AnyContent] =>
      try {
        CommonService.insertComment(username, request)
        Ok("Ok")
      }
      catch {
        case e: Exception => Ok("Error")
      }
  }

}
