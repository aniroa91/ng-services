package churn.controllers

import play.api.data.Form
import play.api.data.Forms.mapping
import play.api.data.Forms.text
import javax.inject.Inject
import javax.inject.Singleton
import play.api.mvc._
import play.api.mvc.AbstractController
import play.api.mvc.ControllerComponents
import churn.utils.{AgeGroupUtil, CommonUtil}
import services.domain.CommonService
import controllers.Secured
import com.sksamuel.elastic4s.http.HttpClient
import com.sksamuel.elastic4s.ElasticsearchClientUri
import services.Configure
import play.api.libs.json.Json
import com.sksamuel.elastic4s.http.ElasticDsl._
import com.ftel.bigdata.utils.StringUtil
import service.{ChurnAgeService, OverviewService}

/**
  * This controller creates an `Action` to handle HTTP requests to the
  * application's home page.
  */

@Singleton
class OverviewController @Inject() (cc: ControllerComponents) extends AbstractController(cc) with Secured{
  def index() = withAuth { username => implicit request: Request[AnyContent] =>
    try {
      val rs = OverviewService.getInternet(username, null)
      Ok(churn.views.html.overview.index(rs, rs.month, username, rs.trendRegionMonth._2.map(x=> x._1).toArray.sorted, churn.controllers.routes.OverviewController.index()))
    }
    catch {
      case e: Exception => Ok(churn.views.html.overview.index(null, CommonService.getPrevMonth(), username, null, churn.controllers.routes.OverviewController.index()))
    }
  }

  def getJsonOverview() = withAuth {username => implicit request: Request[AnyContent] =>
    try{
      val rs = OverviewService.getInternet(username, request)
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
        "location"       -> OverviewService.checkLocation(request.body.asFormUrlEncoded.get("province").head)
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
          val rs = ChurnAgeService.getInternet(username, request)
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

  def getComment(tab: String) = withAuth { username => implicit request: Request[AnyContent] =>
    try {
      val comments = OverviewService.getCommentChart(username, tab)
      Ok(comments)
    }
    catch {
      case e: Exception => Ok("Error")
    }
  }

}
