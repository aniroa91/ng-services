package churn.controllers

import play.api.data.Form
import play.api.data.Forms.mapping
import play.api.data.Forms.text
import javax.inject.Inject
import javax.inject.Singleton

import play.api.mvc._
import play.api.mvc.AbstractController
import play.api.mvc.ControllerComponents
import churn.utils.AgeGroupUtil
import services.domain.CommonService
import controllers.Secured
import com.sksamuel.elastic4s.http.HttpClient
import com.sksamuel.elastic4s.ElasticsearchClientUri
import services.Configure
import play.api.libs.json.Json
import com.sksamuel.elastic4s.http.ElasticDsl._
import com.ftel.bigdata.utils.StringUtil
import service.OverviewService

/**
  * This controller creates an `Action` to handle HTTP requests to the
  * application's home page.
  */

@Singleton
class OverviewController @Inject() (cc: ControllerComponents) extends AbstractController(cc) with Secured{

  def index() = withAuth { username => implicit request: Request[AnyContent] =>
    val month = CommonService.getPrevMonth()
    try {
      val rs = OverviewService.getInternet(null)
      Ok(churn.views.html.overview.index(rs, month, username, churn.controllers.routes.OverviewController.index()))
    }
    catch {
      case e: Exception => Ok(churn.views.html.overview.index(null, month, username, churn.controllers.routes.OverviewController.index()))
    }
  }

  def getJsonChurn() = withAuth {username => implicit request: Request[AnyContent] =>
    try{
      val rs = OverviewService.getInternet(request)
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
      val numofMonth = Json.obj(
        "cates"     -> rs.numofMonth.map(x=> x._1).distinct.sorted,
        "huydv"     -> rs.numofMonth.filter(x=> x._2.toInt == 1).sorted.map(x=> x._3),
        "titSeries1" -> "HSSD",
        "titSeries2" -> "CTBDV",
        "ctbdv"     -> rs.numofMonth.filter(x=> x._2.toInt == 3).sorted.map(x=> x._3)
      )
      // trend churn rate by status
      val totalTrend = Json.obj(
        "cates"       -> rs.trendRatePert._1.map(x=> x._1).distinct.sorted,
        "rate"        -> rs.trendRatePert._1.map(x=> x._2),
        "perct"       -> rs.trendRatePert._1.map(x=> x._3),
        "f1"          -> rs.trendRatePert._1.map(x=> x._4)
      )
      val huydvTrend = Json.obj(
        "cates"       -> rs.trendRatePert._2.map(x=> x._1).distinct.sorted,
        "rate"        -> rs.trendRatePert._2.map(x=> x._2),
        "perct"       -> rs.trendRatePert._2.map(x=> x._3),
        "f1"          -> rs.trendRatePert._2.map(x=> x._4)
      )
      val ctbdvTrend = Json.obj(
        "cates"       -> rs.trendRatePert._3.map(x=> x._1).distinct.sorted,
        "rate"        -> rs.trendRatePert._3.map(x=> x._2),
        "perct"       -> rs.trendRatePert._3.map(x=> x._3),
        "f1"          -> rs.trendRatePert._3.map(x=> x._4)
      )
      val trendRatePert = Json.obj(
        "totalTrend"       -> totalTrend,
        "huydvTrend"       -> huydvTrend,
        "ctbdvTrend"       -> ctbdvTrend
      )
      val json = Json.obj(
        "numContract"    -> numContract,
        "churnRate"      -> churnRate,
        "numofMonth"     -> numofMonth,
        "trendRatePert"  -> trendRatePert,
        "lastYYYY"       -> CommonService.getPrevYYYY(rs.month),
        "lastYYYYmm"     -> CommonService.getPrevYYYYMM(rs.month)
      )
      Ok(Json.toJson(json))
    }
    catch{
      case e: Exception => Ok("Error")
    }

  }


}
