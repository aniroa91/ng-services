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
import service.ChurnCallogService

/**
  * This controller creates an `Action` to handle HTTP requests to the
  * application's home page.
  */

@Singleton
class ChurnCallogController @Inject() (cc: ControllerComponents) extends AbstractController(cc) with Secured{

  def index() = withAuth { username => implicit request: Request[AnyContent] =>
    val month = CommonService.getPrevMonth()
    try {
      val rs = ChurnCallogService.getInternet(null)
      Ok(churn.views.html.calllog.index(rs, month, username, churn.controllers.routes.ChurnCallogController.index()))
    }
    catch {
      case e: Exception => Ok(churn.views.html.calllog.index(null, month, username, churn.controllers.routes.ChurnCallogController.index()))
    }
  }

  def getJsonChurn() = withAuth {username => implicit request: Request[AnyContent] =>
    try{
      val rs = ChurnCallogService.getInternet(request)
      val churn1 = Json.obj(
        "cates"        -> rs.whoCallIn.map(x=> x._1),
        "count_ct"     -> rs.whoCallIn.map(x=> x._2),
        "churnPercent" -> rs.whoCallIn.map(x=> x._3),
        "count_all" -> rs.whoCallIn.map(x=> x._4)
      )
      val churn2 = Json.obj(
        "cates"        -> rs.trendCallIn.map(x=> x._1),
        "churnRate"    -> rs.trendCallIn.map(x=> x._2),
        "churnPercent" -> rs.trendCallIn.map(x=> x._3)
      )
      val churn3 = Json.obj(
        "data"   -> rs.callInRegion._2,
        "xAxis"  -> rs.callInRegion._1,
        "yAxis"  -> rs.callInRegion._2.map(x=>x._2).distinct.sorted.map(x=> "Vung " + x),
        "arrMonth" -> rs.callInRegion._2.map(x=>x._1).distinct.sorted,
        "arrRegion" -> rs.callInRegion._2.map(x=>x._2).distinct.sorted
      )
      val minPerc4 = if(rs.trendRegionMonth._2.length > 0) CommonService.format3Decimal(rs.trendRegionMonth._2.map(x=>x._4).min) else 0
      val maxPerc4 = if(rs.trendRegionMonth._2.length > 0) CommonService.format3Decimal(rs.trendRegionMonth._2.map(x=>x._4).max) else 0
      val churn4 = Json.obj(
        "catesMonth" -> rs.trendRegionMonth._1,
        "data"       -> rs.trendRegionMonth._2,
        "minPercent" -> minPerc4,
        "maxPercent" -> maxPerc4
      )
      val churn5 = Json.obj(
        "data"   -> rs.callInRegionAge,
        "xAxis"  -> rs.callInRegionAge.map(x=> x._1.toInt).distinct.sorted.map(x=> AgeGroupUtil.getAgeById(x)),
        "yAxis"  -> rs.callInRegionAge.map(x=>x._2).distinct.sorted.map(x=> "Vung " + x),
        "arrMonth" -> rs.callInRegionAge.map(x=>x._1).distinct.sorted,
        "arrRegion" -> rs.callInRegionAge.map(x=>x._2).distinct.sorted
      )
      val minPerc6 = if(rs.callRegionAge.length > 0) CommonService.format3Decimal(rs.callRegionAge.map(x=>x._4).min) else 0
      val maxPerc6 = if(rs.callRegionAge.length > 0) CommonService.format3Decimal(rs.callRegionAge.map(x=>x._4).max) else 0
      val churn6 = Json.obj(
        "data"       -> rs.callRegionAge,
        "minPercent" -> minPerc6,
        "maxPercent" -> maxPerc6
      )
      val churn7 = Json.obj(
        "cates"        -> rs.churnCates.map(x=> x._1),
        "churnRate"     -> rs.churnCates.map(x=> x._2),
        "churnPercent" -> rs.churnCates.map(x=> x._3)
      )
      val json = Json.obj(
        "churn1" -> churn1,
        "churn2" -> churn2,
        "churn3" -> churn3,
        "churn4" -> churn4,
        "churn5" -> churn5,
        "churn6" -> churn6,
        "churn7" -> churn7
      )
      Ok(Json.toJson(json))
    }
    catch{
      case e: Exception => Ok("Error")
    }
  }

}
