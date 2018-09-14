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
import service.ChurnRegionService

/**
  * This controller creates an `Action` to handle HTTP requests to the
  * application's home page.
  */

@Singleton
class ChurnRegionController @Inject() (cc: ControllerComponents) extends AbstractController(cc) with Secured{

  def index() = withAuth {username => implicit request: Request[AnyContent] =>
    val month = CommonService.getPrevMonth()
    try{
      val rs = ChurnRegionService.getInternet(null)
      Ok(churn.views.html.region.index(rs,month,username, churn.controllers.routes.ChurnRegionController.index()))
    }
    catch{
      case e: Exception => Ok(churn.views.html.region.index(null,month,username, churn.controllers.routes.ChurnRegionController.index()))
    }
  }

  def getJsonChurn() = withAuth {username => implicit request: Request[AnyContent] =>
    try{
      val rs = ChurnRegionService.getInternet(request)
      val churn1 = Json.obj(
        "cates"        -> rs.churnRegion.map(x=> "Vung "+ x._1),
        "churnRate"    -> rs.churnRegion.map(x=> x._2),
        "churnPercent" -> rs.churnRegion.map(x=> x._3)
      )
      val churn2 = Json.obj(
        "cates"        -> rs.churnProfile.map(x=> x._1),
        "churnRate"    -> rs.churnProfile.map(x=> x._2),
        "churnPercent" -> rs.churnProfile.map(x=> x._3)
      )
      val minPerc3 = if(rs.trendRegionMonth._2.length > 0) CommonService.format3Decimal(rs.trendRegionMonth._2.map(x=>x._4).min) else 0
      val maxPerc3 = if(rs.trendRegionMonth._2.length > 0) CommonService.format3Decimal(rs.trendRegionMonth._2.map(x=>x._4).max) else 0
      val churn3 = Json.obj(
        "cates" -> rs.trendRegionMonth._1,
        "data"       -> rs.trendRegionMonth._2,
        "minPercent" -> minPerc3,
        "maxPercent" -> maxPerc3
      )
      val minPerc4 = if(rs.trendProfileMonth._3.length > 0) CommonService.format3Decimal(rs.trendProfileMonth._3.map(x=>x._4).min) else 0
      val maxPerc4 = if(rs.trendProfileMonth._3.length > 0) CommonService.format3Decimal(rs.trendProfileMonth._3.map(x=>x._4).max) else 0
      val churn4 = Json.obj(
        "catesProfile" -> rs.trendProfileMonth._1,
        "catesMonth"  -> rs.trendProfileMonth._2,
        "data"       -> rs.trendProfileMonth._3,
        "minPercent" -> minPerc4,
        "maxPercent" -> maxPerc4
      )
      val minPerc5 = if(rs.trendAgeProfile._2.length > 0) CommonService.format3Decimal(rs.trendAgeProfile._2.map(x=>x._4).min) else 0
      val maxPerc5 = if(rs.trendAgeProfile._2.length > 0) CommonService.format3Decimal(rs.trendAgeProfile._2.map(x=>x._4).max) else 0
      val churn5 = Json.obj(
        "catesProfile" -> rs.trendAgeProfile._1,
        "data"       -> rs.trendAgeProfile._2,
        "minPercent" -> minPerc5,
        "maxPercent" -> maxPerc5
      )
      val heat_map = Json.obj(
        "data"   -> rs.numberOfContracts._3,
        "xAxis"  -> rs.numberOfContracts._1,
        "yAxis"  -> rs.numberOfContracts._2.map(x=> x._1)
      )
      val json = Json.obj(
        "churn1" -> churn1,
        "churn2" -> churn2,
        "churn3" -> churn3,
        "churn4" -> churn4,
        "churn5" -> churn5,
        "heat_map" -> heat_map
      )
      Ok(Json.toJson(json))
    }
    catch{
      case e: Exception => Ok("Error")
    }
  }

}
