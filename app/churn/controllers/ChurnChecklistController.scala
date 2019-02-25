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
import service.ChurnChecklistService

/**
  * This controller creates an `Action` to handle HTTP requests to the
  * application's home page.
  */

@Singleton
class ChurnChecklistController @Inject() (cc: ControllerComponents) extends AbstractController(cc) with Secured{

  def index() =  withAuth {username => implicit request: Request[AnyContent] =>
    val month = CommonService.getPrevMonth()
    try {
      val rs = ChurnChecklistService.getInternet(null)
      Ok(churn.views.html.checklist.index(rs, rs.month, username, churn.controllers.routes.ChurnChecklistController.index()))
    }
    catch {
      case e: Exception => Ok(churn.views.html.checklist.index(null, month, username, churn.controllers.routes.ChurnChecklistController.index()))
    }
  }


  def getJsonChurn() =  withAuth {username => implicit request: Request[AnyContent] =>
    //try{
        val rs = ChurnChecklistService.getInternet(request)
        val churn1 = Json.obj(
          "cates"        -> rs.ctCheckList.map(x=> x._1),
          "count_ct"     -> rs.ctCheckList.map(x=> x._2),
          "churnPercent" -> rs.ctCheckList.map(x=> x._3),
          "count_all"    -> rs.ctCheckList.map(x=> x._4)
        )
        val churn2 = Json.obj(
          "cates"        -> rs.trendChecklist.map(x=> x._1),
          "churnRate"    -> rs.trendChecklist.map(x=> x._2),
          "churnPercent" -> rs.trendChecklist.map(x=> x._3->x._4)
        )
        val churn3 = Json.obj(
          "data"   -> rs.checklistRegion._2,
          "xAxis"  -> rs.checklistRegion._1,
          "yAxis"  -> rs.checklistRegion._2.map(x=>x._2).distinct.sorted.map(x=> "Vung " + x),
          "arrMonth" -> rs.checklistRegion._2.map(x=>x._1).distinct.sorted,
          "arrRegion" -> rs.checklistRegion._2.map(x=>x._2).distinct.sorted
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
          "data"   -> rs.numRegionAge,
          "xAxis"  -> rs.numRegionAge.map(x=> x._1.toInt).distinct.sorted.map(x=> AgeGroupUtil.getAgeById(x)),
          "yAxis"  -> rs.numRegionAge.map(x=>x._2).distinct.sorted.map(x=> "Vung " + x),
          "arrAge" -> rs.numRegionAge.map(x=>x._1.toInt).distinct.sorted,
          "arrRegion" -> rs.numRegionAge.map(x=>x._2).distinct.sorted
        )
        val minPerc6 = if(rs.checklistRegionAge.length > 0) CommonService.format3Decimal(rs.checklistRegionAge.map(x=>x._4).min) else 0
        val maxPerc6 = if(rs.checklistRegionAge.length > 0) CommonService.format3Decimal(rs.checklistRegionAge.map(x=>x._4).max) else 0
        val churn6 = Json.obj(
          "data"       -> rs.checklistRegionAge,
          "minPercent" -> minPerc6,
          "maxPercent" -> maxPerc6
        )
        val churn7 = Json.obj(
          "data"   -> rs.processTime._3,
          "xAxis"  -> rs.processTime._1,
          "yAxis"  -> rs.processTime._2.map(x=> "Vung " + x)
        )
        val churn8 = Json.obj(
          "data"   -> rs.typeRegion._3,
          "xAxis"  -> rs.typeRegion._1.map(x=> x._2),
          "yAxis"  -> rs.typeRegion._2.map(x=> "Vung "+x)
        )
        val minPerc9 = if(rs.trendTypeBubble._2.length > 0) CommonService.format3Decimal(rs.trendTypeBubble._2.map(x=>x._4).min) else 0
        val maxPerc9 = if(rs.trendTypeBubble._2.length > 0) CommonService.format3Decimal(rs.trendTypeBubble._2.map(x=>x._4).max) else 0
        val churn9 = Json.obj(
          "catesType"  -> rs.trendTypeBubble._1,
          "data"       -> rs.trendTypeBubble._2,
          "minPercent" -> minPerc9,
          "maxPercent" -> maxPerc9
        )
        val json = Json.obj(
          "churn1" -> churn1,
          "churn2" -> churn2,
          "churn3" -> churn3,
          "churn4" -> churn4,
          "churn5" -> churn5,
          "churn6" -> churn6,
          "churn7" -> churn7,
          "churn8" -> churn8,
          "churn9" -> churn9
        )
        Ok(Json.toJson(json))
    /*}
    catch{
      case e: Exception => Ok("Error")
    }*/

  }


}
