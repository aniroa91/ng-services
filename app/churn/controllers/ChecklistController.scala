package churn.controllers

import churn.services.CacheService
import javax.inject.Inject
import javax.inject.Singleton
import play.api.mvc._
import play.api.mvc.AbstractController
import play.api.mvc.ControllerComponents
import services.domain.CommonService
import controllers.Secured
import play.api.libs.json.Json

/**
  * This controller creates an `Action` to handle HTTP requests to the
  * application's home page.
  */

@Singleton
class ChecklistController @Inject()(cc: ControllerComponents) extends AbstractController(cc) with Secured{

  def getJsonChurn() =  withAuth {username => implicit request: Request[AnyContent] =>
    try{
        val rs = CacheService.getChecklistResponse(request)
        // num contract have checklist
        var perct = if(rs.numContract._1.prev.toLong == 0) "NaN" else CommonService.percentDouble(rs.numContract._1.curr.toDouble, rs.numContract._1.prev.toDouble).toString
        val contractCL = Json.obj(
          "curr"       -> CommonService.formatPattern(rs.numContract._1.curr),
          "prev"       -> CommonService.formatPattern(rs.numContract._1.prev),
          "perct"      -> perct
        )
        // num checklist
        perct = if(rs.numContract._2.prev.toLong == 0) "NaN" else CommonService.percentDouble(rs.numContract._2.curr.toDouble, rs.numContract._2.prev.toDouble).toString
        val numCL = Json.obj(
          "curr"       -> CommonService.formatPattern(rs.numContract._2.curr),
          "prev"       -> CommonService.formatPattern(rs.numContract._2.prev),
          "perct"      -> perct
        )
        // Process Time checklist 75%
        perct = if(rs.numContract._3.curr == 0) "NaN" else CommonService.percentDouble(rs.numContract._3.avg, rs.numContract._3.curr).toString
        val timeCL = Json.obj(
          "curr"       -> CommonService.formatPatternDouble(rs.numContract._3.avg),
          "prev"       -> CommonService.formatPatternDouble(rs.numContract._3.curr),
          "perct"      -> perct
        )
        // Churn rate have checklist
        perct = if(rs.churnRate.curr == 0) "NaN" else CommonService.percentDouble(rs.churnRate.avg, rs.churnRate.curr).toString
        val churnRateCL = Json.obj(
          "curr"       -> CommonService.formatPatternDouble(rs.churnRate.avg),
          "prev"       -> CommonService.formatPatternDouble(rs.churnRate.curr),
          "perct"      -> perct
        )
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
        val json = Json.obj(
          "contractCL" -> contractCL,
          "numCL"  -> numCL,
          "timeCL" -> timeCL,
          "churnRateCL" -> churnRateCL,
          "churn1" -> churn1,
          "churn2" -> churn2,
          "lastYYYY"  -> CommonService.getPrevYYYY(rs.month)
        )
        Ok(Json.toJson(json))
    }
    catch{
      case e: Exception => Ok("Error")
    }
  }

  def index() =  withAuth {username => implicit request: Request[AnyContent] =>
    try {
      val rs = CacheService.getChecklistResponse(null)
      Ok(churn.views.html.checklist.index(rs, rs.month, username, rs.trendChecklist.map(x=> x._1).sorted, churn.controllers.routes.ChecklistController.index()))
    }
    catch {
      case e: Exception => Ok(churn.views.html.checklist.index(null, CommonService.getPrevMonth(), username, null, churn.controllers.routes.ChecklistController.index()))
    }
  }

  def getJsonByTab() = withAuth {username => implicit request: Request[AnyContent] =>
    try{
      val tabName = request.body.asFormUrlEncoded.get("tabName").head
      tabName match {
        // tab Region trending
        case "tabRegion" => {
          val rs = CacheService.getChecklistRegionResponse(request, username)
          // Number of Contracts That Have Checklist(s) by Region (%) 2-3 sai
          val heatmapCL = Json.obj(
            "cateX" -> rs.checklistRegion._1,
            "cateY" -> rs.checklistRegion._2.map(x=>x._2).distinct.sorted,
            "data"  -> rs.checklistRegion._2,
            "dataX" -> rs.checklistRegion._2.map(x=>x._1).distinct.sorted,
            "dataY" -> rs.checklistRegion._2.map(x=>x._2).distinct.sorted
          )
          // bubble charts Region and Month
          val minPercRegionMonth = if(rs.trendRegionMonth._3.length > 0) CommonService.format2Decimal(rs.trendRegionMonth._3.map(x=>x._4).min) else 0
          val maxPercRegionMonth = if(rs.trendRegionMonth._3.length > 0) CommonService.format2Decimal(rs.trendRegionMonth._3.map(x=>x._4).max) else 0
          val isNullRegionMonth  = if(rs.trendRegionMonth._3.length > 0 && rs.trendRegionMonth._3.map(x=>x._4).min == 0 && rs.trendRegionMonth._3.map(x=>x._3).min == 0) 1 else 0
          val churnRegionMonth = Json.obj(
            "catesRegion" -> rs.trendRegionMonth._1,
            "catesMonth"  -> rs.trendRegionMonth._2,
            "data"       -> rs.trendRegionMonth._3,
            "minPercent" -> minPercRegionMonth,
            "maxPercent" -> maxPercRegionMonth,
            "isNull"     -> isNullRegionMonth
          )
          // bubble charts Package & Month
          val minPercTBT = if(rs.trendTBTMonth._3.length > 0) CommonService.format2Decimal(rs.trendTBTMonth._3.map(x=>x._4).min) else 0
          val maxPercTBT = if(rs.trendTBTMonth._3.length > 0) CommonService.format2Decimal(rs.trendTBTMonth._3.map(x=>x._4).max) else 0
          val isNullTBT  = if(rs.trendTBTMonth._3.length > 0 && rs.trendTBTMonth._3.map(x=>x._4).min == 0 && rs.trendTBTMonth._3.map(x=>x._3).min == 0) 1 else 0
          val churnTBT = Json.obj(
            "catesTBT" -> rs.trendTBTMonth._1,
            "catesMonth"  -> rs.trendTBTMonth._2,
            "data"       -> rs.trendTBTMonth._3,
            "minPercent" -> minPercTBT,
            "maxPercent" -> maxPercTBT,
            "isNull"     -> isNullTBT
          )
          // sparkline table
          val tbTBT = Json.obj(
            "name" -> rs.tbTBT._1,
            "data" -> rs.tbTBT._2,
            "arrMonth" -> rs.trendTBTMonth._2.map(x=> x._1).toArray.sorted
          )
          val json = Json.obj(
            "churnRegionMonth" -> churnRegionMonth,
            "heatmapCL" -> heatmapCL,
            "churnTBTMonth"   -> churnTBT,
            "tbTBT"     -> tbTBT,
            "cmtChart"  -> rs.comments
          )
          Ok(Json.toJson(json))
        }
        case "tabAge" => {
          val rs = CacheService.getChecklistAgeResponse(request, username)
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
          val heatmapCL = Json.obj(
            "cateX" -> rs.cLRegionAge._1,
            "cateY" -> rs.cLRegionAge._2.map(x=>x._2).distinct.sorted,
            "data"  -> rs.cLRegionAge._2,
            "dataX" -> rs.cLRegionAge._2.map(x=>x._1).distinct.sorted,
            "dataY" -> rs.cLRegionAge._2.map(x=>x._2).distinct.sorted
          )
          val json = Json.obj(
            "cmtChart"  -> rs.comments,
            "cLRegionAge" -> heatmapCL,
            "trendAgeLocation" -> churnLocation
          )
          Ok(Json.toJson(json))
        }
        case "tabTime" => {
          val rs = CacheService.getChecklistTimeResponse(request, username)
          // bubble charts Time & Location
          val minPercLocation = if(rs.trendTimeLocation._3.length > 0) CommonService.format2Decimal(rs.trendTimeLocation._3.map(x=>x._4).min) else 0
          val maxPercLocation = if(rs.trendTimeLocation._3.length > 0) CommonService.format2Decimal(rs.trendTimeLocation._3.map(x=>x._4).max) else 0
          val isNullLocation  = if(rs.trendTimeLocation._3.length > 0 && rs.trendTimeLocation._3.map(x=>x._4).min == 0 && rs.trendTimeLocation._3.map(x=>x._3).min == 0) 1 else 0
          val trendTimeLocation = Json.obj(
            "catesRegion" -> rs.trendTimeLocation._1,
            "catesMonth"  -> rs.trendTimeLocation._2,
            "data"       -> rs.trendTimeLocation._3,
            "minPercent" -> minPercLocation,
            "maxPercent" -> maxPercLocation,
            "isNull"     -> isNullLocation
          )
          val cLRegionTime = Json.obj(
            "cateX" -> rs.cLRegionTime._1,
            "cateY" -> rs.cLRegionTime._2,
            "data"  -> rs.cLRegionTime._3
          )
          val json = Json.obj(
            "cmtChart"  -> rs.comments,
            "cLRegionTime" -> cLRegionTime,
            "trendTimeLocation" -> trendTimeLocation
          )
          Ok(Json.toJson(json))
        }
        case "tabCause" => {
          val rs = CacheService.getChecklistCauseResponse(request, username)
          // bubble charts Cause & Location
          val minPercLocCause = if(rs.trendCauseLocation._3.length > 0) CommonService.format2Decimal(rs.trendCauseLocation._3.map(x=>x._4).min) else 0
          val maxPercLocCause = if(rs.trendCauseLocation._3.length > 0) CommonService.format2Decimal(rs.trendCauseLocation._3.map(x=>x._4).max) else 0
          val isNullLocCause  = if(rs.trendCauseLocation._3.length > 0 && rs.trendCauseLocation._3.map(x=>x._4).min == 0 && rs.trendCauseLocation._3.map(x=>x._3).min == 0) 1 else 0
          val trendCauseLocation = Json.obj(
            "catesRegion" -> rs.trendCauseLocation._1,
            "catesMonth"  -> rs.trendCauseLocation._2,
            "data"       -> rs.trendCauseLocation._3,
            "minPercent" -> minPercLocCause,
            "maxPercent" -> maxPercLocCause,
            "isNull"     -> isNullLocCause
          )
          val cLRegionCause = Json.obj(
            "cateX" -> rs.cLRegionCause._1,
            "cateY" -> rs.cLRegionCause._2,
            "data"  -> rs.cLRegionCause._3
          )
          // bubble charts Position & Location
          val minPercLocPosition = if(rs.trendPositionLocation._3.length > 0) CommonService.format2Decimal(rs.trendPositionLocation._3.map(x=>x._4).min) else 0
          val maxPercLocPosition = if(rs.trendPositionLocation._3.length > 0) CommonService.format2Decimal(rs.trendPositionLocation._3.map(x=>x._4).max) else 0
          val isNullLocPosition  = if(rs.trendPositionLocation._3.length > 0 && rs.trendPositionLocation._3.map(x=>x._4).min == 0 && rs.trendPositionLocation._3.map(x=>x._3).min == 0) 1 else 0
          val trendPositionLocation = Json.obj(
            "catesRegion" -> rs.trendPositionLocation._1,
            "catesMonth"  -> rs.trendPositionLocation._2,
            "data"       -> rs.trendPositionLocation._3,
            "minPercent" -> minPercLocPosition,
            "maxPercent" -> maxPercLocPosition,
            "isNull"     -> isNullLocPosition
          )
          val cLRegionPosition = Json.obj(
            "cateX" -> rs.cLRegionPosition._1,
            "cateY" -> rs.cLRegionPosition._2,
            "data"  -> rs.cLRegionPosition._3
          )
          val json = Json.obj(
            "cmtChart"  -> rs.comments,
            "trendCauseLocation" -> trendCauseLocation,
            "cLRegionCause" -> cLRegionCause,
            "trendPositionLocation" -> trendPositionLocation,
            "cLRegionPosition" -> cLRegionPosition
          )
          Ok(Json.toJson(json))
        }
      }
    }
    catch{
      case e: Exception => Ok("Error")
    }
  }
}
