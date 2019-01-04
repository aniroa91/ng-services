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
import service.ChurnDetectService
import services.domain.CommonService

/**
  * This controller creates an `Action` to handle HTTP requests to the
  * application's home page.
  */

@Singleton
class ChurnDetectController @Inject() (cc: ControllerComponents) extends AbstractController(cc) with Secured{

  def index() = withAuth {username => implicit request: Request[AnyContent] =>
    val month = CommonService.getPrevMonth()
    try {
      val rs = ChurnDetectService.getInternet(request, 0)
      Ok(churn.views.html.detect.index(rs, rs.linkFilters.get("month").get, username, churn.controllers.routes.ChurnDetectController.index()))
    }
    catch {
      case e: Exception => Ok(churn.views.html.detect.index(null, month, username, churn.controllers.routes.ChurnDetectController.index()))
    }
  }

  def exportCSV() = withAuth {username => implicit request: Request[AnyContent] =>
    try{
     /* val contractGrp = request.body.asFormUrlEncoded.get("groupCt").head
      val maintain = request.body.asFormUrlEncoded.get("maintain").head
      val cate = if(request.body.asFormUrlEncoded.get("cate").head == "") "" else "\"" + request.body.asFormUrlEncoded.get("cate").head + "\""
      val cause = if(request.body.asFormUrlEncoded.get("cause").head == "") "" else "\"" + request.body.asFormUrlEncoded.get("cause").head + "\""
      val complain = if(request.body.asFormUrlEncoded.get("complain").head == "*") "*" else "problem:\"" + request.body.asFormUrlEncoded.get("complain").head + "\""
      val status = "status:"+request.body.asFormUrlEncoded.get("status").head */
      val month = request.body.asFormUrlEncoded.get("month").head
      val packages = if(request.body.asFormUrlEncoded.get("package").head == "*") "*" else "\"" +request.body.asFormUrlEncoded.get("package").head +"\""
      val region = request.body.asFormUrlEncoded.get("region").head
      val age = request.body.asFormUrlEncoded.get("age").head
      val _typeCsv = request.body.asFormUrlEncoded.get("_typeCsv").head
      val arrContracts = if(_typeCsv == "callog") ChurnDetectService.getTopCallContent(s"lifeGroup:$age AND region:$region AND tenGoi:$packages", month, 3000, "*", "", "", "")
          else ChurnDetectService.getTopChecklistContent(s"lifeGroup:$age AND region:$region AND tenGoi:$packages", month, 3000, "*", "", "", "")
      val isLimit = if(arrContracts.length > 3000) 1 else 0
      val rs = Json.obj(
        "data"  -> arrContracts.sortWith((x, y) => x._2 > y._2).slice(0, 3000),
        "limit" -> isLimit
      )
      Ok(Json.toJson(rs))
    }
    catch{
      case e: Exception => Ok("Error")
    }
  }

  def getJsonChurn() = withAuth {username => implicit request: Request[AnyContent] =>
    try{
      val rs = ChurnDetectService.getInternet(request, 1)
      val churn1 = Json.obj(
        "numCt"     -> CommonService.formatPattern(rs.cardMetrics._1),
        "churnCt"   -> CommonService.formatPattern(rs.cardMetrics._2),
        "churnRate" -> CommonService.format3Decimal(if(rs.cardMetrics._1 != 0) rs.cardMetrics._2 * 100.0 / rs.cardMetrics._1 else 0),
        "churnPert" -> CommonService.format3Decimal(if(rs.cardMetrics._3 != 0) rs.cardMetrics._2 * 100.0 / rs.cardMetrics._3 else 0),
        "f1score"   -> CommonService.format3Decimal(if(rs.cardMetrics._1 !=0 && rs.cardMetrics._2 != 0) 2.00 * (rs.cardMetrics._2 * 100.0 / rs.cardMetrics._1 * rs.cardMetrics._3 * 100.0 / rs.cardMetrics._2) / (rs.cardMetrics._2 * 100.0 / rs.cardMetrics._1 + rs.cardMetrics._3 * 100.0 / rs.cardMetrics._2) else 0),
        "numCall"   -> CommonService.formatPattern(rs.cardMetrics._4),
        "numCheck"  -> CommonService.formatPattern(rs.cardMetrics._5)
      )
      val churn2 = Json.obj(
        "CheckYesCallYes" -> CommonService.formatPattern(rs.numCallChecklist._1.toInt),
        "CheckNoCallYes"  -> CommonService.formatPattern(rs.numCallChecklist._2.toInt),
        "CheckYesCallNo"  -> CommonService.formatPattern(rs.numCallChecklist._3.toInt),
        "CheckNoCallNo"   -> CommonService.formatPattern(rs.numCallChecklist._4.toInt),
        "CallYes"         -> CommonService.formatPattern(rs.numCallChecklist._1.toInt + rs.numCallChecklist._2.toInt),
        "CallNo"          -> CommonService.formatPattern(rs.numCallChecklist._4.toInt + rs.numCallChecklist._3.toInt),
        "total"           -> CommonService.formatPattern(rs.numCallChecklist._1.toInt +rs.numCallChecklist._2.toInt+rs.numCallChecklist._3.toInt+rs.numCallChecklist._4.toInt)
      )
      val churn3 = Json.obj(
        "data"      -> rs.complain,
        "drilldown" -> rs.complain.slice(11, rs.complain.length)
      )
      val churn4 = Json.obj(
        "data"      -> rs.cates
      )
      val churn5 = Json.obj(
        "xAxis" -> rs.medianHours.map(x=> x._1),
        "data"  -> rs.medianHours.map(x=> x._2)
    )
      val churn6 = Json.obj(
        "xAxis"  -> rs.numCauses.map(x=> x._1),
        "data"   -> rs.numCauses.map(x=> x._2)
      )
      val churn7 = Json.obj(
        "xAxis"     -> rs.medianMaintain.map(x=> x._1),
        "median"    -> rs.medianMaintain.map(x=> x._2),
        "contract"  -> rs.medianMaintain.map(x=> x._3)
      )
      val churn8 = Json.obj(
        "data"      -> rs.overall._1.map(x=> ("Vung "+x._1, x._2))
      )
      val churn9 = Json.obj(
        "data"      -> rs.overall._2
      )
      val churn10 = Json.obj(
        "data"      -> rs.overall._3
      )
      val tb12 = Json.obj(
        "data"  -> rs.topCallContent
      )
      val tb13 = Json.obj(
        "data"  -> rs.topChecklistContent
      )
      val splot1 = Json.obj(
        "xAxis"  -> rs.indicators.infErrors.map(x=> x._1).slice(0, rs.indicators.infErrors.length-1),
        "churn"  -> rs.indicators.infErrors.map(x=> x._3).slice(0, rs.indicators.infErrors.length-1),
        "all"    -> rs.indicators.infErrors.map(x=> x._2).slice(0, rs.indicators.infErrors.length-1)
      )
      val splot2 = Json.obj(
        "xAxis"  -> rs.indicators.signin.map(x=> x._1).slice(0, rs.indicators.signin.length-1),
        "churn"  -> rs.indicators.signin.map(x=> x._3).slice(0, rs.indicators.signin.length-1),
        "all"    -> rs.indicators.signin.map(x=> x._2).slice(0, rs.indicators.signin.length-1)
      )
      val splot3 = Json.obj(
        "xAxis"  -> rs.indicators.suyhao.map(x=> x._1).slice(0, rs.indicators.suyhao.length-1),
        "churn"  -> rs.indicators.suyhao.map(x=> x._3).slice(0, rs.indicators.suyhao.length-1),
        "all"    -> rs.indicators.suyhao.map(x=> x._2).slice(0, rs.indicators.suyhao.length-1)
      )
      val splot4 = Json.obj(
        "xAxis"  -> rs.indicators.download.map(x=> x._1).slice(0, rs.indicators.download.length-1),
        "churn"  -> rs.indicators.download.map(x=> x._3).slice(0, rs.indicators.download.length-1),
        "all"    -> rs.indicators.download.map(x=> x._2).slice(0, rs.indicators.download.length-1)
      )
      val splot5 = Json.obj(
        "xAxis"  -> rs.indicators.fee.map(x=> x._1).slice(0, rs.indicators.fee.length-1),
        "churn"  -> rs.indicators.fee.map(x=> x._3).slice(0, rs.indicators.fee.length-1),
        "all"    -> rs.indicators.fee.map(x=> x._2).slice(0, rs.indicators.fee.length-1)
      )
      val indicators = Json.obj(
        "errChurn"    -> rs.indicators.infErrors(rs.indicators.infErrors.length-1)._3,
        "errAll"      -> rs.indicators.infErrors(rs.indicators.infErrors.length-1)._2,
        "signinChurn" -> rs.indicators.signin(rs.indicators.signin.length-1)._3,
        "signinAll"   -> rs.indicators.signin(rs.indicators.signin.length-1)._2,
        "suyhaoChurn" -> rs.indicators.suyhao(rs.indicators.suyhao.length-1)._3,
        "suyhaoAll"   -> rs.indicators.suyhao(rs.indicators.suyhao.length-1)._2,
        "downChurn"    -> CommonService.formatPattern(rs.indicators.download(rs.indicators.download.length-1)._3.toInt),
        "downAll"      -> CommonService.formatPattern(rs.indicators.download(rs.indicators.download.length-1)._2.toInt),
        "feeChurn"    -> CommonService.formatPattern(rs.indicators.fee(rs.indicators.fee.length-1)._3.toInt),
        "feeAll"      -> CommonService.formatPattern(rs.indicators.fee(rs.indicators.fee.length-1)._2.toInt),
        "downChurn"   -> rs.indicators.infErrors(rs.indicators.infErrors.length-1)._3,
        "downAll"     -> rs.indicators.infErrors(rs.indicators.infErrors.length-1)._3
      )
      val json = Json.obj(
        "churn1"       -> churn1,
        "churn2"       -> churn2,
        "churn3"       -> churn3,
        "churn4"       -> churn4,
        "churn5"       -> churn5,
        "churn6"       -> churn6,
        "churn7"       -> churn7,
        "churn8"       -> churn8,
        "churn9"       -> churn9,
        "churn10"      -> churn10,
        "tb12"         -> tb12,
        "tb13"         -> tb13,
        "indicators"   -> indicators,
        "splot1"       -> splot1,
        "splot2"       -> splot2,
        "splot3"       -> splot3,
        "splot4"       -> splot4,
        "splot5"       -> splot5
      )
      Ok(Json.toJson(json))
    }
    catch{
      case e: Exception => Ok("Error")
    }

  }

  def getFormContract(contract: String) = withAuth {username => implicit request: Request[AnyContent] =>
    val key1    = if(contract.indexOf("_")>=0) contract.split("_")(0).split(":")(0) else contract.split(":")(0)
    val values1 = if(contract.indexOf("_")>=0) contract.split("_")(0).split(":")(1) else contract.split(":")(1)
    val key2    = if(contract.indexOf("_")>=0) contract.split("_")(1).split(":")(0) else ""
    val values2 = if(contract.indexOf("_")>=0) contract.split("_")(1).split(":")(1) else ""
    println(key1 +" vs "+ values1)
    Redirect(churn.controllers.routes.ChurnDetectController.index()).flashing( key1 -> values1, key2 -> values2)
  }
}
