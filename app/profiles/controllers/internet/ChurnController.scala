package profile.controllers.internet

import play.api.data.Form
import play.api.data.Forms.mapping
import play.api.data.Forms.text
import javax.inject.Inject
import javax.inject.Singleton

import play.api.mvc._
import controllers.AuthenticatedRequest
import play.api.mvc.AbstractController
import play.api.mvc.ControllerComponents
import profiles.utils.AgeGroupUtil
import services.domain.CommonService
//import controllers.SearchContract
import controllers.Secured
import com.sksamuel.elastic4s.http.HttpClient
import com.sksamuel.elastic4s.ElasticsearchClientUri
import services.Configure
import play.api.libs.json.Json
import com.sksamuel.elastic4s.http.ElasticDsl._
import profile.services.internet.HistoryService
import com.ftel.bigdata.utils.StringUtil
import profile.services.internet.response.History
import profile.services.internet.response.Hourly
import controllers.InternetContract

import service.DevService
import service.DevService.bubbleHeatChart

/**
  * This controller creates an `Action` to handle HTTP requests to the
  * application's home page.
  */
@Singleton
class ChurnController @Inject() (cc: ControllerComponents) extends AbstractController(cc) with Secured{

  /* Authentication action*/
  def Authenticated(f: AuthenticatedRequest => Result) = {
    Action { request =>
      val username = request.session.get("username").get.toString
      username match {
        case "btgd@ftel" =>
          f(AuthenticatedRequest(username, request))
        case none =>
          Redirect("/").withNewSession.flashing(
            "success" -> "You are now logged out."
          )
      }
    }
  }

  def index() = Authenticated { implicit request =>
    // get churn contract response
    var month = "2018-03"
    val rs = DevService.getInternet(month)
    Ok(profiles.views.html.internet.churn.index(rs,month,request.session.get("username").get.toString))
  }

  def getJsonByStatus(status: String,month: String) = Action { implicit request =>
    try{
      val jSon = if(month.equals("")) DevService.getChurn("2018-03") else DevService.getChurn(month)
      var rsChurn = DevService.calChurnRateAndPercentageForChurnbyStatus(jSon,status.toInt)
      val jsChurn = Json.obj(
        "churnRate" -> rsChurn,
        "minPercent" -> CommonService.format3Decimal(rsChurn.map(x=>x._4).asInstanceOf[Array[(Double)]].min),
        "maxPercent" -> CommonService.format3Decimal(rsChurn.map(x=>x._4).asInstanceOf[Array[(Double)]].max)
      )
      Ok(Json.toJson(jsChurn))
    }
    catch{
      case e: Exception => Ok("Error")
    }
  }

  def getChurnByRegionAge(age: String,region: String) = Action { implicit request =>
    try{
      val jsChurn1 = Json.obj(
        "churnRate" -> DevService.calChurnRateAndPercentageByAge(DevService.getChurnByMonth(s"$age AND $region")).map(x=> x._2),
        "churnPercent" -> DevService.calChurnRateAndPercentageByAge(DevService.getChurnByMonth(s"$age AND $region")).map(x=> x._3),
        "categories" -> DevService.calChurnRateAndPercentageByAge(DevService.getChurnByMonth(s"$age AND $region")).map(x=> AgeGroupUtil.getIdAge(x._1))
      )
      val jsChurn2 = Json.obj(
        "churnRate" -> DevService.calChurnRateAndPercentageByMonth(DevService.getChurnByMonth(s"$age AND $region")).map(x=> x._2),
        "churnPercent" -> DevService.calChurnRateAndPercentageByMonth(DevService.getChurnByMonth(s"$age AND $region")).map(x=> x._3),
        "min" -> DevService.calChurnRateAndPercentageByMonth(DevService.getChurnByMonth(s"$age AND $region")).map(x=> x._3).min,
        "max" -> DevService.calChurnRateAndPercentageByMonth(DevService.getChurnByMonth(s"$age AND $region")).map(x=> x._3).max
      )

      val jsChurn = Json.obj(
        "jsChurn1" -> jsChurn1,
        "jsChurn2" -> jsChurn2

      )
      Ok(Json.toJson(jsChurn))
    }
    catch{
      case e: Exception => Ok("Error")
    }
  }

  def getJsonByMonth(status: String,month: String) = Action { implicit request =>
    try{
      val jSon = DevService.getChurn(month)
      var rsChurn = DevService.calChurnRateAndPercentageForChurnbyStatus(jSon,status.toInt)
      val jsChurn = Json.obj(
        "churnBubbleChart" -> rsChurn,
        "minPercent" -> CommonService.format3Decimal(rsChurn.map(x=>x._4).asInstanceOf[Array[(Double)]].min),
        "maxPercent" -> CommonService.format3Decimal(rsChurn.map(x=>x._4).asInstanceOf[Array[(Double)]].max),
        "churnRate" -> DevService.calChurnRateAndPercentageByAge(DevService.getChurnByMonth(s"month:$month")).map(x=> x._2),
        "churnPercent" -> DevService.calChurnRateAndPercentageByAge(DevService.getChurnByMonth(s"month:$month")).map(x=> x._3),
        "minPercent" -> DevService.calChurnRateAndPercentageByAge(DevService.getChurnByMonth(s"month:$month")).map(x=> x._3).min,
        "maxPercent" -> DevService.calChurnRateAndPercentageByAge(DevService.getChurnByMonth(s"month:$month")).map(x=> x._3).max
      )
      Ok(Json.toJson(jsChurn))
    }
    catch{
      case e: Exception => Ok("Error")
    }
  }

  def getJsonByAge(query: String) = Action { implicit request =>
    try{
      val jSon = DevService.getChurnByMonth(query)
      val jsChurn = Json.obj(
        "churnRate" -> DevService.calChurnRateAndPercentageByMonth(jSon).map(x=> x._2),
        "churnPercent" -> DevService.calChurnRateAndPercentageByMonth(jSon).map(x=> x._3)

      )
      Ok(Json.toJson(jsChurn))
    }
    catch{
      case e: Exception => Ok("Error")
    }
  }

}
