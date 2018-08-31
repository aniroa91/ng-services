package churn.controllers

import play.api.data.Form
import play.api.data.Forms.mapping
import play.api.data.Forms.text
import javax.inject.Inject
import javax.inject.Singleton

import play.api.mvc._
import churn.controllers.AuthenticatedRequest
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
import service.ChurnAgeService

/**
  * This controller creates an `Action` to handle HTTP requests to the
  * application's home page.
  */
case class AuthenticatedRequest (val username: String, request: Request[AnyContent]) extends WrappedRequest(request)

@Singleton
class ChurnAgeController @Inject() (cc: ControllerComponents) extends AbstractController(cc) with Secured{

  /* Authentication action*/
  def Authenticated(f: AuthenticatedRequest => Result) = {
    Action { request =>
      val username = request.session.get("username").get.toString
      username match {
        case "admin" =>
          f(AuthenticatedRequest(username, request))
        case none =>
          Redirect("/").withNewSession.flashing(
            "success" -> "You are now logged out."
          )
      }
    }
  }

  def index() = Authenticated { implicit request: Request[AnyContent] =>
    val month = CommonService.getPrevMonth()
    val rs = ChurnAgeService.getInternet(null)
    Ok(churn.views.html.age.index(rs,month,request.session.get("username").get.toString, churn.controllers.routes.ChurnAgeController.index()))
  }

  def getJsonByStatus(status: String,month: String) = Action { implicit request =>
    try{
      val jSon = if(month.equals("")) ChurnAgeService.getChurn("2018-03") else ChurnAgeService.getChurn(month)
      val rsChurn = ChurnAgeService.calChurnRateAndPercentageForChurnbyStatus(jSon,status.toInt)
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

  def getJsonChurn() = Action { implicit request: Request[AnyContent] =>
   // try{
      val rs = ChurnAgeService.getInternet(request)
      val regionAge = Json.obj(
        "data"       -> rs._1,
        "minPercent" -> CommonService.format3Decimal(rs._1.map(x=>x._4).min),
        "maxPercent" -> CommonService.format3Decimal(rs._1.map(x=>x._4).max)
      )
      val rsAge = Json.obj(
        "churnRate"    -> rs._2.map(x=> x._2),
        "churnPercent" -> rs._2.map(x=> x._3)
      )
      val rsMonth = Json.obj(
        "churnRate"    -> rs._3.map(x=> x._2),
        "churnPercent" -> rs._3.map(x=> x._3)
      )
      val json = Json.obj(
        "churnAge"       -> rsAge,
        "churnMonth"     -> rsMonth,
        "churnRegionAge" -> regionAge
      )
      Ok(Json.toJson(json))
    /*}
    catch{
      case e: Exception => Ok("Error")
    }*/

  }

  def getChurnByRegionAge(age: String,region: String) = Action { implicit request =>
    try{
     /* val jsChurn1 = Json.obj(
        "churnRate" -> ChurnAgeService.calChurnRateAndPercentageByAge(ChurnAgeService.getChurnByMonth(s"$age AND $region")).map(x=> x._2),
        "churnPercent" -> ChurnAgeService.calChurnRateAndPercentageByAge(ChurnAgeService.getChurnByMonth(s"$age AND $region")).map(x=> x._3),
        "categories" -> ChurnAgeService.calChurnRateAndPercentageByAge(ChurnAgeService.getChurnByMonth(s"$age AND $region")).map(x=> AgeGroupUtil.getIdAge(x._1))
      )
      val jsChurn2 = Json.obj(
        "churnRate" -> ChurnAgeService.calChurnRateAndPercentageByMonth(ChurnAgeService.getChurnByMonth(s"$age AND $region")).map(x=> x._2),
        "churnPercent" -> ChurnAgeService.calChurnRateAndPercentageByMonth(ChurnAgeService.getChurnByMonth(s"$age AND $region")).map(x=> x._3),
        "min" -> ChurnAgeService.calChurnRateAndPercentageByMonth(ChurnAgeService.getChurnByMonth(s"$age AND $region")).map(x=> x._3).min,
        "max" -> ChurnAgeService.calChurnRateAndPercentageByMonth(ChurnAgeService.getChurnByMonth(s"$age AND $region")).map(x=> x._3).max
      )

      val jsChurn = Json.obj(
        "jsChurn1" -> jsChurn1,
        "jsChurn2" -> jsChurn2

      )
      Ok(Json.toJson(jsChurn))*/
      Ok("ok")
    }
    catch{
      case e: Exception => Ok("Error")
    }
  }

  def getJsonByMonth(status: String,month: String) = Action { implicit request =>
    try{
      /*val jSon = ChurnAgeService.getChurn(month)
      var rsChurn = ChurnAgeService.calChurnRateAndPercentageForChurnbyStatus(jSon,status.toInt)
      val jsChurn = Json.obj(
        "churnBubbleChart" -> rsChurn,
        "minPercent" -> CommonService.format3Decimal(rsChurn.map(x=>x._4).asInstanceOf[Array[(Double)]].min),
        "maxPercent" -> CommonService.format3Decimal(rsChurn.map(x=>x._4).asInstanceOf[Array[(Double)]].max),
        "churnRate" -> ChurnAgeService.calChurnRateAndPercentageByAge(ChurnAgeService.getChurnByMonth(s"month:$month")).map(x=> x._2),
        "churnPercent" -> ChurnAgeService.calChurnRateAndPercentageByAge(ChurnAgeService.getChurnByMonth(s"month:$month")).map(x=> x._3),
        "minPercent" -> ChurnAgeService.calChurnRateAndPercentageByAge(ChurnAgeService.getChurnByMonth(s"month:$month")).map(x=> x._3).min,
        "maxPercent" -> ChurnAgeService.calChurnRateAndPercentageByAge(ChurnAgeService.getChurnByMonth(s"month:$month")).map(x=> x._3).max
      )
      Ok(Json.toJson(jsChurn))*/
      Ok("ok")
    }
    catch{
      case e: Exception => Ok("Error")
    }
  }

  def getJsonByAge(query: String) = Action { implicit request =>
    try{
     /* val jSon = ChurnAgeService.getChurnByMonth(query)
      val jsChurn = Json.obj(
        "churnRate" -> ChurnAgeService.calChurnRateAndPercentageByMonth(jSon).map(x=> x._2),
        "churnPercent" -> ChurnAgeService.calChurnRateAndPercentageByMonth(jSon).map(x=> x._3)

      )
      Ok(Json.toJson(jsChurn))*/
      Ok("ok")
    }
    catch{
      case e: Exception => Ok("Error")
    }
  }

}
