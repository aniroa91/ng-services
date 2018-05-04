package controllers

import javax.inject.Inject
import javax.inject.Singleton

import play.api.libs.json.Json
import model.paytv.PaytvResponse
import play.api.mvc.AbstractController
import play.api.mvc.ControllerComponents
import services.domain.CommonService
import play.api.mvc._
import profile.utils.CommonUtil
import service.DevService

/**
  * This controller creates an `Action` to handle HTTP requests to the
  * application's home page.
  */
@Singleton
class PaytvClientController @Inject() (cc: ControllerComponents) extends AbstractController(cc) with Secured{

  /* Authentication action*/
  def Authenticated(f: AuthenticatedRequest => Result) = {
    Action { request =>
      val username = request.session.get("username").get.toString
      username match {
        case "btgd@ftel" =>
          f(AuthenticatedRequest(username, request))
        case none =>
          Redirect(routes.LoginController.index).withNewSession.flashing(
            "success" -> "You are now logged out."
          )
      }
    }
  }

  def index() = Authenticated { implicit request =>
    // get paytv contract response
    //val response = PaytvService.getContractResponse(null)
    //DevService.get().status.foreach(println)
    val rs = DevService.get()

    Ok(profiles.views.html.paytv.index(rs,request.session.get("username").get.toString))
  }

  def drilldownJson(code: String) = Action { implicit request =>
    try{
      println(code)
      val jSon = DevService.get(code)
      println(jSon.usage.filter(x => x._2 == "usage").map(x=>x._3))

      val jsBras = Json.obj(
        // Status Contract
        "statusContract" -> jSon.status,
        "monthStatus" -> jSon.status.map(x=> x._1).asInstanceOf[Array[(String)]].distinct.sorted,
        "keyCodeStatus" -> jSon.status.map(x=> x._2).asInstanceOf[Array[(String)]].distinct,

        // Name Contract
        "nameContract" -> jSon.tengoi,
        "keyName" -> jSon.tengoi.map(x=> x._2).asInstanceOf[Array[(String)]].distinct,

        // Province Contract
        "provinceContract" -> jSon.province,
        "keyProvince" -> jSon.province.map(x=> x._2).asInstanceOf[Array[(String)]].distinct,

        // UsageQuantile
        "dtaUsageQuantile" -> jSon.usageQuantile.map(x=>x._2),
        // LteQuantile
        "dtaLteQuantile" -> jSon.lteQuantile.map(x=> x._2),
        // Usage Bar chart
        "maxRange" -> jSon.usage.filter(x => x._2 == "usage").map(x=>x._3).asInstanceOf[Array[(Int)]].max,
        "no_usage" -> jSon.usage.filter(x => x._2 == "no_usage").map(x => -x._3),
        "usage" -> jSon.usage.filter(x => x._2 == "usage").map(x => x._3)

      )
      Ok(Json.toJson(jsBras))
    }
    catch{
      case e: Exception => Ok("Error")
    }
  }

}
