package controllers

import javax.inject.Inject
import javax.inject.Singleton

import play.api.libs.json.Json
import model.paytv.PaytvResponse
import play.api.mvc.AbstractController
import play.api.mvc.ControllerComponents
import services.domain.CommonService
import play.api.mvc._
import profile.model.PayTVResponse
import profile.utils.CommonUtil
import service.DevService

/**
  * This controller creates an `Action` to handle HTTP requests to the
  * application's home page.
  */
case class AuthenticatedRequest (val username: String, request: Request[AnyContent])
  extends WrappedRequest(request)

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
    val rs = DevService.get()
    val arr = rs.getTenGoiForSparkline()
    val arrOthersSparklines = arr.map(x=> x._3)

    val contractOthers = arr.map(x=>x._2)
    var sumOthercontract = 0
    for(i <- 11 until contractOthers.length){
      sumOthercontract += contractOthers(i)
    }
    var others = arrOthersSparklines(11)
    for(i <- 12 until arrOthersSparklines.length){
      others = others.zip(arrOthersSparklines(i)).map { case (x, y) => x + y }
    }
    Ok(profiles.views.html.paytv.index(rs,(sumOthercontract,others),request.session.get("username").get.toString))
  }

  def drilldownJson(code: String) = Action { implicit request =>
    try{
      //println(code)
      var isRegion = 1
      if(code.indexOf("Region")<0) isRegion = 0
      val jSon = DevService.get(code,isRegion)

      val jsBras = Json.obj(
        // Status Contract
        "statusContract" -> jSon.status,
        "monthStatus" -> jSon.status.map(x=> x._1).asInstanceOf[Array[(String)]].distinct.sorted,
        "keyCodeStatus" -> jSon.status.map(x=> x._2).asInstanceOf[Array[(String)]].distinct,

        // Name Contract
        "nameContract" -> jSon.tengoi,
        "keyName" -> jSon.tengoi.map(x=> x._2).asInstanceOf[Array[(String)]].distinct,

        // Province or Region Contract
        "provinceContract" -> jSon.regions,
        "keyProvince" -> jSon.regions.map(x=> x._2).asInstanceOf[Array[(String)]].distinct.sorted,

        // UsageQuantile
        "dtaUsageQuantile" -> jSon.usageQuantile.map(x=>x._2),
        // LteQuantile
        "dtaLteQuantile" -> jSon.lteQuantile.map(x=> x._2),
        // sparkline
        "tenGoiForSparkline" -> jSon.getTenGoiForSparkline(),
        // Usage Bar chart
        "range0" -> jSon.usage.filter(x => x._2 == "range0").map(x => x._3),
        "range1" -> jSon.usage.filter(x => x._2 == "range1").map(x => x._3),
        "range2" -> jSon.usage.filter(x => x._2 == "range2").map(x => x._3),
        "range3" -> jSon.usage.filter(x => x._2 == "range3").map(x => x._3)
      )
      Ok(Json.toJson(jsBras))
    }
    catch{
      case e: Exception => Ok("Error")
    }
  }

  def getProvincesByRegion(regionId: String) = Action { implicit request =>
    try{
      //println(regionId)
      val lstProvince = if(regionId.indexOf("(RegionCode")>=0) DevService.getRegion(regionId) else DevService.getProvincesByRegion(regionId)
      val months = DevService.getMonth().map(x => x._1)
      val rs = CommonService.normalizeArray(months,lstProvince)
      val jsBras = Json.obj(
        "lstProvince" -> rs,
        "monthArray" -> months,
        "keyProvince" -> rs.map(x=> x._2).asInstanceOf[Array[(String)]].distinct.sorted
      )
      Ok(Json.toJson(jsBras))
    }
    catch{
      case e: Exception => Ok("Error")
    }
  }

}
