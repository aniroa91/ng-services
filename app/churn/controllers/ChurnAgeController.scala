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
import service.ChurnAgeService

/**
  * This controller creates an `Action` to handle HTTP requests to the
  * application's home page.
  */

@Singleton
class ChurnAgeController @Inject() (cc: ControllerComponents) extends AbstractController(cc) with Secured{

  def index() = withAuth { username => implicit request: Request[AnyContent] =>
    val month = CommonService.getPrevMonth()
    try {
      val rs = ChurnAgeService.getInternet(null)
      Ok(churn.views.html.age.index(rs, rs._4, username, churn.controllers.routes.ChurnAgeController.index()))
    }
    catch {
      case e: Exception => Ok(churn.views.html.age.index(null, month, username, churn.controllers.routes.ChurnAgeController.index()))
    }
  }

  def getJsonChurn() = withAuth {username => implicit request: Request[AnyContent] =>
   try{
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
    }
    catch{
      case e: Exception => Ok("Error")
    }

  }


}
