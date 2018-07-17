package profile.controllers.internet

import play.api.data.Form
import play.api.data.Forms.mapping
import play.api.data.Forms.text
import javax.inject.Inject
import javax.inject.Singleton

import play.api.mvc.AbstractController
import play.api.mvc.ControllerComponents
import controllers.{AuthenticatedRequest, InternetContract, SearchContract, Secured}
import com.sksamuel.elastic4s.http.HttpClient
import com.sksamuel.elastic4s.ElasticsearchClientUri
import services.Configure
import com.sksamuel.elastic4s.http.ElasticDsl._
import profile.services.internet.HistoryService
import com.ftel.bigdata.utils.StringUtil
import profile.services.internet.response.History
import profile.services.internet.response.Hourly
import profile.services.internet.CompareService
import play.api.mvc._

/**
  * This controller creates an `Action` to handle HTTP requests to the
  * application's home page.
  */
@Singleton
class CompareController @Inject()(cc: ControllerComponents) extends AbstractController(cc) with Secured{

  val client = Configure.client
  val form = Form(
    mapping(
      "tpTime" -> text,
      "date" -> text,
      "ct" -> text
    )(InternetContract.apply)(InternetContract.unapply))

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

  def index =  Authenticated { implicit request =>
    val formValidationResult = form.bindFromRequest
    try {
      if (!formValidationResult.hasErrors) {
        val contract = formValidationResult.get.ct.trim()
        val date = formValidationResult.get.date.trim()
        val tpTime = formValidationResult.get.tpTime.trim()
        Ok(profiles.views.html.internet.compare.date.index(form, request.session.get("username").get.toString, contract,null, date,tpTime))
      } else {
        Ok(profiles.views.html.internet.compare.date.index(form, request.session.get("username").get.toString, null, null,null,null))
      }
    } catch {
      case e: Exception => Ok(profiles.views.html.internet.compare.date.index(form, request.session.get("username").get.toString, null,null,null, null))
    }
  }
}



