package controllers

import play.api.data.Form
import play.api.data.Forms.mapping
import play.api.data.Forms.text
import javax.inject.Inject
import javax.inject.Singleton
import play.api.mvc.AbstractController
import play.api.mvc.ControllerComponents
import play.api.mvc._

/**
  * This controller creates an `Action` to handle HTTP requests to the
  * application's home page.
  */
case class InternetContract(tpTime: String,date: String,ct: String)

@Singleton
class InternetHistoryController @Inject()(cc: ControllerComponents) extends AbstractController(cc) with Secured{

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
          Redirect(routes.LoginController.index).withNewSession.flashing(
            "success" -> "You are now logged out."
          )
      }
    }
  }

  def compareContract =  Authenticated { implicit request =>
    val formValidationResult = form.bindFromRequest
    try {
      if (!formValidationResult.hasErrors) {
        val ct = formValidationResult.get.ct.trim()
        val day = formValidationResult.get.date.trim()
        val tptime = formValidationResult.get.tpTime.trim()
        Ok(profiles.views.html.internet.compareContract(form, request.session.get("username").get.toString,ct,day,tptime))
      } else {
        Ok(profiles.views.html.internet.compareContract(form, request.session.get("username").get.toString,null,null,null))
      }
    } catch {
      case e: Exception => Ok(profiles.views.html.internet.compareContract(form, request.session.get("username").get.toString,null,null,null))
    }
  }

}

