package controllers

import play.api._
import play.api.mvc._
import play.api.data._
import play.api.data.Forms._
import views._
import javax.inject.Inject
import javax.inject.Singleton
import play.api.mvc.AbstractController
import play.api.mvc.ControllerComponents

@Singleton
class LoginController @Inject()(cc: ControllerComponents) extends AbstractController(cc) {

  val loginForm = Form(
    tuple(
      "email" -> text,
      "password" -> text
    ) verifying ("Invalid email or password", result => result match {
      case (email, password) => check(email, password)
    })
  )

  def check(username: String, password: String) = {
    (username == "btgd@ftel" && password == "da@171020")
  }

  def index = Action { implicit request =>
    val ssId = request.session.get("username").toString
    if(ssId != "None") {
      val username = request.session.get("username").get.toString
      username match {
        case "btgd@ftel" =>
          Redirect(routes.PaytvClientController.index)
      }
    }
    else
       Ok(common.views.html.login.index(loginForm))
  }

  def login = Action { implicit request =>
    loginForm.bindFromRequest.fold(
      formWithErrors => Redirect(routes.LoginController.index),
      user =>{
        user._1 match {
          case "btgd@ftel" =>
            Redirect(routes.PaytvClientController.index).withSession(Security.username -> user._1)
        }
      }
    )
  }

  def logout = Action {
    Redirect(routes.LoginController.index).withNewSession.flashing(
      "success" -> "You are now logged out."
    )
  }

}