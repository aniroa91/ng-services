package controllers

import javax.inject.Inject
import javax.inject.Singleton

import play.api.mvc.AbstractController
import play.api.mvc.ControllerComponents
import service.BrasService

import scala.concurrent.Await
import scala.concurrent.duration.Duration

/**
  * This controller creates an `Action` to handle HTTP requests to the
  * application's home page.
  */
@Singleton
class DeviceController @Inject()(cc: ControllerComponents) extends AbstractController(cc) with Secured{

  def index =  withAuth { username => implicit request =>
    try {
      val bras = Await.result(BrasService.listBrasOutlier, Duration.Inf)
      Ok(views.html.device.index(username, bras))
    }
    catch{
      case e: Exception => Ok(views.html.device.index(username,null))
    }
  }
}


