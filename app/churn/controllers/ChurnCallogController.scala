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

/**
  * This controller creates an `Action` to handle HTTP requests to the
  * application's home page.
  */

@Singleton
class ChurnCallogController @Inject() (cc: ControllerComponents) extends AbstractController(cc) with Secured{

  def index() = withAuth { username => implicit request: Request[AnyContent] =>
    val month = CommonService.getPrevMonth()
    try {
      //val rs = ChurnAgeService.getInternet(null)
      Ok(churn.views.html.calllog.index(null, month, username, churn.controllers.routes.ChurnCallogController.index()))
    }
    catch {
      case e: Exception => Ok(churn.views.html.age.index(null, month, username, churn.controllers.routes.ChurnCallogController.index()))
    }
  }


}
