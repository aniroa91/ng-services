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

//case class InternetContract(tpTime: String,date: String,ct: String)

/**
  * This controller creates an `Action` to handle HTTP requests to the
  * application's home page.
  */
@Singleton
class HistoryController @Inject()(cc: ControllerComponents) extends AbstractController(cc) with Secured{

  val day = CommonService.getCurrentDay()

  val client = Configure.client
  //  val form = Form(
  //    mapping(
  //      "ct" -> text)(SearchContract.apply)(SearchContract.unapply))

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

  def index(date: String) =  Authenticated { implicit request =>
    //println("DATE: " + date)

    //    try {
    val formValidationResult = form.bindFromRequest
    if (!formValidationResult.hasErrors) {
      val contract = formValidationResult.get.ct.trim()
      val _type = formValidationResult.get.tpTime.trim()
      val time = formValidationResult.get.date.trim()

      println(_type + ": " + time)
      //println(formValidationResult.get.ct)
      //println(formValidationResult.get.date)
      //println(formValidationResult.get.tpTime)
      if (StringUtil.isNullOrEmpty(contract)) {
        //println(contract)
        //Ok(views.html.profile.internet.history.index(form, username, HistoryService.get("day", date), date, "day"))
        //Ok(views.html.profile.internet.history.index(form, username, HistoryService.get("week", "2018-02-08"), date, "week"))
        //          Ok(views.html.profile.internet.history.index(form, username, HistoryService.getAll("month", "2018-02-01"), date, "month"))
        Ok(profiles.views.html.internet.history.index(form,
          request.session.get("username").get.toString,
          HistoryService.getAll(_type, time),
          time,
          _type))
      } else {
        if (StringUtil.isNullOrEmpty(_type) && StringUtil.isNullOrEmpty(time)) {
          Ok(profiles.views.html.internet.history.indexContract(form,
            request.session.get("username").get.toString, HistoryService.getContract("M", "02/2018", contract.toLowerCase()),
            contract,
            time,
            "M"))
        } else {
          Ok(profiles.views.html.internet.history.indexContract(form,
            request.session.get("username").get.toString, HistoryService.getContract(_type, time, contract.toLowerCase()),
            contract,
            time,
            _type))
        }
        //Ok(views.html.profile.internet.history.indexContract(form, username, HistoryService.getContract("week", "2018-02-08", contract.toLowerCase()), contract, "week"))
        //Ok(views.html.profile.internet.history.indexContract(form, username, HistoryService.getContract("month", "2018-02-01", contract.toLowerCase()), contract, "month"))
      }
    } else {
      println("============")
      Ok(profiles.views.html.internet.history.index(form, request.session.get("username").get.toString, HistoryService.getAll("M", "02/2018"), "02/2018", "M"))
    }

  }

  def realtime() =  Authenticated { implicit request =>
    Ok(profiles.views.html.internet.history.realtime(request.session.get("username").get.toString, HistoryService.getAll("M", "02/2018"), HistoryService.getRealtime(day)))
  }

  def realtimeJson() =  withAuth { username => implicit request =>
    val streaming = HistoryService.getRealtime(day)
    val jsRealtime = Json.obj(
      "numberOfContract" -> streaming.numberOfContract,
      "numberOfSession" -> streaming.numberOfSession,
      "minutes" -> streaming.hourly.contract.map(x=> x._1 -> x._2),
      "session" -> streaming.hourly.session.map(x=> x._2),
      "download" -> streaming.hourly.download.map(x=> x._2),
      "upload" -> streaming.hourly.upload.map(x=> x._2),
      "sumSession" -> streaming.hourly.session.map(x=> x._2).sum,
      "sumDown" -> streaming.hourly.download.map(x=> x._2).sum,
      "sumUp" -> streaming.hourly.upload.map(x=> x._2).sum,
      "tbContract" -> streaming.contracts,
      "tbProvince" -> streaming.provinces,
      "jsContract" -> streaming.regions.map(x => x._2),
      "jsDownload" -> streaming.regions.map(x => x._4),
      "jsUpload" -> streaming.regions.map(x => x._5),
      "jsDuration" -> streaming.regions.map(x => x._6)
    )
    Ok(Json.toJson(jsRealtime))
  }
}



