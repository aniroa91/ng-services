package controllers.internet

import play.api.data.Form
import play.api.data.Forms.mapping
import play.api.data.Forms.text
import javax.inject.Inject
import javax.inject.Singleton

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
import profile.services.internet.Common

//case class InternetContract(tpTime: String,date: String,ct: String)

/**
  * This controller creates an `Action` to handle HTTP requests to the
  * application's home page.
  */
@Singleton
class BrasController @Inject()(cc: ControllerComponents) extends AbstractController(cc) with Secured {

  var unit = 0

  def index() =  withAuth { username => implicit request =>
    Ok(profiles.views.html.internet.history.bras(username, HistoryService.getBrasRealtime().map(x => x._1.toLong -> x._2)))
  }

  def json() =  withAuth { username => implicit request =>
    unit = unit + 1
    val streaming = HistoryService.getBrasRealtime2(unit).map(x => x._1.toLong -> x._2)
    val jsRealtime = Json.obj("minutes" -> streaming)
    //streaming.hourly.contract.map(x=> x._1 -> x._2).foreach(println)
    // val jsRealtime = Json.obj(
    //   "numberOfContract" -> streaming.numberOfContract,
    //   "numberOfSession" -> streaming.numberOfSession,
    //   "minutes" -> streaming.hourly.contract.map(x=> x._1 -> x._2),
    //   "session" -> streaming.hourly.session.map(x=> x._2),
    //   "download" -> streaming.hourly.download.map(x=> x._2),
    //   "upload" -> streaming.hourly.upload.map(x=> x._2),
    //   "sumSession" -> streaming.hourly.session.map(x=> x._2).sum,
    //   "sumDown" -> Common.bytesToTerabytes(streaming.hourly.download.map(x=> x._2).sum),
    //   "sumUp" -> streaming.hourly.upload.map(x=> x._2).sum,
    //   "tbContract" -> streaming.contracts

    // )
    Ok(Json.toJson(jsRealtime))
  }
}



