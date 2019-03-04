package service

import com.sksamuel.elastic4s.http.ElasticDsl.{RichFuture, RichString, SearchHttpExecutable, SearchShow, percentilesAggregation, query, rangeAggregation, search, termsAgg, termsAggregation, _}
import com.sksamuel.elastic4s.http.search.SearchResponse
import churn.utils.{AgeGroupUtil, CommonUtil}
import play.api.Logger
import play.api.mvc.{AnyContent, Request}
import services.Configure
import services.domain.CommonService

object OverviewService{

  val client = Configure.client

  val logger = Logger(this.getClass())

  def checkExistsIndex(name: String) = {
    val isExists = client.execute{
      indexExists(name)
    }.await
    isExists.isExists
  }

  def getInternet(request: Request[AnyContent]) = {
    logger.info("========START OVERVIEW SERVICE=========")
    val t0 = System.currentTimeMillis()
    var status = 1 // Huy dich vu default
    var age = "*"
    var region = "*"
    var month = CommonService.getPrevMonth()
    logger.info("t0: "+(System.currentTimeMillis() - t0))
    if(!checkExistsIndex(s"churn-contract-info-$month")) month = CommonService.getPrevMonth(2)
    if(request != null) {
      status = request.body.asFormUrlEncoded.get("status").head.toInt
      age = if(request.body.asFormUrlEncoded.get("age").head != "") request.body.asFormUrlEncoded.get("age").head else "*"
      region = if(request.body.asFormUrlEncoded.get("region").head != "") request.body.asFormUrlEncoded.get("region").head else "*"
      month = request.body.asFormUrlEncoded.get("month").head
    }

    logger.info("Time: "+(System.currentTimeMillis() - t0))
    logger.info("========END OVERVIEW SERVICE=========")
    null
  }
}