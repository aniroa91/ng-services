package service

import churn.utils.CommonUtil
import play.api.mvc.{AnyContent, Request}
import churn.models.CLCauseResponse
import play.api.Logger
import services.Configure
import services.domain.CommonService
import service.ChecklistService.{getFilterGroup, getQueryNested, getTopCause}
import service.OverviewAgeService.{calChurnRateAndPercentForAgeMonth, getTopOltByAge}
import service.OverviewService.{checkLocation, getCommentChart}
import scala.concurrent.{Await, Future}
import scala.concurrent.ExecutionContext.Implicits.global
import scala.async.Async.{async, await}
import scala.concurrent.duration.Duration

object ChecklistCauseService{

  val client = Configure.client

  val logger = Logger(this.getClass())

  def getDataPosition(month: String, status: String, queries: String, cause: String, processTime: String, region: String) = {
    val numOfPosition = ChecklistTimeService.getChurnByRegionProcessTime(month, queries, getQueryNested(cause, "*", processTime), region, "vitrixayrasuco")
    val position_X     = getTopOltByAge(numOfPosition.map(x=> (x._2, x._1, x._3.toDouble)))
    val locPosition_Y   = if(checkLocation(region) == "olt_name") getTopOltByAge(numOfPosition.map(x=> (x._1, x._2, x._3.toDouble))) else numOfPosition.map(x=> x._2).distinct.sorted
    val mapPosition = (0 until position_X.length).map(x=> position_X(x) -> x).toMap
    val mapLocPosition = (0 until locPosition_Y.length).map(x=> locPosition_Y(x) -> x).toMap
    val dataPosition = numOfPosition.filter(x=> locPosition_Y.indexOf(x._2) >=0).filter(x=> position_X.indexOf(x._1) >=0).map(x=> (mapPosition.get(x._1).get, mapLocPosition.get(x._2).get, x._3)).filter(x=> x._3 >0)

    // Trend Position and location
    val arrPositionLoc = ChecklistTimeService.getChecklistTimeRegion(month, queries, getQueryNested(cause, "*", processTime) ,region, "vitrixayrasuco")
    val trendRegionPosition = calChurnRateAndPercentForAgeMonth(arrPositionLoc, status, region).filter(x=> x._2 != CommonService.getCurrentMonth()).sorted
    val rsLocationPosition = trendRegionPosition.filter(x=> position_X.indexOf(x._1) >=0).filter(x=> locPosition_Y.indexOf(x._2) >=0)
      .map(x=> (mapLocPosition.map(x=> x._1 -> (x._2+1)).get(x._2).get, mapPosition.map(x=> x._1 -> (x._2+1)).get(x._1).get, x._3, x._4, x._5))

    Future{
      (position_X, locPosition_Y, dataPosition, mapLocPosition.map(x=> x._1 -> (x._2+1)), mapPosition.map(x=> x._1 -> (x._2+1)), rsLocationPosition)
    }
  }

  def getDataCause(month: String, queries: String, region: String, position: String, processTime: String, status: String) = {
    val numOfCause = ChecklistTimeService.getChurnByRegionProcessTime(month, queries, getQueryNested("*", position, processTime), region, "Nguyennhan")
    val cause_X     = getTopOltByAge(numOfCause.map(x=> (x._2, x._1, x._3.toDouble)))
    val locCause_Y   = if(checkLocation(region) == "olt_name") getTopOltByAge(numOfCause.map(x=> (x._1, x._2, x._3.toDouble))) else numOfCause.map(x=> x._2).distinct.sorted
    val mapCause = (0 until cause_X.length).map(x=> cause_X(x) -> x).toMap
    val mapLocCause = (0 until locCause_Y.length).map(x=> locCause_Y(x) -> x).toMap
    val dataCause = numOfCause.filter(x=> locCause_Y.indexOf(x._2) >=0).filter(x=> cause_X.indexOf(x._1) >=0).map(x=> (mapCause.get(x._1).get, mapLocCause.get(x._2).get, x._3)).filter(x=> x._3 >0)

    // Trend Cause and location
    val arrCauseLoc = ChecklistTimeService.getChecklistTimeRegion(month, queries, getQueryNested("*", position, processTime) ,region, "Nguyennhan")
    val trendRegionCause = calChurnRateAndPercentForAgeMonth(arrCauseLoc, status, region).filter(x=> x._2 != CommonService.getCurrentMonth()).sorted
    val rsLocationCause = trendRegionCause.filter(x=> cause_X.indexOf(x._1) >=0).filter(x=> locCause_Y.indexOf(x._2) >=0)
      .map(x=> (mapLocCause.map(x=> x._1 -> (x._2+1)).get(x._2).get, mapCause.map(x=> x._1 -> (x._2+1)).get(x._1).get, x._3, x._4, x._5))

    Future{
      (cause_X, locCause_Y, dataCause, mapLocCause.map(x=> x._1 -> (x._2+1)), mapCause.map(x=> x._1 -> (x._2+1)), rsLocationCause)
    }
  }

  def getInternet(user: String, request: Request[AnyContent]) = async{
    logger.info("========START CHECKLIST CAUSE TAB SERVICE=========")
    val t0 = System.currentTimeMillis()
    val groupCL = request.body.asFormUrlEncoded.get("groupCL").head
    val status = request.body.asFormUrlEncoded.get("status").head
    val age = ""
    val timeCL = request.body.asFormUrlEncoded.get("timeCL").head.toInt
    var position = request.body.asFormUrlEncoded.get("position").head
    var cause = request.body.asFormUrlEncoded.get("cause").head
    val region = request.body.asFormUrlEncoded.get("region").head
    val month = request.body.asFormUrlEncoded.get("month").head
    val packages = request.body.asFormUrlEncoded.get("package").head
    val queries = getFilterGroup(timeCL, age, region, packages, month, 1)
    //println(queries)
    var processTime = request.body.asFormUrlEncoded.get("processTime").head
    // get top 5 Nguyen nhan
    val topCause = getTopCause(CommonService.getPrevMonth(), "Nguyennhan").map(x=> x._1)
    // get top 5 vi Tri xay ra Checklist
    val topPostion = getTopCause(CommonService.getPrevMonth(), "vitrixayrasuco").map(x=> x._1)
    // nested query filter
    request.body.asFormUrlEncoded.get("processTime").head match {
      case "*" => {
        processTime = "*"
      }
      case _ => {
        processTime = "checklist.processTimeGroup:\"" + request.body.asFormUrlEncoded.get("processTime").head + "\""
      }
    }
    request.body.asFormUrlEncoded.get("cause").head match {
      case "*" => {
        cause = "*"
      }
      case "others" => {
        cause = topCause.map(x => "!(checklist.Nguyennhan:\"" + x + "\")").mkString(" AND ")
      }
      case _ => {
        cause = "checklist.Nguyennhan:\"" + request.body.asFormUrlEncoded.get("cause").head + "\""
      }
    }
    request.body.asFormUrlEncoded.get("position").head match {
      case "*" => {
        position = "*"
      }
      case "others" => {
        position = topPostion.map(x => "!(checklist.vitrixayrasuco:\"" + x + "\")").mkString(" AND ")
      }
      case _ => {
        position = "checklist.vitrixayrasuco:\"" + request.body.asFormUrlEncoded.get("position").head + "\""
      }
    }

    val t1 = System.currentTimeMillis()
    //  Number of Checklist by Checklist Cause by Region
    val dataCause = Await.result(getDataCause(month, queries, region, position, processTime, status), Duration.Inf)
    logger.info("t1: "+(System.currentTimeMillis() -t1))

    val t2 = System.currentTimeMillis()
    //  Number of Checklist by Checklist Position by Region
    val rsPosition = Await.result(getDataPosition(month, status, queries, cause, processTime, region), Duration.Inf)
    logger.info("t2: "+(System.currentTimeMillis() -t2))

    val t3 = System.currentTimeMillis()
    // comments content
    val cmtChart = Await.result(Future{ getCommentChart(user, CommonUtil.PAGE_ID.get(1).get+"_tabCause") }, Duration.Inf)
    logger.info("t3: "+(System.currentTimeMillis() - t3))

    logger.info("Time: "+(System.currentTimeMillis() -t0))
    logger.info("========END CHECKLIST CAUSE TAB SERVICE=========")
    await(
      Future {
        CLCauseResponse((dataCause._1, dataCause._2, dataCause._3), (dataCause._4, dataCause._5, dataCause._6),
          (rsPosition._1, rsPosition._2, rsPosition._3), (rsPosition._4, rsPosition._5, rsPosition._6), cmtChart, month)
      }
    )
  }
}