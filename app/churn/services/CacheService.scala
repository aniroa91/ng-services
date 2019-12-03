package churn.services

import churn.models.{CLAgeResponse, CLCauseResponse, CLRegionResponse, CLTimeResponse, ChecklistResponse}
import play.api.mvc.{AnyContent, Request}
import service.{ChecklistAgeService, ChecklistCauseService, ChecklistRegionService, ChecklistService, ChecklistTimeService}
import services.domain.CommonService

object CacheService {
  var checklistCache:(String, ChecklistResponse) = "" -> null
  var checklistRegionCache:(String, CLRegionResponse) = "" -> null
  var checklistAgeCache:(String, CLAgeResponse) = "" -> null
  var checklistTimeCache:(String, CLTimeResponse) = "" -> null
  var checklistCauseCache:(String, CLCauseResponse) = "" -> null

  /* PAGE CHECKLIST */
  private def getFilterChecklist(request: Request[AnyContent]) = {
    if(request == null) CommonService.getCurrentDay()
    else {
      val currDate = CommonService.getCurrentDay()
      val groupCL = request.body.asFormUrlEncoded.get("groupCL").head
      val status = if (request.body.asFormUrlEncoded.get("status").head == "") 13 else request.body.asFormUrlEncoded.get("status").head.toInt
      val timeCL = request.body.asFormUrlEncoded.get("timeCL").head.toInt
      val month = request.body.asFormUrlEncoded.get("month").head
      val age = if (request.body.asFormUrlEncoded.get("age").head == "") "*" else request.body.asFormUrlEncoded.get("age").head
      val packages = if (request.body.asFormUrlEncoded.get("package").head == "") "*" else request.body.asFormUrlEncoded.get("package").head
      val region = if (request.body.asFormUrlEncoded.get("region").head == "") "*" else request.body.asFormUrlEncoded.get("region").head
      val queries = s"($currDate) AND ($status) AND ($month) AND ($age) AND ($packages) AND ($region) AND ($timeCL)"

      // nested query filter
      val processTime = request.body.asFormUrlEncoded.get("processTime").head
      val cause = request.body.asFormUrlEncoded.get("cause").head
      val position = request.body.asFormUrlEncoded.get("position").head
      val nested = s"($cause) AND ($position) AND ($processTime)"
      queries + nested
    }
  }
  // CHECKLIST OVERVIEW
  def getChecklistResponse(request: Request[AnyContent]) = {
    if(getFilterChecklist(request) == checklistCache._1)
      checklistCache._2
    else {
      val response = ChecklistService.getInternet(request)
      checklistCache = getFilterChecklist(request)-> response
      response
    }
  }
  // TAB REGION
  def getChecklistRegionResponse(request: Request[AnyContent], username: String) = {
    if(getFilterChecklist(request) == checklistRegionCache._1)
      checklistRegionCache._2
    else {
      val response = ChecklistRegionService.getInternet(username,request)
      checklistRegionCache = getFilterChecklist(request)-> response
      response
    }
  }
  // TAB AGE
  def getChecklistAgeResponse(request: Request[AnyContent], username: String) = {
    if(getFilterChecklist(request) == checklistAgeCache._1)
      checklistAgeCache._2
    else {
      val response = ChecklistAgeService.getInternet(username,request)
      checklistAgeCache = getFilterChecklist(request)-> response
      response
    }
  }
  // TAB PROCESSING TIME
  def getChecklistTimeResponse(request: Request[AnyContent], username: String) = {
    if(getFilterChecklist(request) == checklistTimeCache._1)
      checklistTimeCache._2
    else {
      val response = ChecklistTimeService.getInternet(username,request)
      checklistTimeCache = getFilterChecklist(request)-> response
      response
    }
  }
  // TAB CAUSE
  def getChecklistCauseResponse(request: Request[AnyContent], username: String) = {
    if(getFilterChecklist(request) == checklistCauseCache._1)
      checklistCauseCache._2
    else {
      val response = ChecklistCauseService.getInternet(username,request)
      checklistCauseCache = getFilterChecklist(request)-> response
      response
    }
  }

  /* PAGE OVERVIEW */
}
