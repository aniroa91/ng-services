package service

import com.sksamuel.elastic4s.http.ElasticDsl.{RichFuture, RichString, SearchHttpExecutable, rangeAggregation, search, termsAggregation, _}
import churn.utils.CommonUtil
import play.api.mvc.{AnyContent, Request}
import churn.models.CLAgeResponse
import play.api.Logger
import scalaj.http.Http
import services.Configure
import services.domain.CommonService
import play.api.libs.json.JsArray
import play.api.libs.json.JsNumber
import play.api.libs.json.JsString
import play.api.libs.json.Json
import service.ChecklistService.{getFilterGroup, getQueryNested, getTopCause}
import service.OverviewAgeService.{calChurnRateAndPercentForAgeMonth, getTopOltByAge}
import service.OverviewService.{checkLocation, getCommentChart}

object ChecklistAgeService{

  val client = Configure.client

  val logger = Logger(this.getClass())

  def getChecklistRegionAge(month: String, queries: String, queryNested: String, region: String) = {
    val location = checkLocation(region)
    val query = s"""
        {
            "query": {
                "bool": {
                    "filter": [
                         {
                           "query_string" : {
                              "query": "${queries.replace("\"", "\\\"")}"
                           }
                         },
                         {
                           "nested": {
                               "path": "checklist",
                               "query": {
                                     "query_string": {
                                           "query": "${queryNested.replace("\"", "\\\"")}"
                                     }
                               }
                           }
                         }
                    ]
                }
            },
            "aggs": {
               "region": {
                  "terms": {
                     "field": "$location",
                     "size": 1000
                  },
                  "aggs": {
                     "status": {
                        "terms": {
                           "field": "status",
                           "size": 1000
                        },
                        "aggs": {
                           "lifeGroup": {
                               "terms": {
                                  "field": "lifeGroup",
                                  "size": 1000
                               }
                           }
                        }
                     }
                  }
               }
            },
            "size": 0
        }
        """
    //println(query)
    val body = Http(s"http://172.27.11.151:9200/churn-checklist-${month}/docs/_search")
      .postData(query)
      .header("content-type", "application/JSON")
      .asString.body
    val json = Json.parse(body)
    val array = json.\("aggregations").\("region").\("buckets").get.asInstanceOf[JsArray].value.toArray
      .map(x => (x.\("key").get.asInstanceOf[JsNumber].toString().replace("\"",""), x.\("doc_count").get.asInstanceOf[JsNumber].toString().toLong,
        x.\("status").\("buckets").get.asInstanceOf[JsArray].value.toArray
          .map(y=> (y.\("key").get.asInstanceOf[JsNumber].toString(), y.\("doc_count").get.asInstanceOf[JsNumber].toString().toInt,
            y.\("lifeGroup").\("buckets").get.asInstanceOf[JsArray].value.toArray.map(k=> (k.\("key").get.asInstanceOf[JsString].toString(), k.\("doc_count").get.asInstanceOf[JsNumber].toString().toInt))))))
    val rs = array.flatMap(x=> x._3.map(y=> (x._1, x._2) -> y)).map(x=> (x._1._1, x._1._2, x._2._1, x._2._2, x._2._3)).flatMap(x=> x._5.map(y=> (x._1, x._2, x._3, x._4) -> y))
      .map(x=> (x._1._1, x._1._3, x._2._1.replace("\"",""), x._1._4, x._2._2))
    rs.map(x=> (x._1, x._2,if(x._3 == "6-12") "06-12" else x._3, x._4, x._5))
  }

  def getChurnByRegionAgeAll(month: String, queries: String, region: String) ={
    val location = checkLocation(region)
    val request = search(s"churn-contract-info-${month}" / "docs") query(queries + " AND " + CommonUtil.filterCommon("package_name")) aggregations (
      rangeAggregation("age")
        .field("age")
        .range("0-6", 0, 6.0001)
        .range("06-12", 6.001, 12.001)
        .range("12-18", 12.001, 18.001)
        .range("18-24", 18.001, 24.001)
        .range("24-30", 24.001, 30.001)
        .range("30-36", 30.001, 36.001)
        .range("36-42", 36.001, 42.001)
        .range("42-48", 42.001, 48.001)
        .range("48-54", 48.001, 54.001)
        .range("54-60", 54.001, 60.001)
        .range(">60", 60.001, Double.MaxValue)
        .subaggs(
          termsAggregation("region")
            .field(s"$location") size 1000
        )
      )
    val rs = client.execute(request).await
    CommonService.getSecondAggregations(rs.aggregations.get("age"), "region")
      .flatMap(x=> x._2.map(y=> x._1 -> y))
      .map(x=> (x._1, x._2._1, x._2._2))
  }

  def getChurnByRegionAgeChecklist(month: String,queryStr: String, queryNested: String, field: String) ={
    val queries = queryStr + " AND " + CommonUtil.filterCommon("tenGoi")
    val location = checkLocation(field)
    val query = s"""
        {
            "query": {
                "bool": {
                    "filter": [
                         {
                            "query_string" : {
                                "query": "${queries.replace("\"", "\\\"")}"
                            }
                         },
                         {
                            "nested": {
                                 "path": "checklist",
                                 "query": {
                                      "query_string": {
                                            "query": "${queryNested.replace("\"", "\\\"")}"
                                      }
                                 }
                            }
                         }
                    ]
                }
            },
            "aggs": {
                "age": {
                    "range": {
                       "field": "age",
                       "ranges": [
                          {
                            "key": "0-6",
                            "from": 0,
                            "to": 6.0001
                          },
                          {
                            "key": "06-12",
                            "from": 6.001,
                            "to": 12.001
                          },
                          {
                            "key": "12-18",
                            "from": 12.001,
                            "to": 18.001
                          },
                          {
                            "key": "18-24",
                            "from": 18.001,
                            "to": 24.001
                          },
                          {
                            "key": "24-30",
                            "from": 24.001,
                            "to": 30.001
                          },
                          {
                            "key": "30-36",
                            "from": 30.001,
                            "to": 36.001
                          },
                          {
                            "key": "36-42",
                            "from": 36.001,
                            "to": 42.001
                          },
                          {
                            "key": "42-48",
                            "from": 42.001,
                            "to": 48.001
                          },
                          {
                            "key": "48-54",
                            "from": 48.001,
                            "to": 54.001
                          },
                          {
                            "key": "54-60",
                            "from": 54.001,
                            "to": 60.001
                          },
                          {
                            "key": ">60",
                            "from": 60.001,
                            "to": 1.7976931348623157e+308
                          }
                        ]
                      },
                      "aggs": {
                         "region": {
                            "terms": {
                                "field": "$location",
                                "size": 1000
                            }
                         }
                    }
                }
            }
            ,
            "size": 0
        }
        """
    //println(query)
    val body = Http(s"http://172.27.11.151:9200/churn-checklist-${month}/docs/_search")
      .postData(query)
      .header("content-type", "application/JSON")
      .asString.body
    val json = Json.parse(body)
    val array = json.\("aggregations").\("age").\("buckets").get.asInstanceOf[JsArray].value.toArray
      .map(x => (x.\("key").get.asInstanceOf[JsString].toString().replace("\"",""),
        x.\("region").\("buckets").get.asInstanceOf[JsArray].value.toArray.map(y=> (y.\("key").get.asInstanceOf[JsNumber].toString(), y.\("doc_count").get.asInstanceOf[JsNumber].toString().toLong))))
    array.flatMap(x=> x._2.map(y=> x._1 -> y)).map(x=> (x._1, x._2._1, x._2._2))
  }

  def getInternet(user: String, request: Request[AnyContent]) = {
    logger.info("========START CHECKLIST AGE TAB SERVICE=========")
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
    println(queries)
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
    val queryNested = getQueryNested(cause, position, processTime)
    val t1 = System.currentTimeMillis()
    // Number of Contracts Who Have Checklist(s) by Region by Contract Age (%)
    val numOfChecklist      = getChurnByRegionAgeChecklist(month, queries, queryNested, region)
    val checklistRegionAge  = ChecklistRegionService.calChurnCLRateAndPercentagebyRegion(numOfChecklist, getChurnByRegionAgeAll(month, queries, region), region)
    logger.info("t1: "+(System.currentTimeMillis() -t1))
    val t2 = System.currentTimeMillis()
    // Trend Age and location
    val arrAgeLoc = getChecklistRegionAge(month, queries, queryNested ,region)
    val trendRegionAge = calChurnRateAndPercentForAgeMonth(arrAgeLoc, status, region).filter(x=> x._2 != CommonService.getCurrentMonth()).sorted
    /* get top location */
    val topLocationByPert = if(checkLocation(region) == "olt_name") getTopOltByAge(trendRegionAge.map(x=> (x._1, x._2, x._4))) else trendRegionAge.map(x=> x._2).distinct
    val sizeTopLocation = topLocationByPert.length +1
    val mapLocation = (1 until sizeTopLocation).map(x=> topLocationByPert(x-1) -> x).toMap
    /* get top Age Location */
    val topAgeLocByPert = trendRegionAge.map(x=> x._1).distinct
    val sizeTopAgeLoc = topAgeLocByPert.length +1
    val mapAge = (1 until sizeTopAgeLoc).map(x=> topAgeLocByPert(x-1) -> x).toMap
    val rsLocationAge = trendRegionAge.filter(x=> topAgeLocByPert.indexOf(x._1) >=0).filter(x=> topLocationByPert.indexOf(x._2) >=0)
      .map(x=> (mapLocation.get(x._2).get, mapAge.get(x._1).get, x._3, x._4, x._5))
    logger.info("t2: "+(System.currentTimeMillis() - t2))
    val t3 = System.currentTimeMillis()
    // comments content
    val cmtChart = getCommentChart(user, CommonUtil.PAGE_ID.get(1).get+"_tabAge")
    logger.info("t3: "+(System.currentTimeMillis() - t3))
    logger.info("Time: "+(System.currentTimeMillis() -t0))
    logger.info("========END CHECKLIST AGE TAB SERVICE=========")
    CLAgeResponse(checklistRegionAge, (mapLocation, mapAge, rsLocationAge), cmtChart, month)
  }
}