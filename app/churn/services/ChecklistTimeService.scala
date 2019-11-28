package service

import churn.utils.{AgeGroupUtil, CommonUtil, ProvinceUtil}
import play.api.mvc.{AnyContent, Request}
import churn.models.CLTimeResponse
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

object ChecklistTimeService{

  val client = Configure.client

  val logger = Logger(this.getClass())

  def getChurnByRegionProcessTime(month: String, queryStr: String, queryNested: String, region: String, tab: String) ={
    val queries = queryStr+" AND "+CommonUtil.filterCommon("tenGoi")
    val groupTab = if(tab == "time")
      """
        "range": {
             "field": "checklist.processTime",
             "ranges": [
                        {
                         "key": "0-3 hours",
                         "from": 0,
                         "to": 3
                        },
                        {
                         "key": "03-24 hours",
                         "from": 3,
                         "to": 24
                        },
                        {
                         "key": "24-48 hours",
                         "from": 24,
                         "to": 48
                        },
                        {
                         "key": ">=48 hours",
                         "from": 48,
                         "to": 1.7976931348623157e+308
                        }
           ]
        }
        """.stripMargin else
      s"""
        "terms": {
             "field": "checklist.${tab}",
             "size": 1000
        }
        """.stripMargin
    val location = checkLocation(region)
    val request = s"""
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
                           "size" : 1000
                         },
                         "aggs": {
                           "checklist": {
                             "nested": {
                               "path": "checklist"
                             },
                             "aggs": {
                               "time": {
                                 ${groupTab}
                               }
                             }
                           }
                         }
                       }
             },
             "size": 0
        }
        """
    //println(request)
    val body = Http(s"http://172.27.11.151:9200/churn-checklist-${month}/docs/_search")
      .postData(request)
      .header("content-type", "application/JSON")
      .asString.body
    val json = Json.parse(body)
    val array = json.\("aggregations").\("region").\("buckets").get.asInstanceOf[JsArray].value.toArray
    val arrRss = array.map(x => (x.\("key").get.asInstanceOf[JsNumber], x.\("checklist").get.\("time").\("buckets").get.asInstanceOf[JsArray].value.toArray
      .map(y=> (y.\("key").get.asInstanceOf[JsString], y.\("doc_count").get.asInstanceOf[JsNumber]))))
    val res = arrRss.flatMap(x=> x._2.map(y=> x._1.value -> y)).map(x=> (x._2._1.toString().replace("\"", ""), x._1.toString(), x._2._2.toString().toLong))
    val intRegex = """(\d+)""".r
    region match {
      case ""   => res.map(x=> (x._1, "Vung "+x._2, x._3))
      case intRegex(region) => res.map(x=> ( x._1, CommonService.toTitleCase(ProvinceUtil.getProvince(x._2.toInt)), x._3))
      case _       => res
    }
  }

  def getChecklistTimeRegion(month: String, queries: String, queryNested: String, region: String, tab: String) = {
    val location = checkLocation(region)
    val groupTab = if(tab == "time")
      """
        "range": {
             "field": "checklist.processTime",
             "ranges": [
                        {
                         "key": "0-3 hours",
                         "from": 0,
                         "to": 3
                        },
                        {
                         "key": "03-24 hours",
                         "from": 3,
                         "to": 24
                        },
                        {
                         "key": "24-48 hours",
                         "from": 24,
                         "to": 48
                        },
                        {
                         "key": ">=48 hours",
                         "from": 48,
                         "to": 1.7976931348623157e+308
                        }
           ]
        }
        """.stripMargin else
      s"""
        "terms": {
             "field": "checklist.${tab}",
             "size": 1000
        }
        """.stripMargin
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
                           "checklist": {
                             "nested": {
                               "path": "checklist"
                             },
                             "aggs": {
                               "time": {
                                 ${groupTab}
                               }
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
            y.\("checklist").\("time").\("buckets").get.asInstanceOf[JsArray].value.toArray.map(k=> (k.\("key").get.asInstanceOf[JsString].toString(), k.\("doc_count").get.asInstanceOf[JsNumber].toString().toInt))))))
    array.flatMap(x=> x._3.map(y=> (x._1, x._2) -> y)).map(x=> (x._1._1, x._1._2, x._2._1, x._2._2, x._2._3)).flatMap(x=> x._5.map(y=> (x._1, x._2, x._3, x._4) -> y))
      .map(x=> (x._1._1, x._1._3, x._2._1.replace("\"",""), x._1._4, x._2._2))
  }

  def getInternet(user: String, request: Request[AnyContent]) = {
    logger.info("========START CHECKLIST PROCESSING TIME TAB SERVICE=========")
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
    val processTime = "*"
    // get top 5 Nguyen nhan
    val topCause = getTopCause(CommonService.getPrevMonth(), "Nguyennhan").map(x=> x._1)
    // get top 5 vi Tri xay ra Checklist
    val topPostion = getTopCause(CommonService.getPrevMonth(), "vitrixayrasuco").map(x=> x._1)
    // nested query filter
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
    //  Number of Checklist by Checklist Processing Time by Region
    val numOfProcessTime   = getChurnByRegionProcessTime(month, queries, queryNested, region, "time")
    val time_X     = numOfProcessTime.map(x=> x._1).distinct.sorted
    val location_Y   = if(checkLocation(region) == "olt_name") getTopOltByAge(numOfProcessTime.map(x=> (x._1, x._2, x._3.toDouble))) else numOfProcessTime.map(x=> x._2).distinct.sorted
    val mapTime = (0 until time_X.length).map(x=> time_X(x) -> x).toMap
    val mapLocation = (0 until location_Y.length).map(x=> location_Y(x) -> x).toMap
    val dataChurn = numOfProcessTime.filter(x=> location_Y.indexOf(x._2) >=0).filter(x=> time_X.indexOf(x._1) >=0).map(x=> (mapTime.get(x._1).get, mapLocation.get(x._2).get, x._3)).filter(x=> x._3 >0)
    logger.info("t1: "+(System.currentTimeMillis() -t1))
    val t2 = System.currentTimeMillis()
    // Trend Processing Time and location
    val arrTimeLoc = getChecklistTimeRegion(month, queries, queryNested ,region, "time")
    val trendRegiontime = calChurnRateAndPercentForAgeMonth(arrTimeLoc, status, region).filter(x=> x._2 != CommonService.getCurrentMonth()).sorted
    val rsLocationTime = trendRegiontime.filter(x=> time_X.indexOf(x._1) >=0).filter(x=> location_Y.indexOf(x._2) >=0)
      .map(x=> (mapLocation.map(x=> x._1 -> (x._2+1)).get(x._2).get, mapTime.map(x=> x._1 -> (x._2+1)).get(x._1).get, x._3, x._4, x._5))
    logger.info("t2: "+(System.currentTimeMillis() - t2))
    val t3 = System.currentTimeMillis()
    // comments content
    val cmtChart = getCommentChart(user, CommonUtil.PAGE_ID.get(1).get+"_tabTime")
    logger.info("t3: "+(System.currentTimeMillis() - t3))
    logger.info("Time: "+(System.currentTimeMillis() -t0))
    logger.info("========END CHECKLIST PROCESSING TIME TAB SERVICE=========")
    CLTimeResponse((time_X, location_Y, dataChurn), (mapLocation.map(x=> x._1 -> (x._2+1)), mapTime.map(x=> x._1 -> (x._2+1)), rsLocationTime), cmtChart, month)
  }
}