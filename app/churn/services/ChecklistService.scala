package service

import com.sksamuel.elastic4s.http.ElasticDsl.{RichFuture, RichString, SearchHttpExecutable, search, termsAggregation, _}
import play.api.libs.json.{JsArray, JsNumber, JsString, JsValue, Json}
import churn.utils.{CommonUtil, ProvinceUtil}
import org.elasticsearch.search.sort.SortOrder
import play.api.mvc.{AnyContent, Request}
import churn.models.{ChecklistResponse, ContractNumber, RateNumber}
import play.api.Logger
import scalaj.http.Http
import service.OverviewService.{checkExistsIndex, getListPackage, getRegionProvince}
import services.Configure
import services.domain.CommonService
import scala.concurrent.{Await, Future}
import scala.concurrent.ExecutionContext.Implicits.global
import scala.async.Async.{async, await}
import scala.concurrent.duration.Duration

object  ChecklistService{

  val client = Configure.client

  val logger = Logger(this.getClass())

  def getNumberOfContractChecklist(queryStr: String, queryNested: String) ={
    val queries = queryStr + " AND " + CommonUtil.filterCommon("tenGoi")
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
              "month": {
                  "terms": {
                       "field": "month",
                       "size": 24,
                       "order":{
                        "_key": "desc"
                       }
                  },
                  "aggs": {
                      "checklist": {
                          "nested": {
                             "path": "checklist"
                          }
                      }
                  }
              }
            },
            "size": 0
        }
        """
    val body = Http("http://172.27.11.151:9200/churn-checklist-*/docs/_search")
      .postData(query)
      .header("content-type", "application/JSON")
      .asString.body
    val json = Json.parse(body)
    val array = json.\("aggregations").\("month").\("buckets").get.asInstanceOf[JsArray].value.toArray
    array.map(x => (x.\("key").get.asInstanceOf[JsString].toString().replace("\"",""), x.\("doc_count").get.asInstanceOf[JsNumber].toString().toLong,
      x.\("checklist").\("doc_count").get.asInstanceOf[JsNumber].toString().toLong))
  }

  def getNumberOfContractAll(queryStr: String) ={
    val queries = queryStr + CommonUtil.filterCommon("package_name")
    val request = search("churn-contract-info-*" / "docs") query(queries) aggregations (
      termsAggregation("month")
        .field("month") size 24
      )  size 0 sortBy( fieldSort("month") order SortOrder.DESC)
    val rs = client.execute(request).await
    CommonService.getAggregationsSiglog(rs.aggregations.get("month")).filter(x=> x._1 != CommonService.getCurrentMonth()).sortWith((x, y) => x._1 > y._1)
  }

  def getUniqueContract(queryStr: String, queryNested: String, year: String) ={
    val queries = queryStr + " AND " + CommonUtil.filterCommon("tenGoi")
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
              "count": {
                  "cardinality": {
                       "field": "contract"
                  }
              }
            },
            "size": 0
        }
        """
    val body = Http(s"http://172.27.11.151:9200/churn-checklist-$year-*/docs/_search")
      .postData(query)
      .header("content-type", "application/JSON")
      .asString.body
    val json = Json.parse(body)
    json.\("aggregations").\("count").\("value").get.toString().toInt
  }

  def getChurnCtHaveChecklistsByStatus(month: String, region: String, age: String, _type: String, time: String, topTypes: String, stt: String) ={
    val arrMonth = CommonService.getRangeDateByLimit(month, 1, "month")
    val queries = arrMonth+ s" AND $stt "+"region:"+region+" AND lifeGroup:"+age+" AND "+CommonUtil.filterCommon("tenGoi")
    val queryNested_type = parseTypes(_type, topTypes)
    var queryNested_time = "*"
    if(time != "*" && time.indexOf(">=") <0) queryNested_time = "checklist.processTime:>="+time.split("-")(0)+" AND checklist.processTime:<"+time.split("-")(1)
    else if(time != "*" && time.indexOf(">=") >= 0) queryNested_time = s"checklist.processTime:$time"
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
                                             "query": "${queryNested_type.replace("\"", "\\\"")} AND ${queryNested_time.replace("\"", "\\\"")}"
                                       }
                                 }
                           }
                         }
                    ]
                }
            },
            "aggs": {
               "contract": {
                  "terms": {
                    "field": "contract",
                    "size": 1000000
                  },
                  "aggs": {
                    "nested_field": {
                      "nested": {
                        "path": "checklist"
                      },
                      "aggs": {
                        "count_field": {
                          "cardinality": {
                            "field": "checklist.id"
                          }
                        }
                      }
                    },
                    "FindIt": {
                      "bucket_selector": {
                        "buckets_path": {
                          "count_field": "nested_field > count_field"
                        },
                        "script": "params.count_field >1"
                      }
                    }
                  }
               }
            },
            "size": 0
        }
        """
    val body = Http("http://172.27.11.151:9200/churn-checklist-*/docs/_search")
      .postData(query)
      .header("content-type", "application/JSON")
      .asString.body
    val json = Json.parse(body)
    val array = json.\("aggregations").\("contract").\("buckets").get.asInstanceOf[JsArray].value.toArray
    array.length
  }

  def getChurnContractbyStatus(queryStr: String, queryNested: String) ={
    val queries = queryStr + " AND " + CommonUtil.filterCommon("tenGoi")
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
               "month": {
                  "terms": {
                     "field": "month",
                     "size": 24,
                     "order":{
                       "_key": "desc"
                     }
                  },
                  "aggs": {
                     "status": {
                        "terms": {
                           "field": "status",
                           "size": 1000
                        }
                     }
                  }
               }
            },
            "size": 0
        }
        """
    //println(query)
    val body = Http("http://172.27.11.151:9200/churn-checklist-*/docs/_search")
      .postData(query)
      .header("content-type", "application/JSON")
      .asString.body
    val json = Json.parse(body)
    val array = json.\("aggregations").\("month").\("buckets").get.asInstanceOf[JsArray].value.toArray
      .map(x => (x.\("key").get.asInstanceOf[JsString].toString().replace("\"",""), x.\("doc_count").get.asInstanceOf[JsNumber].toString().toLong,
        x.\("status").\("buckets").get.asInstanceOf[JsArray].value.toArray.map(y=> (y.\("key").get.asInstanceOf[JsNumber].toString(), y.\("doc_count").get.asInstanceOf[JsNumber].toString().toLong))))

    array.flatMap(x=> x._3.map(y=> (x._1, x._2) -> y)).map(x=> (x._1._1, x._2._1, x._2._2, x._1._2))
  }

  def getAllContractHaveCt(queryStr: String, queryNested: String) = {
    val queries = queryStr + " AND " + CommonUtil.filterCommon("tenGoi")
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
               "month": {
                  "terms": {
                     "field": "month",
                     "size": 24
                  }
               }
            },
            "size": 0
        }
        """
    //println(query)
    val body = Http(s"http://172.27.11.151:9200/churn-checklist-*/docs/_search")
      .postData(query)
      .header("content-type", "application/JSON")
      .asString.body
    val json = Json.parse(body)
    val array = json.\("aggregations").\("month").\("buckets").get.asInstanceOf[JsArray].value.toArray
      .map(x => (x.\("key").get.asInstanceOf[JsString].toString().replace("\"",""), x.\("doc_count").get.asInstanceOf[JsNumber].toString().toLong))
    array.filter(x=> x._1 != CommonService.getCurrentMonth()).sortWith((x, y) => x._1 > y._1).slice(0,12).sorted
  }

  def getQuantileProcessTime(queryStr: String, queryNested: String) ={
    val queries = queryStr + " AND " + CommonUtil.filterCommon("tenGoi")
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
              "checklist": {
                "nested": {
                  "path": "checklist"
                },
                "aggs": {
                  "quantile": {
                    "percentiles": {
                      "field": "checklist.processTime",
                      "percents": [
                        75
                      ]
                    }
                  }
                }
              }
            },
            "size": 0
        }
        """
    //println(query)
    val body = Http("http://172.27.11.151:9200/churn-checklist-*/docs/_search")
      .postData(query)
      .header("content-type", "application/JSON")
      .asString.body
    val json = Json.parse(body)
    val quantile = json.\("aggregations").\("checklist").\("quantile").\("values").\("75.0").get.asInstanceOf[JsValue].toString().replaceAll("\"", "")
    if(quantile == "NaN") 0.0 else quantile.toDouble
  }

  def getRangeTimeNested(time : String) = {
    if(time.indexOf(">=") < 0) ",\"aggs\":{\"countCt\":{\"filter\":{\"bool\":{\"must\":[{\"term\":{\"checklist.processTimeGroup\":\""+time+"\"}}]}}}}"
    else ",\"aggs\":{\"countCt\":{\"filter\":{\"bool\":{\"must\":[{\"term\":{\"checklist.processTimeGroup\":\">48\"}}]}}}}"
  }

  def getTypeChecklist(_type : String) = {
    ",\"aggs\":{\"countCt\":{\"filter\":{\"bool\":{\"must\":[{\"term\":{\"checklist.type\":\""+_type+"\"}}]}}}}"
  }

  // parse query type
  def parseTypes(_types: String, topTypes: String) = {
    val queries = _types match {
      case _types if(_types == "*")      => "*"
      case _types if(_types == "Others") => topTypes.split(",").filter(!_.contains("Others")).map(x=> "!(checklist.type:\""+x+"\")").mkString(" AND ")
      case _  => "checklist.type:\""+_types+"\""
    }
    queries
  }

  def calChecklistRatePertByMonth(callInArray: Array[(String, String, Long, Long)], allChurn: Array[(String, Long)], status: Int) ={
    val rs  = if(status == 13) callInArray.filter(x=> x._2.toInt == 1 || x._2.toInt == 3).filter(x=> allChurn.map(y=> y._1).indexOf(x._1) >=0)
                                .groupBy(x=> x._1).map(x=> (x._1, 11, x._2.map(y=> y._3).sum, x._2.map(y=> y._4).sum)).toArray
              else callInArray.filter(x=> x._2.toInt == status).filter(x=> allChurn.map(y=> y._1).indexOf(x._1) >=0)
    rs.map(x=> (x._1, x._3, x._4, allChurn.toMap.get(x._1).get)).map(x=>(x._1, CommonService.format3Decimal(x._2 * 100.00 / x._3), CommonService.format3Decimal(x._2 * 100.00 / x._4), x._2))
      .sortWith((x, y) => x._1 > y._1).slice(0, 15).sorted
  }

  def calChurnRateByYear(callInArray: Array[(String, String, Long, Long)], status: Int) ={
    val rs  = if(status == 13) callInArray.filter(x=> x._2.toInt == 1 || x._2.toInt == 3)
                               .groupBy(x=> x._1).map(x=> (x._1, 11, x._2.map(y=> y._3).sum, x._2.map(y=> y._4).sum)).toArray
              else callInArray.filter(x=> x._2.toInt == status)
    val rsYear = rs.groupBy(x=> x._2).map(x=> CommonService.format3Decimal(x._2.map(y=> y._3).sum * 100.00 / x._2.map(y=> y._4).sum))
    rsYear.sum
  }

  def getTopCause(month: String, field: String) ={
    val query = s"""
        {
            "aggs": {
              "checklist": {
                "nested": {
                  "path": "checklist"
                },
                "aggs": {
                  "top5": {
                    "terms": {
                      "field": "checklist.$field",
                      "size": 1000
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
    val array = json.\("aggregations").\("checklist").\("top5").\("buckets").get.asInstanceOf[JsArray].value.toArray
      .map(x => (x.\("key").get.asInstanceOf[JsString].toString().replace("\"",""), x.\("doc_count").get.asInstanceOf[JsNumber].toString().toLong))
    array.sortWith((x, y) => x._2 > y._2).slice(0,5)
  }

  def getFilterGroup(timeCL: Int, age:String, region:String, packages:String, month: String, fullMonth: Int): String = {
    val ageFilter = if(age == "") "*" else age.split(",").map(x=> if(x == "06-12") "6-12" else x).map(x=> "lifeGroup:"+x.trim()).mkString(" OR ")
    val packageFilter = if(packages == "") "*" else packages.split(",").map(x=> "package_name:\""+x.trim()+"\"").mkString(" OR ")
    val timeFilter = if(fullMonth == 0) CommonService.getRangeMonthInYearByLimit(month, timeCL.toInt) else CommonService.getRangeDateByLimit(month, timeCL.toInt, "month")
    val intRegex = """(\d+)""".r
    val filterCN = region match {
      case ""   => "*"
      case intRegex(region) => s"region:$region"
      case _       => "province:"+ProvinceUtil.getProvinceCode(region.toUpperCase())
    }
    s"($ageFilter) AND ($packageFilter) AND $filterCN AND ($timeFilter)"
  }

  def getQueryNested(cause: String, position: String, processTime: String)={
    s"($cause) AND ($position) AND ($processTime)"
  }

  def getInternet(request: Request[AnyContent]) = async{
    logger.info("========START SYNC CHECKLIST SERVICE=========")
    var status = 13
    var age = ""
    var region = ""
    var processTime = "*"
    var groupCL = "*"
    var timeCL = 24
    var position = "*"
    var cause = "*"
    var packages = ""
    var month = CommonService.getPrevMonth()
    var queries = "*"
    var queryNested = "*"
    if(!checkExistsIndex(s"churn-contract-info-$month")) month = CommonService.getPrevMonth(2)
    // get list province of region for filter
    val lstProvince = Await.result(Future{ getRegionProvince(CommonService.getPrevMonth(2)) }, Duration.Inf)
    // get list package for filter
    val lstPackage = Await.result(Future{ getListPackage(month).sortWith((x, y) => x._2 > y._2).map(x=> x._1) }, Duration.Inf)
    // get top 5 Nguyen nhan
    val topCause = getTopCause(month, "Nguyennhan").map(x=> x._1)
    // get top 5 vi Tri xay ra Checklist
    val topPosition = getTopCause(month, "vitrixayrasuco").map(x=> x._1)

    if(request != null) {
      groupCL = request.body.asFormUrlEncoded.get("groupCL").head
      status = if(request.body.asFormUrlEncoded.get("status").head == "") 13 else request.body.asFormUrlEncoded.get("status").head.toInt
      age = request.body.asFormUrlEncoded.get("age").head
      timeCL = request.body.asFormUrlEncoded.get("timeCL").head.toInt
      position = request.body.asFormUrlEncoded.get("position").head
      cause = request.body.asFormUrlEncoded.get("cause").head
      region = request.body.asFormUrlEncoded.get("region").head
      month = request.body.asFormUrlEncoded.get("month").head
      packages = request.body.asFormUrlEncoded.get("package").head
      queries = getFilterGroup(timeCL, age, region, packages, month, 1)
      //println(queries)
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
          position = topPosition.map(x => "!(checklist.vitrixayrasuco:\"" + x + "\")").mkString(" AND ")
        }
        case _ => {
          position = "checklist.vitrixayrasuco:\"" + request.body.asFormUrlEncoded.get("position").head + "\""
        }
      }
      queryNested = getQueryNested(cause, position, processTime)
    }
    val t0 = System.currentTimeMillis()

    // Chart 1: Number of Contracts That Have Checklist(s)
    val contractAll   = getNumberOfContractAll(queries)
    val ctCheckList   = getNumberOfContractChecklist(queries, queryNested).filter(x=> contractAll.map(y=> y._1).indexOf(x._1) >=0)
      .map(x=> (x._1, x._2, CommonService.format2Decimal(x._2 * 100.00 / contractAll.toMap.get(x._1).get), x._3)).sorted
    // Box Count Checklist
    val currCl = ctCheckList.filter(x=> x._1.matches(month.substring(0, month.indexOf("-")) +"-.*")).map(x=> x._2).sum.toInt
    val mapPrevCl = getNumberOfContractChecklist(getFilterGroup(timeCL, age, region, packages, CommonService.getPrevYYYYMM(month), 0), queryNested)
    val prevCl = mapPrevCl.map(x=> x._2).sum.toInt
    // Box Count Contract have Checklist
    val currCtCl = Await.result(Future{ getUniqueContract(queries, queryNested, CommonService.getCurrentYear()) }, Duration.Inf)
    val prevCtCl = Await.result(Future{ getUniqueContract(getFilterGroup(timeCL, age, region, packages, CommonService.getPrevYYYYMM(month), 0),
      queryNested, CommonService.getPrevYYYY(month)) }, Duration.Inf)

    logger.info("t0: "+(System.currentTimeMillis() -t0))
    val t1 = System.currentTimeMillis()

    // Chart 2: Churn Rate & Percentage For Customers Who Have Checklist
    val stt = if(status == 13) "(status:1 OR status:3)" else s"status:$status"
    val allChurn_count  = if(processTime == "*" && position == "*" && cause == "*") ChurnCallogService.getNumberOfContractAll(s"$stt AND $queries", "churn-contract-info-*")
                          else getAllContractHaveCt(s"$stt AND $queries", queryNested)
    val trendChecklist  = calChecklistRatePertByMonth(getChurnContractbyStatus(queries, queryNested), allChurn_count , status)
    logger.info("t1: "+(System.currentTimeMillis() -t1))
    val t2 = System.currentTimeMillis()

    // Box Ty le RM KH co checklist
    val currChurnChkl = Await.result(Future{ calChurnRateByYear(getChurnContractbyStatus(queries, queryNested), status)}, Duration.Inf)
    val prevChurnChkl = Await.result(Future{ calChurnRateByYear(getChurnContractbyStatus(getFilterGroup(timeCL, age, region, packages,
        CommonService.getPrevYYYYMM(month), 0), queryNested) , status) }, Duration.Inf)
    logger.info("t2: "+(System.currentTimeMillis() -t2))

    /*val t3 = System.currentTimeMillis()
    // Box Ty le rm KH co nhieu checklist trong thang
    val currCtrmCLGt1 = getChurnCtHaveChecklistsByStatus(month, region, age, _type, processTime, topTypes, stt)
    val prevCtrmCLGt1 = getChurnCtHaveChecklistsByStatus(CommonService.getPrevYYYYMM(month), region, age, _type, processTime, topTypes, stt)
    logger.info("t3: "+(System.currentTimeMillis() -t3))*/

    val t4 = System.currentTimeMillis()
    // Thoi gian xu ly checklist 75%
    val currQuantileTime = Await.result(Future{ getQuantileProcessTime(getFilterGroup(timeCL, age, region, packages, month, 0), queryNested) }, Duration.Inf)
    val prevQuantileTime = Await.result(Future{ getQuantileProcessTime(getFilterGroup(timeCL, age, region, packages, CommonService.getPrevYYYYMM(month), 0), queryNested) }, Duration.Inf)
    logger.info("t4: "+(System.currentTimeMillis() -t4))

    logger.info("Time: "+(System.currentTimeMillis() -t0))
    logger.info("========END SYNC CHECKLIST SERVICE=========")
    await(
      Future {
        ChecklistResponse((ContractNumber(currCtCl, prevCtCl), ContractNumber(currCl, prevCl), RateNumber(currQuantileTime, prevQuantileTime)), RateNumber(currChurnChkl, prevChurnChkl),
          ctCheckList.sortWith((x, y) => x._1 > y._1).slice(0, 15).sorted, trendChecklist, month, lstProvince, lstPackage, topCause, topPosition)
      }
    )
  }
}