package service

import churn.models.{DetectResponse, Indicator}

import scala.collection.immutable.Map
import com.sksamuel.elastic4s.ElasticsearchClientUri
import com.sksamuel.elastic4s.TcpClient
import com.sksamuel.elastic4s.http.ElasticDsl.{RichFuture, RichString, SearchHttpExecutable, SearchShow, percentilesAggregation, query, rangeAggregation, search, termsAgg, termsAggregation, _}
import com.sksamuel.elastic4s.http.search.SearchResponse
import play.api.libs.json.JsArray
import play.api.libs.json.JsNumber
import play.api.libs.json.JsObject
import play.api.libs.json.JsString
import play.api.libs.json.JsValue
import play.api.libs.json.Json
import churn.utils.{BytesUtil, CommonUtil}
import play.api.Logger
import play.api.mvc.{AnyContent, Request}

import scalaj.http.Http
import services.Configure
import services.domain.CommonService

object  ChurnDetectService{

  val client = Configure.client

  val logger = Logger(this.getClass())

  def getCtCallorChecklist(month: String, queryString: String, contractGrp: String, maintain: String, cate: String, cause: String, complain: String, _type: String) ={
    val indexes = if(contractGrp == "*" && maintain == "" && cate == "" && cause == "" && complain == "*") s"churn-${_type}-$month" else s"churn-detect-problem-$month"
    val queries = queryString + s" AND $complain AND "+CommonUtil.filterCommon("tenGoi")
    val full_queries = CommonUtil.getFullQueryHaveNested(queries, contractGrp, maintain, cate, cause)
    val query =
      s"""
      {
         ${full_queries},
         "size": 0
      }
      """
    //println(query)
    val body = Http(s"http://172.27.11.151:9200/${indexes}/docs/_search")
      .postData(query)
      .header("content-type", "application/JSON")
      .asString.body
    Json.parse(body).\("hits").\("total").get.asInstanceOf[JsNumber].toString().toInt
  }

  def getContractByMonth(indexes: String, queryString: String, contractGrp: String, maintain: String, cate: String, cause: String) ={
    val queries = queryString + " AND "+CommonUtil.filterCommon("tenGoi")
    val full_queries = CommonUtil.getFullQueryHaveNested(queries, contractGrp, maintain, cate, cause)
    val query =
      s"""
      {
         ${full_queries},
         "size": 0
      }
      """
    //println(query)
    val body = Http(s"http://172.27.11.151:9200/${indexes}/docs/_search")
      .postData(query)
      .header("content-type", "application/JSON")
      .asString.body
    Json.parse(body).\("hits").\("total").get.asInstanceOf[JsNumber].toString().toInt
  }

  def getNumCallChecklist(queryStr : String, fieldNested: String, maintain: String, cate: String, cause: String) = {
    val queries = queryStr + " AND "+CommonUtil.filterCommon("tenGoi")
    val full_queries = CommonUtil.getFullQueryHaveNested(queries, fieldNested, maintain, cate, cause)
    val query = s"""
        {
            ${full_queries},
            "size": 0
        }
        """
    //println(query)
    val body = Http("http://172.27.11.151:9200/churn-detect-problem-*/docs/_search")
      .postData(query)
      .header("content-type", "application/JSON")
      .asString.body
    Json.parse(body).\("hits").\("total").get.asInstanceOf[JsNumber].toString().toLong
  }

  def getnumByProblem(queryStr: String, month: String, contractGrp: String, maintain: String, cate: String, cause: String) = {
    val last6Month = CommonService.getLast6Month()
    //val queries = queryStr + " AND month:>="+last6Month+" AND month:<=" + month + " AND "+CommonUtil.filterCommon("tenGoi")
    val queries = queryStr + " AND "+CommonUtil.filterCommon("tenGoi")
    val full_queries = CommonUtil.getFullQueryHaveNested(queries, contractGrp, maintain, cate, cause)
    val query =
      s"""
      {
         ${full_queries},
         "aggs": {
            "Problem": {
               "terms": {
                  "field": "problem",
                  "size": 1000
               }
            }
         },
         "size": 0
      }
      """
    //println(query)
    val body = Http(s"http://172.27.11.151:9200/churn-detect-problem-${month}/docs/_search")
      .postData(query)
      .header("content-type", "application/JSON")
      .asString.body
    val json = Json.parse(body)
    val array = json.\("aggregations").\("Problem").\("buckets").get.asInstanceOf[JsArray].value.toArray
    val rss = array.map(y=> (y.\("key").get.asInstanceOf[JsString], y.\("doc_count").get.asInstanceOf[JsNumber]))
    rss.map(x=> (x._1.toString().replace("\"", ""), x._2.toString().toLong))
  }

  def getNumCtbyCause(queryStr: String, month: String, contractGrp: String, maintain: String, cate: String, cause: String) = {
    //val last6Month = CommonService.getLast6Month()
    //val queries = queryStr + " AND month:>="+last6Month+" AND month:<=" + month + " AND "+CommonUtil.filterCommon("tenGoi")
    val queries = queryStr + " AND "+CommonUtil.filterCommon("tenGoi")
    val full_queries = CommonUtil.getFullQueryHaveNested(queries, contractGrp,maintain, cate, cause)
    val query = s"""
        {
            ${full_queries},
            "aggs": {
                "checklist": {
                     "nested": {
                        "path": "checklist"
                     },
                     "aggs": {
                        "top_cause": {
                           "terms": {
                              "field": "checklist.lydo",
                              "size" : 20,
                              "order":{
                                "contract": "desc"
                              }
                           },
                           "aggs": {
                               "contract": {
                                   "reverse_nested": {}
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
    val body = Http(s"http://172.27.11.151:9200/churn-detect-problem-${month}/docs/_search")
      .postData(query)
      .header("content-type", "application/JSON")
      .asString.body
    val json = Json.parse(body)
    val array = json.\("aggregations").\("checklist").\("top_cause").\("buckets").get.asInstanceOf[JsArray].value.toArray
    val rss = array.map(y=> (y.\("key").get.asInstanceOf[JsString], y.\("contract").\("doc_count").get.asInstanceOf[JsNumber]))
    rss.map(x=> (x._1.toString().replace("\"", ""), x._2.toString().toLong))
  }

  def getMedicanByTobaotri(queryStr: String, month: String, contractGrp: String, maintain: String, cate: String, cause: String) = {
    //val last6Month = CommonService.getLast6Month()
    //val queries = queryStr + " AND month:>="+last6Month+" AND month:<=" + month + " AND "+CommonUtil.filterCommon("tenGoi")
    val queries = queryStr + " AND "+CommonUtil.filterCommon("tenGoi")
    val full_queries = CommonUtil.getFullQueryHaveNested(queries, contractGrp, maintain, cate, cause)
    val request =
      s"""
        {
             ${full_queries},
             "aggs": {
                  "checklist": {
                       "nested": {
                            "path": "checklist"
                       },
                       "aggs": {
                          "tobaotri": {
                              "terms": {
                                 "field": "checklist.tobaotri",
                                 "size": 20,
                                 "order": {
                                    "median.50": "desc"
                                 }
                              },
                              "aggs": {
                                  "median": {
                                      "percentiles": {
                                          "field": "checklist.processTime",
                                          "percents": [50]
                                      }
                                  },
                                  "contract":{
                                         "reverse_nested" :{}
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
    val body = Http(s"http://172.27.11.151:9200/churn-detect-problem-${month}/docs/_search")
      .postData(request)
      .header("content-type", "application/JSON")
      .asString.body
    val json = Json.parse(body)
    val array = json.\("aggregations").\("checklist").\("tobaotri").\("buckets").get.asInstanceOf[JsArray].value.toArray
    array.map(x => (x.\("key").get.asInstanceOf[JsString], x.\("median").\("values").\("50.0").get.asInstanceOf[JsNumber], x.\("contract").\("doc_count").get.asInstanceOf[JsNumber]))
      .map(x=> (x._1.toString().replace("\"",""), CommonService.format2Decimal(x._2.toString().toDouble), x._3.toString().toLong)).sortWith((x, y) => x._3 > y._3)
  }

  def getTrendHoursbyRegion(queryStr: String, month: String, contractGrp: String, maintain: String, cate: String, cause: String) = {
    val last6Month = CommonService.getLast6Month()
    val queries = queryStr + " AND month:>="+last6Month+" AND month:<=" + month + " AND "+CommonUtil.filterCommon("tenGoi")
    val full_queries = CommonUtil.getFullQueryHaveNested(queries, contractGrp, maintain, cate, cause)
    val request =
      s"""
        {
             ${full_queries},
             "aggs": {
                "month": {
                    "terms": {
                        "field": "month"
                    },
                    "aggs": {
                        "checklist": {
                            "nested": {
                               "path": "checklist"
                            },
                            "aggs": {
                                 "median": {
                                     "percentiles": {
                                         "field": "checklist.processTime",
                                         "percents": [
                                                50
                                         ]
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
    //println(request)
    val body = Http(s"http://172.27.11.151:9200/churn-full-*/docs/_search")
      .postData(request)
      .header("content-type", "application/JSON")
      .asString.body
    val json = Json.parse(body)
    val array = json.\("aggregations").\("month").\("buckets").get.asInstanceOf[JsArray].value.toArray
    array.map(x => (x.\("key").get.asInstanceOf[JsString], x.\("checklist").get.\("median").\("values").\("50.0").get.asInstanceOf[JsNumber]))
      .map(x=> (x._1.toString().replace("\"",""), CommonService.format2Decimal(x._2.toString().toDouble))).sorted
  }

  def getInfErrors(queryStr: String, month: String, _type: String, contractGrp: String, maintain: String, cate: String, cause: String) = {
    val last6Month = CommonService.getLast6Month()
    val queries = queryStr+" AND month:>="+last6Month+" AND month:<="+month+" AND "+CommonUtil.filterCommon("tenGoi")
    val full_queries = if(contractGrp.equals("-1"))
      s"""
        "query": {
                             "bool": {
                                  "filter": [
                                      {
                                         "query_string" : {
                                              "query": "${queries.replace("\"", "\\\"")}"
                                         }
                                      }
                                  ]
                             }
                     }
      """
    else CommonUtil.getFullQueryHaveNested(queries, contractGrp, maintain, cate, cause)
    val request =
      s"""
        {
             ${full_queries},
             "aggs": {
                "month": {
                    "terms": {
                        "field": "month"
                    },
                    "aggs": {
                         "median": {
                              "percentiles": {
                                  "field": "${_type}",
                                  "percents": [50]
                              }
                         }
                    }
                }
             },
             "size": 0
        }
        """
    //println(request)
    val body = Http(s"http://172.27.11.151:9200/churn-full-*/docs/_search")
      .postData(request)
      .header("content-type", "application/JSON")
      .asString.body
    val json = Json.parse(body)
    val array = json.\("aggregations").\("month").\("buckets").get.asInstanceOf[JsArray].value.toArray
    val rss = array.map(y=> (y.\("key").get.asInstanceOf[JsString], y.\("median").\("values").\("50.0").get.asInstanceOf[JsNumber]))
    rss.map(x=> (x._1.toString().replace("\"", ""), x._2.toString().toDouble)).sorted
  }

  def getTopCallContent(queryStr: String, month: String, numRecords: Long, contractGrp: String, maintain: String, cate: String, cause: String) = {
    /*val last6Month = CommonService.getLast6Month()
    val monthQueries = if(numRecords == 100) s"month:$month" else s"month:>=$last6Month AND month:<=$month"
    val queries = queryStr+" AND "+monthQueries+" AND "+CommonUtil.filterCommon("tenGoi")*/
    val queries = queryStr+" AND "+CommonUtil.filterCommon("tenGoi")
    val full_queries = if(contractGrp.equals("*"))
      s"""
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
                                             "path": "calllog",
                                                "query": {
                                                   "query_string": {
                                                       "query": "_exists_:calllog"
                                                   }
                                                }
                                         }
                                      }
                                  ]
                             }
                     }
      """
    else CommonUtil.getFullQueryHaveNested(queries, contractGrp, maintain, cate, cause)
    val request = s"""
        {
             ${full_queries},
             "sort":{
               "month": "desc"
             },
             "size": $numRecords
        }
        """
    //println(request)
    val body = Http(s"http://172.27.11.151:9200/churn-detect-problem-${month}/docs/_search")
      .postData(request)
      .header("content-type", "application/JSON")
      .asString.body
    val json = Json.parse(body)
    json.\("hits").\("hits").get.asInstanceOf[JsArray].value.toArray
          .map(y=> y.\("_source").\("contract").get.asInstanceOf[JsString] -> y.\("_source").\("calllog").get.asInstanceOf[JsArray].value.toArray.map(k=> k.\("time").get.asInstanceOf[JsString] -> k.\("content").get.asInstanceOf[JsString]))
      .flatMap(x=> x._2.map(y=> x._1.toString().replace("\"","") -> y))
      .map(x=> (x._1, x._2._1.toString().replace("\"",""), x._2._2.toString().replace("\"",""), ""))
  }

  def getTopChecklistContent(queryStr: String, month: String, numRecords: Long, contractGrp: String, maintain: String, cate: String, cause: String) = {
    //val last6Month = CommonService.getLast6Month()
    //val monthQueries = if(numRecords == 100) s"month:$month" else s"month:>=$last6Month AND month:<=$month"
    //val queries = queryStr+" AND "+monthQueries+" AND "+CommonUtil.filterCommon("tenGoi")
    val queries = queryStr+" AND "+CommonUtil.filterCommon("tenGoi")
    val full_queries = if(contractGrp.equals("*"))
      s"""
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
                                                       "query": "_exists_:checklist"
                                                   }
                                                }
                                         }
                                      }
                                  ]
                             }
                     }
      """
    else CommonUtil.getFullQueryHaveNested(queries, contractGrp, maintain, cate, cause)
    val request = s"""
        {
             ${full_queries},
             "sort":{
               "month":"desc"
             },
             "size": $numRecords
        }
        """
    //println(request)
    val body = Http(s"http://172.27.11.151:9200/churn-detect-problem-${month}/docs/_search")
      .postData(request)
      .header("content-type", "application/JSON")
      .asString.body
    val json = Json.parse(body)
    json.\("hits").\("hits").get.asInstanceOf[JsArray].value.toArray
      .map(y=> y.\("_source").\("contract").get.asInstanceOf[JsString] -> y.\("_source").\("checklist").get.asInstanceOf[JsArray].value.toArray.
        map(k=> (k.\("ngaytaochecklist").getOrElse(new JsString("")).asInstanceOf[JsString],
          k.\("ngayhoantatCL").getOrElse(new JsString("")).asInstanceOf[JsString],
          k.\("ghichu").getOrElse(new JsString("")).asInstanceOf[JsString])))
      .flatMap(x=> x._2.map(y=> x._1.toString().replace("\"","") -> y)).map(x=> (x._1, x._2._1.toString().replace("\"",""), x._2._2.toString().replace("\"",""), x._2._3.toString().replace("\"","")))
      .map(x=> (x._1, if(x._2.indexOf(".") >=0) x._2.substring(0, x._2.indexOf(".")) else x._2, if(x._3.indexOf(".") >=0) x._3.substring(0, x._3.indexOf(".")) else x._3, x._4))
  }

  def getContractByRegion(queryStr: String, month: String, indexs: String, groupContract: String, maintain: String, cate: String, cause: String) = {
    val queries = queryStr+" AND "+CommonUtil.filterCommon("tenGoi")
    val full_queries = CommonUtil.getFullQueryHaveNested(queries, groupContract, maintain, cate, cause)
    val request = s"""
        {
             ${full_queries},
             "size": 0,
             "aggs": {
                  "$indexs": {
                      "terms": {
                          "field": "$indexs",
                          "size" : 1000
                      }
                  }
             }
        }
        """
    //println(request)
    val body = Http(s"http://172.27.11.151:9200/churn-detect-problem-${month}/docs/_search")
      .postData(request)
      .header("content-type", "application/JSON")
      .asString.body
    val json = Json.parse(body)
    val array = json.\("aggregations").\(s"${indexs}").\("buckets").get.asInstanceOf[JsArray].value.toArray
    val rss = array.map(y=> (if(indexs == "region") y.\("key").get.asInstanceOf[JsNumber] else y.\("key").get.asInstanceOf[JsString], y.\("doc_count").get.asInstanceOf[JsNumber]))
    rss.map(x=> (x._1.toString().replace("\"", ""), x._2.toString().toLong)).sorted
  }

  def getTopCategory(queryStr: String, month: String, contractGrp: String, maintain: String, cate: String, cause: String) = {
    val last6Month = CommonService.getLast6Month()
    //val queries = queryStr + " AND month:>="+last6Month+" AND month:<="+month+" AND "+CommonUtil.filterCommon("tenGoi")
    val queries = queryStr + " AND "+CommonUtil.filterCommon("tenGoi")
    val full_queries = CommonUtil.getFullQueryHaveNested(queries, contractGrp, maintain, cate, cause)
    val query = s"""
        {
            ${full_queries},
            "aggs": {
                "calllog": {
                     "nested": {
                        "path": "calllog"
                     },
                     "aggs": {
                        "top_cate": {
                           "terms": {
                              "field": "calllog.cate",
                              "size" : 1000
                           },
                           "aggs": {
                               "contract": {
                                   "reverse_nested": {}
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
    val body = Http(s"http://172.27.11.151:9200/churn-detect-problem-${month}/docs/_search")
      .postData(query)
      .header("content-type", "application/JSON")
      .asString.body
    val json = Json.parse(body)
    val array = json.\("aggregations").\("calllog").\("top_cate").\("buckets").get.asInstanceOf[JsArray].value.toArray
    val rss = array.map(y=> (y.\("key").get.asInstanceOf[JsString], y.\("contract").\("doc_count").get.asInstanceOf[JsNumber]))
    rss.map(x=> (x._1.toString().replace("\"", ""), x._2.toString().toLong))
  }

  def getInternet(request: Request[AnyContent], isFwd: Int) = {
    logger.info("========START DETECT SERVICE=========")
    var status = "status:*"
    var contractGrp = "*"
    var region = request.flash.get("Region").getOrElse("*")
    var packages = request.flash.get("TenGoi").getOrElse("*")
        if(packages != "*") packages = "\"" + packages + "\""
    var age = request.flash.get("Age").getOrElse("*")
    var complain = "*"
    var cause = ""
    var maintain = ""
    var cate = ""
    var month = request.flash.get("Month").getOrElse("2018-11")
    if(isFwd == 1) {
      contractGrp = request.body.asFormUrlEncoded.get("groupCt").head
      status = "status:"+request.body.asFormUrlEncoded.get("status").head
      month = request.body.asFormUrlEncoded.get("month").head
      maintain = request.body.asFormUrlEncoded.get("maintain").head
      cate = if(request.body.asFormUrlEncoded.get("cate").head == "") "" else "\"" + request.body.asFormUrlEncoded.get("cate").head + "\""
      cause = if(request.body.asFormUrlEncoded.get("cause").head == "") "" else "\"" + request.body.asFormUrlEncoded.get("cause").head + "\""
      complain = if(request.body.asFormUrlEncoded.get("complain").head == "*") "*" else "problem:\"" + request.body.asFormUrlEncoded.get("complain").head + "\""
      packages = if(request.body.asFormUrlEncoded.get("package").head == "*") "*" else "\"" +request.body.asFormUrlEncoded.get("package").head +"\""
      region = request.body.asFormUrlEncoded.get("region").head
      age = request.body.asFormUrlEncoded.get("age").head
    }
    println(s"Age:$age\nRegion:$region\nMonth:$month")
    val time = System.currentTimeMillis()
    // chart 1
    val numContract  = getContractByMonth("churn-detect-problem-*", s"month:$month AND $complain AND lifeGroup:$age AND region:$region AND tenGoi:$packages", contractGrp, maintain, cate, cause)
    val numChurnCt   = getContractByMonth("churn-detect-problem-*", s"month:$month AND status:1 AND $complain AND lifeGroup:$age AND region:$region AND tenGoi:$packages", contractGrp, maintain, cate, cause)
    //val numAllChurn = getContractByMonth("churn-detect-problem-*", s"month:$month AND status:1 AND region:$region AND $complain", contractGrp, maintain, cate, cause)
    val numAllChurn = getContractByMonth("churn-detect-problem-*", s"month:$month AND status:1", "", "", "", "")
    val numCall      = getCtCallorChecklist(month, s"lifeGroup:$age AND region:$region AND tenGoi:$packages", contractGrp, maintain, cate, cause, complain, "calllog")
    val numChecklist = getCtCallorChecklist(month, s"lifeGroup:$age AND region:$region AND tenGoi:$packages", contractGrp, maintain, cate, cause, complain, "checklist")
    logger.info("t1: "+(System.currentTimeMillis() -time))
    val t2 = System.currentTimeMillis()

    /* OVERALL*/
      // chart 2
      val callYesCtYes = getNumCallChecklist(s"month:$month AND status:1 AND $complain AND lifeGroup:$age AND region:$region AND tenGoi:$packages", "Call Yes_Checklist Yes", maintain, cate, cause)
      val callYesCtNo  = getNumCallChecklist(s"month:$month AND status:1 AND $complain AND lifeGroup:$age AND region:$region AND tenGoi:$packages", "Call Yes_Checklist No",  maintain, cate, cause)
      val callNoCtYes  = getNumCallChecklist(s"month:$month AND status:1 AND $complain AND lifeGroup:$age AND region:$region AND tenGoi:$packages", "Call No_Checklist Yes", maintain, cate, cause)
      val callNoCtNo   = getNumCallChecklist(s"month:$month AND status:1 AND $complain AND lifeGroup:$age AND region:$region AND tenGoi:$packages", "Call No_Checklist No", maintain, cate, cause)

    logger.info("t2: "+(System.currentTimeMillis() -t2))
    val t3 = System.currentTimeMillis()

      // chart 8
      val numCtByRegion = getContractByRegion(s"$complain AND lifeGroup:$age AND tenGoi:$packages", month, "region", contractGrp, maintain, cate, cause)
      // chart 9
      val numCtByAge = getContractByRegion(s"$complain AND region:$region AND tenGoi:$packages", month, "lifeGroup", contractGrp, maintain, cate, cause)
      // chart 10
      val arrTengoi = getContractByRegion(s"$complain AND lifeGroup:$age AND region:$region", month, "tenGoi", contractGrp, maintain, cate, cause).sortWith((x, y) => x._2 > y._2)
      val numCtByTengoi = arrTengoi.slice(0,10) :+ ("Others", arrTengoi.slice(11, arrTengoi.length).map(x=> x._2).sum)

    logger.info("t3: "+(System.currentTimeMillis() -t3))
    val t4 = System.currentTimeMillis()

    /* CHECKLIST */
      //chart 5
      val medianHours = getTrendHoursbyRegion(s"$complain AND lifeGroup:$age AND region:$region AND tenGoi:$packages", month, contractGrp, maintain, cate, cause)
      //chart 6
      val medianMaintain = getMedicanByTobaotri(s"$complain AND lifeGroup:$age AND region:$region AND tenGoi:$packages", month, contractGrp, "", cate, cause)
      //chart 7
      val numCauses   = getNumCtbyCause(s"$complain AND lifeGroup:$age AND region:$region AND tenGoi:$packages", month, contractGrp, maintain, cate, "")
      // chart 12
      val topChecklistContent = getTopChecklistContent(s"$complain AND lifeGroup:$age AND region:$region AND tenGoi:$packages", month, 100, contractGrp, maintain, cate, cause).sortWith((x, y) => x._2 > y._2).slice(0,100)

    logger.info("t4: "+(System.currentTimeMillis() -t4))
    val t5 = System.currentTimeMillis()

    /* COMPLAIN */
      // chart 3
      val arrProblem   = getnumByProblem(s"$complain AND lifeGroup:$age AND region:$region AND tenGoi:$packages", month, contractGrp, maintain, cate, cause).sortWith((x,y) => x._2 > y._2)
    logger.info("t51: "+(System.currentTimeMillis() -t5))
    val t52 = System.currentTimeMillis()
    /* CALLOG GROUP*/
      // chart 4
      val topCates       = getTopCategory(s"$complain AND lifeGroup:$age AND region:$region AND tenGoi:$packages", month, contractGrp, maintain, "", cause).sortWith((x,y) => x._2 > y._2).slice(0,10)
    logger.info("t52: "+(System.currentTimeMillis() -t52))
    val t53 = System.currentTimeMillis()
    // chart 12
      val topCallContent = getTopCallContent(s"$complain AND lifeGroup:$age AND region:$region AND tenGoi:$packages", month, 100, contractGrp, maintain, cate, cause).sortWith((x, y) => x._2 > y._2).slice(0,100)

    logger.info("t53: "+(System.currentTimeMillis() -t53))
    val t6 = System.currentTimeMillis()

    /* INDICATOR*/
    // chart 11
      // splot 1
      val errorsChurn = getInfErrors(s"status:1 AND $complain AND lifeGroup:$age AND region:$region AND tenGoi:$packages", month, "inf", contractGrp, maintain, cate, cause).toMap
      val rsErrors = getInfErrors(s"lifeGroup:$age AND region:$region AND tenGoi:$packages", month, "inf", "-1", "", "", "").map(x=> (x._1, CommonService.format2Decimal(x._2), CommonService.format2Decimal(errorsChurn.get(x._1).getOrElse(0))))
    logger.info("--tt1--: "+(System.currentTimeMillis() -t6))
    val tt2 = System.currentTimeMillis()
    // splot 2
      val signinChurn = getInfErrors(s"status:1 AND $complain AND lifeGroup:$age AND region:$region AND tenGoi:$packages", month, "signin", contractGrp, maintain, cate, cause).toMap
      val rsSignin = getInfErrors(s"lifeGroup:$age AND region:$region AND tenGoi:$packages", month, "signin", "-1", "", "", "").map(x=> (x._1, CommonService.format2Decimal(x._2), CommonService.format2Decimal(signinChurn.get(x._1).getOrElse(0))))
    logger.info("--tt2--: "+(System.currentTimeMillis() -tt2))
    val tt3 = System.currentTimeMillis()
    // splot 3
      val suyhaoChurn = getInfErrors(s"status:1 AND $complain AND lifeGroup:$age AND region:$region AND tenGoi:$packages", month, "suyhao", contractGrp, maintain, cate, cause).toMap
      val rsSuyhao = getInfErrors(s"lifeGroup:$age AND region:$region AND tenGoi:$packages", month, "suyhao", "-1", "", "", "").map(x=> (x._1, CommonService.format2Decimal(x._2), CommonService.format2Decimal(suyhaoChurn.get(x._1).getOrElse(0))))
    logger.info("--tt3--: "+(System.currentTimeMillis() -tt3))
    val tt4 = System.currentTimeMillis()
    // splot 4
      val downChurn = getInfErrors(s"status:1 AND $complain AND lifeGroup:$age AND region:$region AND tenGoi:$packages", month, "usage", contractGrp, maintain, cate, cause).toMap
      val rsDownload = getInfErrors(s"lifeGroup:$age AND region:$region AND tenGoi:$packages", month, "usage", "-1", "", "", "")
        .map(x=> (x._1, CommonService.format2Decimal(BytesUtil.bytesToGigabytes(x._2.toLong)),CommonService.format2Decimal(BytesUtil.bytesToGigabytes(downChurn.get(x._1).getOrElse(0.0).toLong))))
    logger.info("--tt4--: "+(System.currentTimeMillis() -tt4))
    val tt5 = System.currentTimeMillis()
    // splot 5
      val feeChurn = getInfErrors(s"status:1 AND $complain AND lifeGroup:$age AND region:$region AND tenGoi:$packages", month, "fee", contractGrp, maintain, cate, cause).toMap
      val rsFee = getInfErrors(s"lifeGroup:$age AND region:$region AND tenGoi:$packages", month, "fee", "-1", "", "", "").map(x=> (x._1, CommonService.format2Decimal(x._2), CommonService.format2Decimal(feeChurn.get(x._1).getOrElse(0))))

    logger.info("--tt5--: "+(System.currentTimeMillis() -tt5))

    logger.info("Time: "+(System.currentTimeMillis() -time))
    val linkFilters = Map("region"-> region, "age"-> age, "package"-> request.flash.get("TenGoi").getOrElse("*"), "month" -> month)
    println(linkFilters)
    logger.info("========END DETECT SERVICE=========")
    DetectResponse((numContract, numChurnCt, numAllChurn, numCall, numChecklist), (callYesCtYes, callYesCtNo, callNoCtYes, callNoCtNo),(numCtByRegion, numCtByAge, numCtByTengoi),
      arrProblem, topCates, topCallContent, medianHours, numCauses, topChecklistContent, medianMaintain, Indicator(rsErrors, rsSignin, rsSuyhao, rsDownload, rsFee), linkFilters)
  }
}