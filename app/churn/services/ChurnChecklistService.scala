package service

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
import churn.utils.{AgeGroupUtil, CommonUtil, ProcessTimeUtil}
import org.elasticsearch.search.sort.SortOrder
import play.api.mvc.{AnyContent, Request}
import churn.models.{CallogResponse, ChecklistResponse}
import play.api.Logger

import scalaj.http.Http
import services.Configure
import services.domain.CommonService

object  ChurnChecklistService{

  val client = Configure.client

  val logger = Logger(this.getClass())

  def getNumberOfContractChecklist(region:String, age: String, _type: String, time: String) ={
    val queries = "region:"+region+" AND lifeGroup:"+age+" AND "+CommonUtil.filterCommon("tenGoi")
    val queryNested_type = parseTypes(_type)
    var queryNested_time = "*"
    var queryRangeTime = ""
    if(time != "*"){
      queryNested_time = if(time.indexOf(">=") < 0) "checklist.processTime:>="+time.split("-")(0)+" AND checklist.processTime:<"+time.split("-")(1) else s"checklist.processTime:$time"
      queryRangeTime = getRangeTimeNested(time)
    }
    val queryAggsType = if(_type == "*") "" else getTypeChecklist(_type)
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
              "month": {
                  "terms": {
                       "field": "month",
                       "size": 24
                  },
                  "aggs": {
                      "checklist": {
                          "nested": {
                             "path": "checklist"
                          }
                          $queryRangeTime
                          $queryAggsType
                      }
                  }
              }
            },
            "size": 0,
            "sort": [
               {
                 "month": {
                    "order": "desc"
                 }
               }
            ]
        }
        """
    //println(query)
    val body = Http("http://172.27.11.151:9200/churn-checklist-*/docs/_search")
      .postData(query)
      .header("content-type", "application/JSON")
      .asString.body
    val json = Json.parse(body)
    val array = json.\("aggregations").\("month").\("buckets").get.asInstanceOf[JsArray].value.toArray
    array.map(x => (x.\("key").get.asInstanceOf[JsString].toString().replace("\"",""), x.\("doc_count").get.asInstanceOf[JsNumber].toString().toLong,
      if(time == "*" && _type == "*") x.\("checklist").\("doc_count").get.asInstanceOf[JsNumber].toString().toLong else x.\("checklist").\("countCt").\("doc_count").get.asInstanceOf[JsNumber].toString().toLong))
  }

  def getChurnContractbyStatus(region: String, age: String, _type: String, time: String) ={
    val queries = "region:"+region+" AND lifeGroup:"+age+" AND "+CommonUtil.filterCommon("tenGoi")
    val queryNested_type = if(_type == "*") "*" else "checklist.type:\""+_type+"\""
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
               "month": {
                  "terms": {
                     "field": "month",
                     "size": 24
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
            "size": 0,
            "sort": [
               {
                 "month": {
                    "order": "desc"
                 }
               }
            ]
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

  def getChurnCallInbyRegionChecklist(age: String, _type: String, time: String) ={
    val queries = "lifeGroup:"+age+" AND "+CommonUtil.filterCommon("tenGoi")
    val queryNested_type = if(_type == "*") "*" else "checklist.type:\""+_type+"\""
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
               "month": {
                  "terms": {
                     "field": "month",
                     "size": 15
                  },
                  "aggs": {
                     "region": {
                        "terms": {
                           "field": "region",
                           "size": 1000
                        }
                     }
                  }
               }
            },
            "size": 0,
            "sort": [
               {
                 "month": {
                    "order": "desc"
                 }
               }
            ]
        }
        """
    //println(query)
    val body = Http("http://172.27.11.151:9200/churn-checklist-*/docs/_search")
      .postData(query)
      .header("content-type", "application/JSON")
      .asString.body
    val json = Json.parse(body)
    val array = json.\("aggregations").\("month").\("buckets").get.asInstanceOf[JsArray].value.toArray
      .map(x => (x.\("key").get.asInstanceOf[JsString].toString().replace("\"",""),
        x.\("region").\("buckets").get.asInstanceOf[JsArray].value.toArray.map(y=> (y.\("key").get.asInstanceOf[JsNumber].toString(), y.\("doc_count").get.asInstanceOf[JsNumber].toString().toLong))))

    array.flatMap(x=> x._2.map(y=> x._1 -> y)).map(x=> (x._1, x._2._1, x._2._2)).filter(x=> x._1 != CommonService.getCurrentMonth()).sortWith((x, y) => x._1 > y._1)
  }

  def getChecklistRegionMonth(age: String, _type: String, time: String) = {
    val queries = "lifeGroup:"+age+" AND "+CommonUtil.filterCommon("tenGoi")
    val queryNested_type = if(_type == "*") "*" else "checklist.type:\""+_type+"\""
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
               "month": {
                  "terms": {
                     "field": "month",
                     "size": 24
                  },
                  "aggs": {
                     "status": {
                        "terms": {
                           "field": "status",
                           "size": 1000
                        },
                        "aggs": {
                           "region": {
                               "terms": {
                                  "field": "region",
                                  "size": 1000
                               }
                           }
                        }
                     }
                  }
               }
            },
            "size": 0,
            "sort": [
               {
                 "month": {
                    "order": "desc"
                 }
               }
            ]
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
        x.\("status").\("buckets").get.asInstanceOf[JsArray].value.toArray
          .map(y=> (y.\("key").get.asInstanceOf[JsNumber].toString(), y.\("doc_count").get.asInstanceOf[JsNumber].toString().toInt,
            y.\("region").\("buckets").get.asInstanceOf[JsArray].value.toArray.map(k=> (k.\("key").get.asInstanceOf[JsNumber].toString(), k.\("doc_count").get.asInstanceOf[JsNumber].toString().toInt))))))
    array.flatMap(x=> x._3.map(y=> (x._1, x._2) -> y)).map(x=> (x._1._1, x._1._2, x._2._1, x._2._2, x._2._3)).flatMap(x=> x._5.map(y=> (x._1, x._2, x._3, x._4) -> y))
      .map(x=> (x._1._1, x._1._3, x._2._1, x._1._4, x._2._2))

  }

  def getChurnByRegionAgeChecklist(month: String, _type: String, time: String) ={
    val queries = CommonUtil.filterCommon("tenGoi")
    val queryNested_type = if(_type == "*") "*" else "checklist.type:\""+_type+"\""
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
                "age": {
                    "range": {
                       "field": "age",
                       "ranges": [
                          {
                            "key": "6",
                            "from": 0,
                            "to": 6.0001
                          },
                          {
                            "key": "12",
                            "from": 6.001,
                            "to": 12.001
                          },
                          {
                            "key": "18",
                            "from": 12.001,
                            "to": 18.001
                          },
                          {
                            "key": "24",
                            "from": 18.001,
                            "to": 24.001
                          },
                          {
                            "key": "30",
                            "from": 24.001,
                            "to": 30.001
                          },
                          {
                            "key": "36",
                            "from": 30.001,
                            "to": 36.001
                          },
                          {
                            "key": "42",
                            "from": 36.001,
                            "to": 42.001
                          },
                          {
                            "key": "48",
                            "from": 42.001,
                            "to": 48.001
                          },
                          {
                            "key": "54",
                            "from": 48.001,
                            "to": 54.001
                          },
                          {
                            "key": "60",
                            "from": 54.001,
                            "to": 60.001
                          },
                          {
                            "key": "66",
                            "from": 60.001,
                            "to": 1.7976931348623157e+308
                          }
                        ]
                      },
                      "aggs": {
                         "region": {
                            "terms": {
                                "field": "region",
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

  def getChecklistRegionAge(month: String, _type: String) = {
    val queries = CommonUtil.filterCommon("tenGoi")
    val queryNested = if(_type == "*") "*" else "checklist.type:\""+_type+"\""
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
                     "field": "region"
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
    array.flatMap(x=> x._3.map(y=> (x._1, x._2) -> y)).map(x=> (x._1._1, x._1._2, x._2._1, x._2._2, x._2._3)).flatMap(x=> x._5.map(y=> (x._1, x._2, x._3, x._4) -> y))
      .map(x=> (x._1._1, x._1._3, x._2._1.replace("\"",""), x._1._4, x._2._2))

  }

  def getChurnByRegionProcessTime(month: String, age: String, _type: String) ={
    val queries = "lifeGroup:"+age+" AND "+CommonUtil.filterCommon("tenGoi")
    val queryNested = if(_type == "*") "*" else "checklist.type:\""+_type+"\""
    val queryAggsType = if(_type == "*") "" else getTypeChecklist(_type)
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
             "size": 0,
                   "aggs": {
                       "region": {
                         "terms": {
                           "field": "region"
                         },
                         "aggs": {
                           "checklist": {
                             "nested": {
                               "path": "checklist"
                             },
                             "aggs": {
                               "time": {
                                 "range": {
                                      "field": "checklist.processTime",
                                      "ranges": [
                                        {
                                          "key": "3",
                                          "from": 0,
                                          "to": 3
                                        },
                                        {
                                          "key": "24",
                                          "from": 3,
                                          "to": 24
                                        },
                                        {
                                          "key": "48",
                                          "from": 24,
                                          "to": 48
                                        },
                                        {
                                          "key": "60",
                                          "from": 48,
                                          "to": 1.7976931348623157e+308
                                        }
                                      ]
                                 }
                                 $queryAggsType
                               }
                             }
                           }
                         }
                       }
                     }
        }
        """
    //println(request)
    val body = Http(s"http://172.27.11.151:9200/churn-checklist-${month}/docs/_search")
      .postData(request)
      .header("content-type", "application/JSON")
      .asString.body
    val json = Json.parse(body)
    val array = json.\("aggregations").\("region").\("buckets").get.asInstanceOf[JsArray].value.toArray
    val rss = array.map(x => (x.\("key").get.asInstanceOf[JsNumber], x.\("checklist").get.\("time").\("buckets").get.asInstanceOf[JsArray].value.toArray
      .map(y=> (y.\("key").get.asInstanceOf[JsString], if(_type == "*") y.\("doc_count").get.asInstanceOf[JsNumber] else y.\("countCt").\("doc_count").get.asInstanceOf[JsNumber]))))
    rss.flatMap(x=> x._2.map(y=> x._1.value -> y)).map(x=> (x._2._1.toString().replace("\"", "").toInt, x._1.toString().toLong, x._2._2.toString().toLong))

  }

  def getChurnByRegionType(month: String, age: String, time: String) ={
    val queries = "lifeGroup:"+age+" AND "+CommonUtil.filterCommon("tenGoi")
    var queryNested = "*"
    if(time != "*" && time.indexOf(">=") <0) queryNested = "checklist.processTime:>="+time.split("-")(0)+" AND checklist.processTime:<"+time.split("-")(1)
    else if(time != "*" && time.indexOf(">=") >= 0) queryNested = s"checklist.processTime:$time"
    val queryRangeTime = if(time == "*") "" else getRangeTimeNested(time)
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
            "size": 0,
                   "aggs": {
                       "region": {
                         "terms": {
                           "field": "region"
                         },
                         "aggs": {
                           "checklist": {
                             "nested": {
                               "path": "checklist"
                             },
                             "aggs": {
                               "type": {
                                 "terms": {
                                      "field": "checklist.type",
                                      "size": 1000
                                 }
                                 $queryRangeTime
                               }
                             }
                           }
                         }
                       }
                     }
        }
        """
    //println(request)
    val body = Http(s"http://172.27.11.151:9200/churn-checklist-${month}/docs/_search")
      .postData(request)
      .header("content-type", "application/JSON")
      .asString.body
    val json = Json.parse(body)
    val array = json.\("aggregations").\("region").\("buckets").get.asInstanceOf[JsArray].value.toArray
    val rss = array.map(x => (x.\("key").get.asInstanceOf[JsNumber], x.\("checklist").get.\("type").\("buckets").get.asInstanceOf[JsArray].value.toArray
      .map(y=> (y.\("key").get.asInstanceOf[JsString], if(time == "*") y.\("doc_count").get.asInstanceOf[JsNumber] else y.\("countCt").\("doc_count").get.asInstanceOf[JsNumber]))))
    rss.flatMap(x=> x._2.map(y=> x._1.value -> y)).map(x=> (x._2._1.toString().replace("\"", ""), x._1.toString().toLong, x._2._2.toString().toLong))

  }

  def getChecklistRegionType(month: String, age: String, time: String) = {
    val queries = "lifeGroup:"+age+" AND "+CommonUtil.filterCommon("tenGoi")
    var queryNested = "*"
    if(time != "*" && time.indexOf(">=") <0) queryNested = "checklist.processTime:>="+time.split("-")(0)+" AND checklist.processTime:<"+time.split("-")(1)
    else if(time != "*" && time.indexOf(">=") >= 0) queryNested = s"checklist.processTime:$time"
    val queryRangeTime = if(time == "*") "" else getRangeTimeNested(time)
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
                    "type": {
                      "terms": {
                        "field": "checklist.type",
                        "size": 1000
                      },
                      "aggs": {
                        "contract": {
                          "reverse_nested": {},
                          "aggs": {
                            "region": {
                              "terms": {
                                "field": "region"
                              },
                              "aggs": {
                                "status": {
                                  "terms": {
                                    "field": "status"
                                  }
                                  $queryRangeTime
                                }
                              }
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
    val array = json.\("aggregations").\("checklist").get.\("type").\("buckets").get.asInstanceOf[JsArray].value.toArray
      .map(x => (x.\("key").get.asInstanceOf[JsString].toString().replace("\"",""),
               x.\("contract").get.\("region").\("buckets").get.asInstanceOf[JsArray].value.toArray.map(
                  y =>  (y.\("key").get.asInstanceOf[JsNumber].toString().toInt, y.\("doc_count").get.asInstanceOf[JsNumber].toString().toLong,
                  y.\("status").\("buckets").get.asInstanceOf[JsArray].value.toArray.map(k=> (k.\("key").get.asInstanceOf[JsNumber].toString().toInt, k.\("doc_count").get.asInstanceOf[JsNumber].toString().toLong)))
               )
        )
      )

    array.flatMap(x=> x._2.map(y=> x._1 -> y)).map(x=> (x._1, x._2._1,x._2._2, x._2._3)).flatMap(x=> x._4.map(y=> (x._1, x._2, x._3) -> y)).
      map(x=> (x._1._1, x._1._2, x._2._1, x._2._2, x._1._3))
  }

  def getOtherTypeRegion(month: String, age: String, time: String,  _type: String) = {
    val queries = "lifeGroup:"+age+" AND "+CommonUtil.filterCommon("tenGoi")
    val queryNested_type = if(_type == "") "*" else _type
    var queryNested_time = "*"
    if(time != "*" && time.indexOf(">=") <0) queryNested_time = "checklist.processTime:>="+time.split("-")(0)+" AND checklist.processTime:<"+time.split("-")(1)
    else if(time != "*" && time.indexOf(">=") >= 0) queryNested_time = s"checklist.processTime:$time"
    val queryRangeTime = if(time == "*") "" else getRangeTimeNested(time)
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
               "region": {
                  "terms": {
                     "field": "region"
                  },
                  "aggs": {
                     "status": {
                        "terms": {
                           "field": "status"
                        }
                        $queryRangeTime
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
      .map(x => (x.\("key").get.asInstanceOf[JsNumber].toString().toInt, x.\("doc_count").get.asInstanceOf[JsNumber].toString().toLong,
        x.\("status").\("buckets").get.asInstanceOf[JsArray].value.toArray.map(y=> (y.\("key").get.asInstanceOf[JsNumber].toString().toInt, y.\("doc_count").get.asInstanceOf[JsNumber].toString().toLong))))
    array.flatMap(x=> x._3.map(y=> (x._1,x._2) -> y)).map(x=> ("Others",x._1._1, x._2._1, x._2._2, x._1._2))
  }

  def getAllContractHaveCt(status: String, _type: String) = {
    val fieldQuery = if(_type == "*") "checklist.processTime:>=0" else "checklist.type:*"
    val queries = status +" AND "+CommonUtil.filterCommon("tenGoi")
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
                                    "query": "${fieldQuery.replace("\"", "\\\"")}"
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
                     "size": 13
                  }
               }
            },
            "size": 0
        }
        """
    println(query)
    val body = Http(s"http://172.27.11.151:9200/churn-checklist-*/docs/_search")
      .postData(query)
      .header("content-type", "application/JSON")
      .asString.body
    val json = Json.parse(body)
    val array = json.\("aggregations").\("month").\("buckets").get.asInstanceOf[JsArray].value.toArray
      .map(x => (x.\("key").get.asInstanceOf[JsString].toString().replace("\"",""), x.\("doc_count").get.asInstanceOf[JsNumber].toString().toLong))
    array.filter(x=> x._1 != CommonService.getCurrentMonth()).sortWith((x, y) => x._1 > y._1).slice(0,12).sorted
  }

  def calTrendTypeRegion(array: Array[(String, Int, Int, Long, Long)], status: Int, sumAll_churn: Array[(Int, Long)]) = {
    val rsHuyDv = array.filter(x=> x._3 == status).map(x=> (x._1, x._2, x._4, x._5))
    //rsHuyDv.foreach(println)
    rsHuyDv.map(x=> (x._1, x._2, CommonService.format2Decimal(x._3 * 100.00 / x._4), CommonService.format2Decimal(x._3 * 100.00 / sumAll_churn.find(y=> y._1 == x._2).get._2), x._3))
  }

  def calTrendCallinRateAndPercentage(callInArray: Array[(String, String, Long, Long)], allChurn: Array[(String, Long)], status: Int) ={
    val rs  = callInArray.filter(x=> x._2.toInt == status).filter(x=> allChurn.map(y=> y._1).indexOf(x._1) >=0)
    rs.map(x=> (x._1, x._3, x._4, allChurn.toMap.get(x._1).get)).map(x=>(x._1, CommonService.format3Decimal(x._2 * 100.00 / x._3), CommonService.format3Decimal(x._2 * 100.00 / x._4)))
      .sortWith((x, y) => x._1 > y._1).slice(0, 12).sorted
  }

  def getRangeTimeNested(time : String) = {
    if(time.indexOf(">=") < 0) ",\"aggs\":{\"countCt\":{\"filter\":{\"bool\":{\"must\":[{\"range\":{\"checklist.processTime\":{\"gte\":"+time.split("-")(0)+",\"lt\":"+time.split("-")(1)+"}}}]}}}}"
    else ",\"aggs\":{\"countCt\":{\"filter\":{\"bool\":{\"must\":[{\"range\":{\"checklist.processTime\":{\"gte\":"+time.substring(time.indexOf("=")+1)+"}}}]}}}}"
  }

  def getTypeChecklist(_type : String) = {
    ",\"aggs\":{\"countCt\":{\"filter\":{\"bool\":{\"must\":[{\"term\":{\"checklist.type\":\""+_type+"\"}}]}}}}"
  }

  // parse query type
  def parseTypes(_types: String) = {
    val queries = _types match {
      case _types if(_types == "*")      => "*"
      case _types if(_types == "Others") => _types.split(",").filter(!_.contains("Others")).map(x=> "!(checklist.type:\""+x+"\")").mkString(" AND ")
      case _  => "checklist.type:\""+_types+"\""
    }
    queries
  }

  def getInternet(request: Request[AnyContent]) = {
    logger.info("========START CHECKLIST SERVICE=========")
    var status = 1 // Huy dich vu default
    var ageChecklist = "*"
    var ageAll = "*"
    var region = "*"
    var _type = "*"
    var processTime = "*"
    var month = CommonService.getPrevMonth()
    if(request != null) {
      status = request.body.asFormUrlEncoded.get("status").head.toInt
      if(request.body.asFormUrlEncoded.get("age").head != "" && request.body.asFormUrlEncoded.get("age").head == "12"){
        ageChecklist =  "\"6-12\""
      }
      else if(request.body.asFormUrlEncoded.get("age").head != "" && request.body.asFormUrlEncoded.get("age").head != "12"){
        ageChecklist = "\"" + AgeGroupUtil.getAgeById(request.body.asFormUrlEncoded.get("age").head.toInt) + "\""
      }
      ageAll = if(request.body.asFormUrlEncoded.get("age").head != "")  AgeGroupUtil.getCalAgeByName(AgeGroupUtil.getAgeById(request.body.asFormUrlEncoded.get("age").head.toInt)) else "*"
      region = if(request.body.asFormUrlEncoded.get("region").head != "") request.body.asFormUrlEncoded.get("region").head else "*"
      month = request.body.asFormUrlEncoded.get("month").head
      processTime = if(request.body.asFormUrlEncoded.get("processTime").head != "") request.body.asFormUrlEncoded.get("processTime").head else "*"
      /*request.body.asFormUrlEncoded.get("typeCurr").head match {
        case "" => {
          _type = "*"
        }
        case "Others" =>  {
          val topTypes = request.body.asFormUrlEncoded.get("topType").head
          _type = topTypes.split(",").filter(!_.contains("Others")).map(x=> "!(checklist.type:\""+x+"\")").mkString(" AND ")
        }
        case _ => {
          _type = "checklist.type:\""+request.body.asFormUrlEncoded.get("typeCurr").head+"\""
        }
      }*/
      _type = if(request.body.asFormUrlEncoded.get("typeCurr").head != "") request.body.asFormUrlEncoded.get("typeCurr").head else "*"
    }
    val t0 = System.currentTimeMillis()
    // Chart 1: Number of Contracts That Have Checklist(s)
    val contractAll   = ChurnCallogService.getNumberOfContractAll(s"region:$region AND $ageAll", "churn-contract-info-*").slice(0,12).sorted
    val ctCheckList   = getNumberOfContractChecklist(region, ageChecklist, _type, processTime).filter(x=> contractAll.map(y=> y._1).indexOf(x._1) >=0)
      .map(x=> (x._1, x._2, CommonService.format2Decimal(x._2 * 100.00 / contractAll.toMap.get(x._1).get), x._3)).sorted
    logger.info("t0: "+(System.currentTimeMillis() -t0))
    val t1 = System.currentTimeMillis()
    // Chart 2: Churn Rate & Percentage For Customers Who Have Checklist
    val allChurn_count  = if(processTime == "*" && _type == "*") ChurnCallogService.getNumberOfContractAll(s"status:$status AND $ageAll AND region:$region", "churn-contract-info-*")
                          else getAllContractHaveCt(s"status:$status", _type)
    val trendChecklist  = calTrendCallinRateAndPercentage(getChurnContractbyStatus(region, ageChecklist, _type, processTime), allChurn_count , status)
    logger.info("t1: "+(System.currentTimeMillis() -t1))
    val t2 = System.currentTimeMillis()
    // Chart 3: Number of Contracts That Have Checklist(s) by Region (%) 2-3 sai
    val ctAllRegion     = ChurnCallogService.getChurnCallInbyRegionAll(s"$ageAll AND !(region:0)")
    val checklistRegion = ChurnCallogService.calChurnCallinRateAndPercentagebyRegion(getChurnCallInbyRegionChecklist(ageChecklist, _type, processTime), ctAllRegion)
    logger.info("t2: "+(System.currentTimeMillis() -t2))
    val t3 = System.currentTimeMillis()
    // Chart 4: For Contracts That Have Checklist(s): Churn Rate and Churn Percentage by Region 2-3 sai
    val trendRegionMonth = ChurnRegionService.calChurnRateAndPercentageForRegionMonth(getChecklistRegionMonth(ageChecklist, _type, processTime), status).filter(x=> x._2 != CommonService.getCurrentMonth()).sorted
    val top12monthRegion = trendRegionMonth.map(x=> x._2).distinct.sortWith((x, y) => x > y).filter(x=> x != CommonService.getCurrentMonth()).slice(0,12).sorted
    val topMonth = if(top12monthRegion.length >= 12) 13 else top12monthRegion.length+1
    val mapMonthRegion   = if(top12monthRegion.length >0) (1 until topMonth).map(x=> top12monthRegion(x-1) -> x).toMap else Map[String, Int]()
    val rsRegionMonth    = if(top12monthRegion.length >0) trendRegionMonth.filter(x=> top12monthRegion.indexOf(x._2) >=0).map(x=> (x._1, mapMonthRegion.get(x._2).get, x._3, x._4, x._5))
                           else Array[(Int, Int, Double, Double, Int)]()
    var arrGroup = Array[(Int, Int)]()
    val mapRegion = CommonUtil.REGION.map(x=> x._2).toArray
    val mapMonths = mapMonthRegion.map(x=> x._2).toArray
    val arrEmpty = Array((0.0, 0.0, 0))
    for(i <- 0 until mapRegion.size){
      for(j <- 0 until mapMonths.size){
        arrGroup :+= (mapRegion(i) -> mapMonths(j))
      }
    }
    val checklistRegionMonth = arrGroup.map(x=> (x._1, x._2, if(rsRegionMonth.filter(y=> y._1 == x._1).filter(y=> y._2 == x._2).length ==0) arrEmpty else rsRegionMonth.filter(y=> y._1 == x._1).filter(y=> y._2 == x._2)
      .map(x=> (x._3, x._4, x._5)))).flatMap(x=> x._3.map(y=> (x._1, x._2) -> y))
      .map(x=> (x._1._1, x._1._2, x._2._1, x._2._2, x._2._3))
    logger.info("t3: "+(System.currentTimeMillis() -t3))
    val t4 = System.currentTimeMillis()
    // Chart 5: Number of Contracts Who Have Checklist(s) by Region by Contract Age (%)
    val numOfChecklist     = getChurnByRegionAgeChecklist(month, _type, processTime)
    val numOfContract      = ChurnCallogService.getChurnByRegionAgeAll(month)
    val checklistRegionAge = numOfChecklist.map(x=> (x._1, x._2, CommonService.format2Decimal(x._3 * 100.00 / numOfContract.filter(y=> y._1 == x._1).filter(y=> y._2 == x._2).map(y=> y._3).sum), x._3))
    logger.info("t4: "+(System.currentTimeMillis() -t4))
    val t5 = System.currentTimeMillis()
    // Chart 6: For Contracts Who Have Checklist(s): Churn Rate and Churn Percentage by Region by Contract Age
    val trendRegionAge = ChurnCallogService.calCallInRateAndPercentageRegionAge(getChecklistRegionAge(month, _type), status)
    val mapAge = AgeGroupUtil.AGE.map(y=> y._2).toArray
    val mapRegion1 = CommonUtil.REGION.map(x=> x._2).toArray
    var mapGroup1 = Array[(Int, Int)]()
    val arrEmpty1 = Array((0.0, 0.0, 0))
    for(i <- 0 until mapRegion1.size){
      for(j <- 0 until mapAge.size){
        mapGroup1 :+= (mapRegion1(i) -> mapAge(j))
      }
    }
    val rsTrendRegionAge = mapGroup1.map(x=> (x._1, x._2, if(trendRegionAge.filter(y=> y._1 == x._1).filter(y=> x._2 == AgeGroupUtil.getAgeIdByName(y._2)).length ==0) arrEmpty1 else trendRegionAge.filter(y=> y._1 == x._1)
      .filter(y=> x._2 == AgeGroupUtil.getAgeIdByName(y._2)).map(x=> (x._3, x._4, x._5))))
      .flatMap(x=> x._3.map(y=> (x._1, x._2) -> y))
      .map(x=> (x._1._1, x._1._2, x._2._1, x._2._2, x._2._3))
    logger.info("t5: "+(System.currentTimeMillis() -t5))
    val t6 = System.currentTimeMillis()
    // Chart 7: Number of Checklist(s) by Checklist Processing Time by Region
    val numOfProcessTime   = getChurnByRegionProcessTime(month, ageChecklist, _type)
    val time_X     = numOfProcessTime.map(x=> x._1).distinct.sorted.map(x=> ProcessTimeUtil.getNameById(x))
    val region_Y   = numOfProcessTime.map(x=> x._2).distinct.sorted
    val dataChurn7 = numOfProcessTime.map(x=> (ProcessTimeUtil.getIndexById(x._1), x._2 -1, x._3))
    logger.info("t6: "+(System.currentTimeMillis() -t6))
    val t7 = System.currentTimeMillis()
    // Chart 8: Number of Checklist(s) by Checklist Types by Region
    val numOfTypeRegion = getChurnByRegionType(month, ageChecklist, processTime)
    val top12Types = numOfTypeRegion.groupBy(x=> x._1).map(x=> x._1 -> x._2.map(y=> y._3).sum).toArray.sortWith((x, y)=> x._2> y._2).slice(0,12).map(x=> x._1)

    val types_X = if(top12Types.length >=12) (0 until 12).map(x=> x -> top12Types(x)) ++ Array((12 -> "Others")) else (0 until top12Types.length).map(x=> x -> top12Types(x))
    val types_Y = numOfTypeRegion.map(x=> x._2).distinct.sorted

    val mapTopTypes = numOfTypeRegion.filter(x=> top12Types.indexOf(x._1) >= 0).map(x=> (types_X.find(y=> y._2 == x._1).get._1, x._2 -1, x._3))
    val othersType  = numOfTypeRegion.filter(x=> top12Types.indexOf(x._1) < 0).groupBy(x=> x._2).map(x=> (12, x._1 -1, x._2.map(y=> y._3).sum))
    val rsTypes = mapTopTypes ++ othersType
    logger.info("t7: "+(System.currentTimeMillis() -t7))
    val t8 = System.currentTimeMillis()
    // Chart 9: Churn Rate and Churn Percentage by Checklist Type by Region
    val top12trendTypes = getChecklistRegionType(month, ageChecklist, processTime).filter(x=> top12Types.indexOf(x._1) >= 0)
    val otherTrends     = getOtherTypeRegion(month,ageChecklist, processTime, top12Types.map(x=> "!(checklist.type:\""+x+"\")").mkString(" AND "))
    val rsTrendTypes    = calTrendTypeRegion(top12trendTypes ++ otherTrends, status, getOtherTypeRegion(month, ageChecklist, processTime, "").filter(x=> x._3 == status).map(x=> x._2 -> x._4))
    val trendType_X = (0 until rsTrendTypes.map(x=> x._1).distinct.length).map(x=> rsTrendTypes.map(x=> x._1).distinct(x) -> (x+1))
    val trendTypeBubble = rsTrendTypes.map(x=> (x._2, trendType_X.find(y=> y._1 == x._1).get._2, x._3, x._4, x._5))
    /* sort array type and region have null value*/
    var mapGroup9 = Array[(Int, Int)]()
    val mapTypes  = trendType_X.map(x=> x._2)
    val arrEmpty9 = Array((0.0, 0.0, 0L))
    for(i <- 0 until mapRegion1.size){
      for(j <- 0 until mapTypes.size){
        mapGroup9 :+= (mapRegion1(i) -> mapTypes(j))
      }
    }
    val rsTypeBubble = mapGroup9.map(x=> (x._1, x._2, if(trendTypeBubble.filter(y=> y._1 == x._1).filter(y=> x._2 == y._2).length ==0) arrEmpty9 else trendTypeBubble.filter(y=> y._1 == x._1)
      .filter(y=> x._2 == y._2).map(x=> (x._3, x._4, x._5))))
      .flatMap(x=> x._3.map(y=> (x._1, x._2) -> y))
      .map(x=> (x._1._1, x._1._2, x._2._1, x._2._2, x._2._3))
    logger.info("t8: "+(System.currentTimeMillis() -t8))
    logger.info("Time: "+(System.currentTimeMillis() -t0))
    logger.info("========END CHECKLIST SERVICE=========")
    ChecklistResponse(ctCheckList, trendChecklist, checklistRegion, (mapMonthRegion, checklistRegionMonth), rsTrendRegionAge, checklistRegionAge,
      (time_X, region_Y, dataChurn7), (types_X.toArray, types_Y, rsTypes), (trendType_X.toMap, rsTypeBubble))

  }
}