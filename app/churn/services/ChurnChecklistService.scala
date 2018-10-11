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

import scalaj.http.Http
import services.Configure
import services.domain.CommonService

object  ChurnChecklistService{

  val client = Configure.client

  def getNumberOfContractChecklist(region:String, age: String, _type: String, time: String) ={
    val queries = "region:"+region+" AND lifeGroup:"+age+" AND !(region:0) AND !(tenGoi: \"FTTH - TV ONLY\") AND !(tenGoi: \"ADSL - TV ONLY\") AND !(tenGoi: \"ADSL - TV GOLD\") AND !(tenGoi: \"FTTH - TV GOLD\")"
    val queryNested_type = if(_type == "*") "*" else _type
    val queryNested_time = if(time == "*") "*" else "checklist.processTime:>="+time.split("-")(0)+" AND checklist.processTime:<"+time.split("-")(1)
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
      x.\("checklist").\("doc_count").get.asInstanceOf[JsNumber].toString().toLong))
  }

  def getChurnContractbyStatus(region: String, age: String, _type: String, time: String) ={
    val queries = "region:"+region+" AND lifeGroup:"+age+" AND !(region:0) AND !(tenGoi: \"FTTH - TV ONLY\") AND !(tenGoi: \"ADSL - TV ONLY\") AND !(tenGoi: \"ADSL - TV GOLD\") AND !(tenGoi: \"FTTH - TV GOLD\")"
    val queryNested_type = if(_type == "*") "*" else _type
    val queryNested_time = if(time == "*") "*" else "checklist.processTime:>="+time.split("-")(0)+" AND checklist.processTime:<"+time.split("-")(1)
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
    val queries = "lifeGroup:"+age+" AND !(region:0) AND !(tenGoi: \"FTTH - TV ONLY\") AND !(tenGoi: \"ADSL - TV ONLY\") AND !(tenGoi: \"ADSL - TV GOLD\") AND !(tenGoi: \"FTTH - TV GOLD\")"
    val queryNested_type = if(_type == "*") "*" else _type
    val queryNested_time = if(time == "*") "*" else "checklist.processTime:>="+time.split("-")(0)+" AND checklist.processTime:<"+time.split("-")(1)
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
    val queries = "lifeGroup:"+age+" AND !(region:0) AND !(tenGoi: \"FTTH - TV ONLY\") AND !(tenGoi: \"ADSL - TV ONLY\") AND !(tenGoi: \"ADSL - TV GOLD\") AND !(tenGoi: \"FTTH - TV GOLD\")"
    val queryNested_type = if(_type == "*") "*" else _type
    val queryNested_time = if(time == "*") "*" else "checklist.processTime:>="+time.split("-")(0)+" AND checklist.processTime:<"+time.split("-")(1)
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
    val queries = "!(region:0) AND !(tenGoi: \"FTTH - TV ONLY\") AND !(tenGoi: \"ADSL - TV ONLY\") AND !(tenGoi: \"ADSL - TV GOLD\") AND !(tenGoi: \"FTTH - TV GOLD\")"
    val queryNested_type = if(_type == "*") "*" else _type
    val queryNested_time = if(time == "*") "*" else "checklist.processTime:>="+time.split("-")(0)+" AND checklist.processTime:<"+time.split("-")(1)
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
    val queries = "!(region:0) AND !(tenGoi: \"FTTH - TV ONLY\") AND !(tenGoi: \"ADSL - TV ONLY\") AND !(tenGoi: \"ADSL - TV GOLD\") AND !(tenGoi: \"FTTH - TV GOLD\")"
    val queryNested = if(_type == "*") "*" else _type
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
    val queries = "lifeGroup:"+age+" AND !(region:0) AND !(tenGoi: \"FTTH - TV ONLY\") AND !(tenGoi: \"ADSL - TV ONLY\") AND !(tenGoi: \"ADSL - TV GOLD\") AND !(tenGoi: \"FTTH - TV GOLD\")"
    val queryNested = if(_type == "*") "*" else _type
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
      .map(y=> (y.\("key").get.asInstanceOf[JsString], y.\("doc_count").get.asInstanceOf[JsNumber]))))
    rss.flatMap(x=> x._2.map(y=> x._1.value -> y)).map(x=> (x._2._1.toString().replace("\"", "").toInt, x._1.toString().toLong, x._2._2.toString().toLong))

  }

  def getChurnByRegionType(month: String, age: String, time: String) ={
    val queries = "lifeGroup:"+age+" AND !(region:0) AND !(tenGoi: \"FTTH - TV ONLY\") AND !(tenGoi: \"ADSL - TV ONLY\") AND !(tenGoi: \"ADSL - TV GOLD\") AND !(tenGoi: \"FTTH - TV GOLD\")"
    val queryNested = if(time == "*") "*" else "checklist.processTime:\""+time+"\""
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
      .map(y=> (y.\("key").get.asInstanceOf[JsString], y.\("doc_count").get.asInstanceOf[JsNumber]))))
    rss.flatMap(x=> x._2.map(y=> x._1.value -> y)).map(x=> (x._2._1.toString().replace("\"", ""), x._1.toString().toLong, x._2._2.toString().toLong))

  }

  def getChecklistRegionType(month: String, age: String, time: String) = {
    val queries = "lifeGroup:"+age+" AND !(region:0) AND !(tenGoi: \"FTTH - TV ONLY\") AND !(tenGoi: \"ADSL - TV ONLY\") AND !(tenGoi: \"ADSL - TV GOLD\") AND !(tenGoi: \"FTTH - TV GOLD\")"
    val queryNested = if(time == "*") "*" else "checklist.processTime:\""+time+"\""
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
    val queries = "lifeGroup:"+age+" AND !(region:0) AND !(tenGoi: \"FTTH - TV ONLY\") AND !(tenGoi: \"ADSL - TV ONLY\") AND !(tenGoi: \"ADSL - TV GOLD\") AND !(tenGoi: \"FTTH - TV GOLD\")"
    val queryNested_type = if(_type == "") "checklist.type:*" else _type
    val queryNested_time = if(time == "*") "*" else "checklist.processTime:\""+time+"\""
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
      .map(x => (x.\("key").get.asInstanceOf[JsNumber].toString().toInt,x.\("doc_count").get.asInstanceOf[JsNumber].toString().toLong,
        x.\("status").\("buckets").get.asInstanceOf[JsArray].value.toArray.map(y=> (y.\("key").get.asInstanceOf[JsNumber].toString().toInt, y.\("doc_count").get.asInstanceOf[JsNumber].toString().toLong))))
    array.flatMap(x=> x._3.map(y=> (x._1,x._2) -> y)).map(x=> ("Others",x._1._1, x._2._1, x._2._2, x._1._2))
  }

  def calTrendTypeRegion(array: Array[(String, Int, Int, Long, Long)], status: Int, sumAll_churn: Array[(Int, Long)]) = {
    val rsHuyDv = array.filter(x=> x._3 == status).map(x=> (x._1, x._2, x._4, x._5))
    rsHuyDv.map(x=> (x._1, x._2, CommonService.format2Decimal(x._3 * 100.00 / x._4), CommonService.format2Decimal(x._3 * 100.00 / sumAll_churn.find(y=> y._1 == x._2).get._2), x._3))
  }

  def getInternet(request: Request[AnyContent]) = {
    var status = 1 // Huy dich vu default
    var ageChecklist = "*"
    var ageAll = "*"
    var region = "*"
    var _type = "*"
    var processTime = "*"
    var month = "2018-07"
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
      request.body.asFormUrlEncoded.get("typeCurr").head match {
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
      }
    }
    println(s"Status:$status \n Month:$month \n Age:$ageAll \n AgeChecklist: $ageChecklist \n Time: $processTime \n Region:$region \n Type: "+_type)
    println("===")
    // Chart 1: Number of Contracts That Have Checklist(s)
    val contractAll   = ChurnCallogService.getNumberOfContractAll(s"Region:$region AND $ageAll").slice(0,12).sorted
    val ctCheckList   = getNumberOfContractChecklist(region, ageChecklist, _type, processTime).filter(x=> contractAll.map(y=> y._1).indexOf(x._1) >=0)
      .map(x=> (x._1, x._2, CommonService.format2Decimal(x._2 * 100.00 / contractAll.toMap.get(x._1).get), x._3)).sorted

    // Chart 2: Number of Contracts That Have Checklist(s)
    val allChurn_count  = ChurnCallogService.getNumberOfContractAll(s"Status:$status AND $ageAll AND Region:$region")
    val trendChecklist  = ChurnCallogService.calTrendCallinRateAndPercentage(getChurnContractbyStatus(region, ageChecklist, _type, processTime), allChurn_count , status)

    // Chart 3: Number of Contracts That Have Checklist(s) by Region (%) 2-3 sai
    val ctAllRegion     = ChurnCallogService.getChurnCallInbyRegionAll(s"$ageAll AND !(Region:0)")
    val checklistRegion = ChurnCallogService.calChurnCallinRateAndPercentagebyRegion(getChurnCallInbyRegionChecklist(ageChecklist, _type, processTime), ctAllRegion)

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

    // Chart 5: Number of Contracts Who Have Checklist(s) by Region by Contract Age (%)
    val numOfChecklist     = getChurnByRegionAgeChecklist(month, _type, processTime)
    val numOfContract      = ChurnCallogService.getChurnByRegionAgeAll(month)
    val checklistRegionAge = numOfChecklist.map(x=> (x._1, x._2, CommonService.format2Decimal(x._3 * 100.00 / numOfContract.filter(y=> y._1 == x._1).filter(y=> y._2 == x._2).map(y=> y._3).sum), x._3))

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

    // Chart 7: Number of Checklist(s) by Checklist Processing Time by Region
    val numOfProcessTime   = getChurnByRegionProcessTime(month, ageChecklist, _type)
    val time_X     = numOfProcessTime.map(x=> x._1).distinct.sorted.map(x=> ProcessTimeUtil.getNameById(x))
    val region_Y   = numOfProcessTime.map(x=> x._2).distinct.sorted
    val dataChurn7 = numOfProcessTime.map(x=> (ProcessTimeUtil.getIndexById(x._1), x._2 -1, x._3))

    // Chart 8: Number of Checklist(s) by Checklist Types by Region
    val numOfTypeRegion = getChurnByRegionType(month, ageChecklist, processTime)
    val top12Types = numOfTypeRegion.groupBy(x=> x._1).map(x=> x._1 -> x._2.map(y=> y._3).sum).toArray.sortWith((x, y)=> x._2> y._2).slice(0,12).map(x=> x._1)

    val types_X = if(top12Types.length >=12) (0 until 12).map(x=> x -> top12Types(x)) ++ Array((12 -> "Others")) else (0 until top12Types.length).map(x=> x -> top12Types(x))
    val types_Y = numOfTypeRegion.map(x=> x._2).distinct.sorted

    val mapTopTypes = numOfTypeRegion.filter(x=> top12Types.indexOf(x._1) >= 0).map(x=> (types_X.find(y=> y._2 == x._1).get._1, x._2 -1, x._3))
    val othersType  = numOfTypeRegion.filter(x=> top12Types.indexOf(x._1) < 0).groupBy(x=> x._2).map(x=> (12, x._1 -1, x._2.map(y=> y._3).sum))
    val rsTypes = mapTopTypes ++ othersType

    // Chart 9: Churn Rate and Churn Percentage by Checklist Type by Region
    val top12trendTypes = getChecklistRegionType(month, ageChecklist, processTime).filter(x=> top12Types.indexOf(x._1) >= 0)
    val otherTrends     = getOtherTypeRegion(month,ageChecklist, processTime, top12Types.map(x=> "!(checklist.type:\""+x+"\")").mkString(" AND "))
    val rsTrendTypes    = calTrendTypeRegion(top12trendTypes ++ otherTrends, status, getOtherTypeRegion(month, ageChecklist, processTime,"").filter(x=> x._3 == status).map(x=> x._2 -> x._4))
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

    ChecklistResponse(ctCheckList, trendChecklist, checklistRegion, (mapMonthRegion, checklistRegionMonth), rsTrendRegionAge, checklistRegionAge,
      (time_X, region_Y, dataChurn7), (types_X.toArray, types_Y, rsTypes), (trendType_X.toMap, rsTypeBubble))

  }
}