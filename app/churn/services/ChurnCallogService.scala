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
import churn.utils.{AgeGroupUtil, CommonUtil}
import org.elasticsearch.search.sort.SortOrder
import play.api.mvc.{AnyContent, Request}
import churn.models.CallogResponse
import play.api.Logger

import scalaj.http.Http
import services.Configure
import services.domain.CommonService

object  ChurnCallogService{

  val client = Configure.client

  val logger = Logger(this.getClass())

  def getNumberOfContractCallIn(region:String, age: String, category: String) ={
    val queries = "region:"+region+" AND lifeGroup:"+age+" AND "+CommonUtil.filterCommon("tenGoi")
    val queryNested = parseCategories(category)
    val queryAggsNested = parseOtherCatesQuery(category)
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
                              "path": "calllog",
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
                  },
                  "aggs": {
                      "calllog": {
                          "nested": {
                             "path": "calllog"
                          },
                          "aggs": {
                              "countCall": {
                                 "filter" : {
                                      "bool": {
                                        $queryAggsNested
                                     }
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
    val body = Http("http://172.27.11.151:9200/churn-calllog-*/docs/_search")
      .postData(query)
      .header("content-type", "application/JSON")
      .asString.body
    val json = Json.parse(body)
    val array = json.\("aggregations").\("month").\("buckets").get.asInstanceOf[JsArray].value.toArray
    array.map(x => (x.\("key").get.asInstanceOf[JsString].toString().replace("\"",""), x.\("doc_count").get.asInstanceOf[JsNumber].toString().toLong,
      if(category == "*") x.\("calllog").\("doc_count").get.asInstanceOf[JsNumber].toString().toLong else x.\("calllog").\("countCall").\("doc_count").get.asInstanceOf[JsNumber].toString().toLong))
  }

  def getNumberOfContractCallIn1(region:String, age: String, category: String) ={
    val queries = "region:"+region+" AND lifeGroup:"+age+" AND "+CommonUtil.filterCommon("tenGoi")
    val queryNested = if(category == "*") "calllog.cate:*" else "calllog.cate:\""+category+"\""
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
                              "path": "calllog",
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
    val body = Http("http://172.27.11.151:9200/churn-calllog-*/docs/_search")
      .postData(query)
      .header("content-type", "application/JSON")
      .asString.body
    val json = Json.parse(body)
    val array = json.\("aggregations").\("month").\("buckets").get.asInstanceOf[JsArray].value.toArray
    array.map(x => (x.\("key").get.asInstanceOf[JsString].toString().replace("\"",""), x.\("doc_count").get.asInstanceOf[JsNumber].toString().toLong))
  }

 /* def getNumberOfContractCallInHaveFilters(region:String, age: String, category: String) ={
    val rs = client.execute(
      search("churn-calllog-*" / "docs")
        query {
        boolQuery().not(
          termQuery("region", "0"),
          termQuery("tenGoi","FTTH - TV ONLY"),
          termQuery("tenGoi","ADSL - TV ONLY"),
          termQuery("tenGoi","ADSL - TV GOLD"),
          termQuery("tenGoi","FTTH - TV GOLD")
        ).must (
          termQuery("lifeGroup",age),
          nestedQuery("calllog").query(
            boolQuery.must(
              matchQuery("calllog.cate", category)
            )
          )
        )
      }
        aggregations (
        termsAggregation("month")
          .field("month") size 24
        ) size 0
        sortBy( fieldSort("month") order SortOrder.DESC)
    ).await
    CommonService.getAggregationsSiglog(rs.aggregations.get("month")).filter(x=> x._1 != CommonService.getCurrentMonth()).sortWith((x, y) => x._1 > y._1).slice(0,12).sorted

  }*/

  def getNumberOfContractAll(queryString: String, tb: String) ={
    val queries = queryString + " AND "+ CommonUtil.filterCommon(if(tb == "churn-contract-info-*") "package_name" else "tenGoi")
    val request = search(s"$tb" / "docs") query(queries) aggregations (
      termsAggregation("month")
        .field("month") size 24
      )  size 0 sortBy( fieldSort("month") order SortOrder.DESC)
    val rs = client.execute(request).await
    CommonService.getAggregationsSiglog(rs.aggregations.get("month")).filter(x=> x._1 != CommonService.getCurrentMonth()).sortWith((x, y) => x._1 > y._1).slice(0,12).sorted

  }

  def getCateOthers(queryString: String, cateOthers: Array[String]) = {
    val queries = queryString +" AND !(region:0) AND "+CommonUtil.filterCommon("tenGoi")
    val queryNested = cateOthers.mkString(" AND ")
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
                              "path": "calllog",
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
               "status": {
                  "terms": {
                    "field": "status",
                    "size": 1000
                }
              }
            },
            "size": 0
        }
        """
    //println(query)
    val body = Http("http://172.27.11.151:9200/churn-calllog-*/docs/_search")
      .postData(query)
      .header("content-type", "application/JSON")
      .asString.body
    val json = Json.parse(body)
    val array = json.\("aggregations").\("status").\("buckets").get.asInstanceOf[JsArray].value.toArray
    array.map(x => (x.\("key").get.asInstanceOf[JsNumber].toString(), x.\("doc_count").get.asInstanceOf[JsNumber].toString().toLong))
  }

  def getChurnByCates(queryString: String) ={
    val queries = queryString +" AND "+CommonUtil.filterCommon("tenGoi")
    val request = s"""
        {
                    "query": {
                      "query_string": {
                        "query": "${queries.replace("\"", "\\\"")}"
                      }
                    },
                    "size": 0,
                    "aggs": {
                       "status": {
                         "terms": {
                           "field": "status"
                         },
                         "aggs": {
                           "calllog": {
                             "nested": {
                               "path": "calllog"
                             },
                             "aggs": {
                               "cate": {
                                 "terms": {
                                   "field": "calllog.cate",
                                   "size": 1000
                                 },
                                 "aggs": {
                                   "contract": {
                                     "reverse_nested": {}
                                   }
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
    val body = Http("http://172.27.11.151:9200/churn-calllog-*/docs/_search")
      .postData(request)
      .header("content-type", "application/JSON")
      .asString.body
    val json = Json.parse(body)
    val array = json.\("aggregations").\("status").\("buckets").get.asInstanceOf[JsArray].value.toArray
    val rss = array.map(x => (x.\("key").get.asInstanceOf[JsNumber], x.\("doc_count").get.asInstanceOf[JsNumber], x.\("calllog").get.\("cate").\("buckets").get.asInstanceOf[JsArray].value.toArray
      .map(y=> (y.\("key").get.asInstanceOf[JsString], y.\("contract").get.\("doc_count").get.asInstanceOf[JsNumber]))))
    rss.flatMap(x=> x._3.map(y=> (x._1.value, x._2.value) -> y)).map(x=> (x._2._1.toString().replace("\"",""), x._1._1.toString(), x._2._2.toString().toLong, x._1._2.toLong))
  }

  def getMultiAggregationsNested(aggr: Option[AnyRef], secondField: String, thirdField: String) = {
    aggr.getOrElse("buckets", Map[String, AnyRef]()).asInstanceOf[Map[String, AnyRef]]
      .getOrElse("buckets", List).asInstanceOf[List[AnyRef]]
      .map(x => x.asInstanceOf[Map[String, AnyRef]])
      .map(x => {
        val key1 = x.getOrElse("key", "0L").toString
        val count1 = x.getOrElse("doc_count", 0L).toString.toLong
        val map3  = x.getOrElse(s"$secondField",Map[String,AnyRef]()).asInstanceOf[Map[String,AnyRef]]
          .getOrElse("cate",Map[String,AnyRef]()).asInstanceOf[Map[String,AnyRef]]
          .getOrElse("buckets", List).asInstanceOf[List[AnyRef]]
          .map(x => x.asInstanceOf[Map[String, AnyRef]])
          .map(x=> {
            val key3   = x.getOrElse("key", "0L").toString
            val count3 = x.get("contract").get.asInstanceOf[Map[String, Double]].get("doc_count").get.toLong
            (key3, count3)
          })
        (key1, count1, map3)
      }).toArray
  }

  def getTrendCallIn(region: String, age: String, cate: String) ={
    val queries = "region:"+region+" AND lifeGroup:"+age+" AND "+CommonUtil.filterCommon("tenGoi")
    val queryNested = parseCategories(cate)
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
                              "path": "calllog",
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
    val body = Http("http://172.27.11.151:9200/churn-calllog-*/docs/_search")
      .postData(query)
      .header("content-type", "application/JSON")
      .asString.body
    val json = Json.parse(body)
    val array = json.\("aggregations").\("month").\("buckets").get.asInstanceOf[JsArray].value.toArray
      .map(x => (x.\("key").get.asInstanceOf[JsString].toString().replace("\"",""), x.\("doc_count").get.asInstanceOf[JsNumber].toString().toLong,
      x.\("status").\("buckets").get.asInstanceOf[JsArray].value.toArray.map(y=> (y.\("key").get.asInstanceOf[JsNumber].toString(), y.\("doc_count").get.asInstanceOf[JsNumber].toString().toLong))))

    array.flatMap(x=> x._3.map(y=> (x._1, x._2) -> y)).map(x=> (x._1._1, x._2._1, x._2._2, x._1._2))
  }

  /*def getTrendCallIn(queryString: String) ={
    val request = search(s"churn-calllog-*" / "docs") query(queryString + " AND !(region:0) AND !(tenGoi: \"FTTH - TV ONLY\") AND !(tenGoi: \"ADSL - TV ONLY\") AND !(tenGoi: \"ADSL - TV GOLD\") AND !(tenGoi: \"FTTH - TV GOLD\") ") aggregations (
      termsAggregation("month")
        .field("month")
        .subaggs(
          termsAggregation("status")
            .field("status") size 1000
        ) size 24
      ) sortBy( fieldSort("month") order SortOrder.DESC)
    val rs = client.execute(request).await
    ChurnRegionService.getSecondAggregations(rs.aggregations.get("month"), "status")
      .flatMap(x=> x._3.map(y=> (x._1,x._2) -> y))
      .map(x=> (x._1._1, x._2._1, x._2._2, x._1._2))
  }*/

  def getChurnCallInbyRegionAll(queryString: String) ={
    val request = search("churn-contract-info-*" / "docs") query(queryString + " AND "+CommonUtil.filterCommon("package_name")) aggregations (
      termsAggregation("month")
        .field("month")
        .subaggs(
          termsAggregation("region")
            .field("region") size 1000
        ) size 15
      ) size 0 sortBy( fieldSort("month") order SortOrder.DESC)
    val rs = client.execute(request).await
    CommonService.getSecondAggregations(rs.aggregations.get("month"), "region")
      .flatMap(x=> x._2.map(y=> x._1 -> y))
      .map(x=> (x._1, x._2._1, x._2._2)).filter(x=> x._1 != CommonService.getCurrentMonth()).sortWith((x, y) => x._1 > y._1)
  }

  def getChurnCallInbyRegionCallIn(age: String, cate: String) ={
    val queries = "lifeGroup:"+age+" AND "+CommonUtil.filterCommon("tenGoi")
    val queryNested = parseCategories(cate)
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
                              "path": "calllog",
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
    val body = Http("http://172.27.11.151:9200/churn-calllog-*/docs/_search")
      .postData(query)
      .header("content-type", "application/JSON")
      .asString.body
    val json = Json.parse(body)
    val array = json.\("aggregations").\("month").\("buckets").get.asInstanceOf[JsArray].value.toArray
      .map(x => (x.\("key").get.asInstanceOf[JsString].toString().replace("\"",""),
        x.\("region").\("buckets").get.asInstanceOf[JsArray].value.toArray.map(y=> (y.\("key").get.asInstanceOf[JsNumber].toString(), y.\("doc_count").get.asInstanceOf[JsNumber].toString().toLong))))

    array.flatMap(x=> x._2.map(y=> x._1 -> y)).map(x=> (x._1, x._2._1, x._2._2)).filter(x=> x._1 != CommonService.getCurrentMonth()).sortWith((x, y) => x._1 > y._1)
  }

 /* def getChurnCallInbyRegionCallIn(indexs: String, queryString: String, region: String) ={
    val request = search(s"$indexs-*" / "docs") query(queryString + " AND !(TenGoi: \"FTTH - TV ONLY\") AND !(TenGoi: \"ADSL - TV ONLY\") AND !(TenGoi: \"ADSL - TV GOLD\") AND !(TenGoi: \"FTTH - TV GOLD\") ") aggregations (
      termsAggregation("month")
        .field("month")
        .subaggs(
          termsAggregation(region)
            .field(region) size 1000
        ) size 24
      ) sortBy( fieldSort("month") order SortOrder.DESC)
    val rs = client.execute(request).await
    CommonService.getSecondAggregations(rs.aggregations.get("month"), region)
      .flatMap(x=> x._2.map(y=> x._1 -> y))
      .map(x=> (x._1, x._2._1, x._2._2)).filter(x=> x._1 != CommonService.getCurrentMonth()).sortWith((x, y) => x._1 > y._1)
  }*/


  def getChurnByRegionAgeAll(month: String) ={
    val request = search(s"churn-contract-info-${month}" / "docs") query(CommonUtil.filterCommon("package_name")) aggregations (
      rangeAggregation("age")
        .field("age")
        .range("6", 0, 6.0001)
        .range("12", 6.001, 12.001)
        .range("18", 12.001, 18.001)
        .range("24", 18.001, 24.001)
        .range("30", 24.001, 30.001)
        .range("36", 30.001, 36.001)
        .range("42", 36.001, 42.001)
        .range("48", 42.001, 48.001)
        .range("54", 48.001, 54.001)
        .range("60", 54.001, 60.001)
        .range("66", 60.001, Double.MaxValue)
        .subaggs(
          termsAggregation("region")
            .field("region") size 1000
        )
      )
    val rs = client.execute(request).await
    CommonService.getSecondAggregations(rs.aggregations.get("age"), "region")
      .flatMap(x=> x._2.map(y=> x._1 -> y))
      .map(x=> (x._1, x._2._1, x._2._2))
  }

  def getChurnByRegionAgeCallIn(month: String, cate: String) ={
    val queries = CommonUtil.filterCommon("tenGoi")
    val queryNested = parseCategories(cate)
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
                              "path": "calllog",
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
    val body = Http(s"http://172.27.11.151:9200/churn-calllog-${month}/docs/_search")
      .postData(query)
      .header("content-type", "application/JSON")
      .asString.body
    val json = Json.parse(body)
    val array = json.\("aggregations").\("age").\("buckets").get.asInstanceOf[JsArray].value.toArray
      .map(x => (x.\("key").get.asInstanceOf[JsString].toString().replace("\"",""),
        x.\("region").\("buckets").get.asInstanceOf[JsArray].value.toArray.map(y=> (y.\("key").get.asInstanceOf[JsNumber].toString(), y.\("doc_count").get.asInstanceOf[JsNumber].toString().toLong))))
    array.flatMap(x=> x._2.map(y=> x._1 -> y)).map(x=> (x._1, x._2._1, x._2._2))

  }

  /*def getChurnByRegionAgeCallIn(indexs:String, queryString: String, age: String, region: String) ={
    val request = search(s"$indexs" / "docs") query(queryString+" AND !(TenGoi: \"FTTH - TV ONLY\") AND !(TenGoi: \"ADSL - TV ONLY\") AND !(TenGoi: \"ADSL - TV GOLD\") AND !(TenGoi: \"FTTH - TV GOLD\") ") aggregations (
      rangeAggregation("age")
        .field(s"$age")
        .range("6", 0, 6.0001)
        .range("12", 6.001, 12.001)
        .range("18", 12.001, 18.001)
        .range("24", 18.001, 24.001)
        .range("30", 24.001, 30.001)
        .range("36", 30.001, 36.001)
        .range("42", 36.001, 42.001)
        .range("48", 42.001, 48.001)
        .range("54", 48.001, 54.001)
        .range("60", 54.001, 60.001)
        .range("66", 60.001, Double.MaxValue)
        .subaggs(
          termsAggregation("region")
            .field(s"$region") size 1000
        )
      )
    val rs = client.execute(request).await
    CommonService.getSecondAggregations(rs.aggregations.get("age"), "region")
      .flatMap(x=> x._2.map(y=> x._1 -> y))
      .map(x=> (x._1, x._2._1, x._2._2))
  }*/

  def getCallInRegionMonth(age: String, cate: String) = {
    val queries = "lifeGroup:"+age+" AND "+CommonUtil.filterCommon("tenGoi")
    val queryNested = parseCategories(cate)
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
                              "path": "calllog",
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
    val body = Http("http://172.27.11.151:9200/churn-calllog-*/docs/_search")
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

  /*def getCallInRegionMonth(queryString: String) = {
    val request = search(s"churn-calllog-*" / "docs") query(queryString+" AND !(region:0) AND !(TenGoi: \"FTTH - TV ONLY\") AND !(TenGoi: \"ADSL - TV ONLY\") AND !(TenGoi: \"ADSL - TV GOLD\") AND !(TenGoi: \"FTTH - TV GOLD\") ") aggregations (
      termsAggregation("month")
        .field("month")
        .subaggs(
          termsAggregation("status")
            .field("status")
            .subaggs(
              termsAggregation("region")
                .field("region") size 1000
            ) size 1000
        ) size 24
      )
    val rs = client.execute(request).await
    ChurnRegionService.getChurnRateAndPercentage(rs,"month","status" , "region").map(x=> (x._1, x._2, x._3, x._5, x._6))
  }*/

  def getCallInRegionAge(month: String, cate: String) = {
    val queries = CommonUtil.filterCommon("tenGoi")
    val queryNested = parseCategories(cate)
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
                              "path": "calllog",
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
    val body = Http(s"http://172.27.11.151:9200/churn-calllog-${month}/docs/_search")
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

  /*def getCallInRegionAge(month: String, queryString: String) = {
    val request = search(s"churn-calllog-${month}" / "docs") query(queryString+" AND !(region:0) AND !(TenGoi: \"FTTH - TV ONLY\") AND !(TenGoi: \"ADSL - TV ONLY\") AND !(TenGoi: \"ADSL - TV GOLD\") AND !(TenGoi: \"FTTH - TV GOLD\") ") aggregations (
      termsAggregation("region")
        .field("region")
        .subaggs(
          termsAggregation("status")
            .field("status")
            .subaggs(
              termsAggregation("lifeGroup")
                .field("lifeGroup") size 100
            ) size 1000
        )
      )
    val rs = client.execute(request).await
    ChurnAgeService.getChurnRateAndPercentage(rs,"region","status" ,"lifeGroup").map(x=> (x._1, x._2, x._3, x._5, x._6))
  }*/


  def calChurnRateAndPercentageCategory(array: Array[(String, String, Long, Long)], status: Int, queryStr: String) ={
    val sumHuydvByCate = array.groupBy(x=> x._1).map(x=> x._1 -> x._2.map(x=> x._3).sum)
    val rsHuydv   = array.filter(x=> x._2.toInt == status).map(x=> (x._1, CommonService.format2Decimal(x._3 * 100.00 / sumHuydvByCate.get(x._1).get), CommonService.format2Decimal(x._3 * 100.00 / x._4)))
    val top9Cates = rsHuydv.map(x=> (x._1, x._2, x._3, 2*x._2*x._3*1.00/(x._2+x._3))).sortWith((x, y) => x._4 > y._4).slice(0,9).map(x=> (x._1,x._2,x._3))

    val arrayOthers = getCateOthers(queryStr, top9Cates.map(x=> "!(calllog.cate:\""+x._1+"\")"))
    val sumOthersRate = arrayOthers.map(x=> x._2).sum
    val sumOtherPercent = if(array.filter(x=> x._2.toInt == status).length >0) array.filter(x=> x._2.toInt == status)(0)._4 else 0
    val others = if(sumOtherPercent >0) arrayOthers.filter(x=> x._1.toInt == status).map(x=> ("Others", CommonService.format2Decimal(x._2 * 100.00 / sumOthersRate), CommonService.format2Decimal(x._2 * 100.00 / sumOtherPercent)))
                 else Array[(String, Double, Double)]()
    top9Cates ++ others
  }

  def calTrendCallinRateAndPercentage(callInArray: Array[(String, String, Long, Long)], allChurn: Array[(String, Long)], status: Int) ={
    val rs  = callInArray.filter(x=> x._2.toInt == status).filter(x=> allChurn.map(y=> y._1).indexOf(x._1) >=0)
    rs.map(x=> (x._1, x._3, x._4, allChurn.toMap.get(x._1).get)).map(x=>(x._1, CommonService.format2Decimal(x._2 * 100.00 / x._3), CommonService.format2Decimal(x._2 * 100.00 / x._4)))
      .sortWith((x, y) => x._1 > y._1).slice(0, 12).sorted
  }

  def calChurnCallinRateAndPercentagebyRegion(callInRegion: Array[(String, String, Long)], allRegion: Array[(String, String, Long)]) = {
    val catesMonth  = allRegion.map(x=> x._1).distinct.sortWith((x, y) => x > y).slice(0,12)
    val rs = callInRegion.filter(x=> catesMonth.indexOf(x._1) >= 0).map(x=> (x._1, x._2, x._3, allRegion.filter(y=> y._1 == x._1).filter(y=> y._2 == x._2).map(y=> y._3).sum))
        .map(x=> (x._1, x._2, CommonService.format2Decimal(x._3 * 100.00 / x._4), x._3)).sorted

    (rs.map(x=>x._1).distinct.sorted, rs)
  }

  def calCallInRateAndPercentageRegionAge(array: Array[(String, String, String, Int, Int)], status: Int) = {
    val rs = array.filter(x => x._2.toInt == status)
    val sumByRegionAge       = rs.map(x=> (x._1, x._3, x._5, CommonService.format2Decimal(x._5 * 100.0 / x._4)))
    val sumByRegionAgeStatus = array.groupBy(x=> x._1-> x._3).map(x=> (x._1._1,x._1._2, x._2.map(y=> y._5).sum))
    sumByRegionAge.map(x=> (x._1.toInt, if(x._2 == "6-12") "06-12" else x._2, CommonService.format2Decimal(x._3 * 100.0 / sumByRegionAgeStatus.filter(y=> y._1 == x._1).filter(y=> y._2 == x._2)
      .map(y=> y._3).sum), x._4, x._3))
  }

  // parse query categories
  def parseCategories(category: String) = {
    val queries = category match {
      case category if(category == "*")            => "calllog.cate:*"
      case category if(category.indexOf(",") >= 0) => category.split(",").filter(!_.contains("Others")).map(x=> "!(calllog.cate:\""+x+"\")").mkString(" AND ")
      case category if(category.indexOf(",") < 0)  => "calllog.cate:\""+category+"\""
    }
    queries
  }

  // parseOtherQueries
  def parseOtherCatesQuery(category: String) = {
    var queries = ""
    if(category.indexOf("Others") < 0){
      queries = "\"must\": [{ \"term\": {\"calllog.cate\": \""+category+"\"} }]"
    }
    else{
      val arrCates = category.split(",").filter(!_.contains("Others"))
      queries = "\"must_not\":[" + arrCates.map(x=> "{\"term\":{\"calllog.cate\":\""+x+"\"}}").mkString(",") + "]"
    }
    queries
  }

  def calChurnRateAndPercentageForRegionMonth(array: Array[(String, String, String, Int, Int)], status: Int) = {
    val rs = array.filter(x => x._2.toInt == status)
    //rs.foreach(println)
    val sumByRegionMonth       = rs.map(x=> (x._1, x._3, x._5, CommonService.format2Decimal(x._5 * 100.0 / x._4)))
    val sumByRegionMonthStatus = array.groupBy(x=> x._1-> x._3).map(x=> (x._1._1,x._1._2, x._2.map(y=> y._5).sum))
    sumByRegionMonth.map(x=> (x._2.toInt, x._1, CommonService.format2Decimal(x._3 * 100.0 / sumByRegionMonthStatus.filter(y=> y._1 == x._1).filter(y=> y._2 == x._2)
      .map(y=> y._3).sum), x._4, x._3))
  }

  def getInternet(request: Request[AnyContent]) = {
    logger.info("========START CALLOG SERVICE=========")
    var status = 1 // Huy dich vu default
    var ageCallIn = "*"
    var ageAll = "*"
    var region = "*"
    var cate = "*"
    var month = CommonService.getPrevMonth()
    if(request != null) {
      status = request.body.asFormUrlEncoded.get("status").head.toInt
      if(request.body.asFormUrlEncoded.get("age").head != "" && request.body.asFormUrlEncoded.get("age").head == "12"){
        ageCallIn =  "\"6-12\""
      }
      else if(request.body.asFormUrlEncoded.get("age").head != "" && request.body.asFormUrlEncoded.get("age").head != "12"){
        ageCallIn = "\"" + AgeGroupUtil.getAgeById(request.body.asFormUrlEncoded.get("age").head.toInt) + "\""
      }
      ageAll = if(request.body.asFormUrlEncoded.get("age").head != "")  AgeGroupUtil.getCalAgeByName(AgeGroupUtil.getAgeById(request.body.asFormUrlEncoded.get("age").head.toInt)) else "*"
      region = if(request.body.asFormUrlEncoded.get("region").head != "") request.body.asFormUrlEncoded.get("region").head else "*"
      month = request.body.asFormUrlEncoded.get("month").head
      request.body.asFormUrlEncoded.get("cateCurr").head match {
        case "" => {
          cate = "*"
        }
        case "Others" =>  {
          cate = request.body.asFormUrlEncoded.get("topCate").head
        }
        case _ => {
          cate = request.body.asFormUrlEncoded.get("cateCurr").head
        }
      }
    }
    //println(cate)
    val t0 = System.currentTimeMillis()
    // Number of Contracts Who Call in and Number of Inbound Calls
    val contractAll = getNumberOfContractAll(s"region:$region AND $ageAll", "churn-contract-info-*").slice(0,12).sorted
    val whoCallIn   = getNumberOfContractCallIn(region, ageCallIn, cate).filter(x=> contractAll.map(y=> y._1).indexOf(x._1) >=0)
      .map(x=> (x._1, x._2, CommonService.format2Decimal(x._2 * 100.00 / contractAll.toMap.get(x._1).get), x._3)).sorted
    logger.info("t0: "+(System.currentTimeMillis() - t0))
    val t1 = System.currentTimeMillis()
    // Churn Rate and Churn Percentage by Call Category
    val churnCates = calChurnRateAndPercentageCategory(getChurnByCates(s"month:$month AND region:$region AND lifeGroup:$ageCallIn"), status, s"month:$month AND region:$region AND lifeGroup:$ageCallIn")
    logger.info("t1: "+(System.currentTimeMillis() - t1))
    val t2 = System.currentTimeMillis()
    // Number of Contracts Who Call in by Region by Contract Age
    val numOfCallIn   = getChurnByRegionAgeCallIn(month, cate)
    val numOfContract = getChurnByRegionAgeAll(month)
    val callInRegionAge = numOfCallIn.map(x=> (x._1, x._2, CommonService.format2Decimal(x._3 * 100.00 / numOfContract.filter(y=> y._1 == x._1).filter(y=> y._2 == x._2).map(y=> y._3).sum), x._3))
    logger.info("t2: "+(System.currentTimeMillis() - t2))
    val t3 = System.currentTimeMillis()
    // For Contracts Who Call in: Trend in Churn Rate and Churn Percentage
    val allChurn_count  = getNumberOfContractAll(s"status:$status AND $ageAll AND region:$region",if(cate == "*") "churn-contract-info-*" else "churn-calllog-*")
    val trendCallIn = calTrendCallinRateAndPercentage(getTrendCallIn(region, ageCallIn, cate), allChurn_count , status)
    logger.info("t3: "+(System.currentTimeMillis() - t3))
    val t4 = System.currentTimeMillis()
    //  Number of Contracts Who Call In by Region
    val ctAllRegion  = getChurnCallInbyRegionAll(s"$ageAll AND !(region:0)")
    val callInRegion = calChurnCallinRateAndPercentagebyRegion(getChurnCallInbyRegionCallIn(ageCallIn, cate), ctAllRegion)
    logger.info("t4: "+(System.currentTimeMillis() - t4))
    val t5 = System.currentTimeMillis()
    // region and month trends
    val trendRegionMonth = calChurnRateAndPercentageForRegionMonth(getCallInRegionMonth(ageCallIn, cate), status).filter(x=> x._2 != CommonService.getCurrentMonth()).sorted
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
    val callRegionMonth = arrGroup.map(x=> (x._1, x._2, if(rsRegionMonth.filter(y=> y._1 == x._1).filter(y=> y._2 == x._2).length ==0) arrEmpty else rsRegionMonth.filter(y=> y._1 == x._1).filter(y=> y._2 == x._2)
      .map(x=> (x._3, x._4, x._5)))).flatMap(x=> x._3.map(y=> (x._1, x._2) -> y))
      .map(x=> (x._1._1, x._1._2, x._2._1, x._2._2, x._2._3))
    logger.info("t5: "+(System.currentTimeMillis() - t5))
    val t6 = System.currentTimeMillis()
    // region and age trends
    val trendRegionAge = calCallInRateAndPercentageRegionAge(getCallInRegionAge(month, cate), status)
    val mapAge = AgeGroupUtil.AGE.map(y=> y._2).toArray
    val mapRegion1 = CommonUtil.REGION.map(x=> x._2).toArray
    var mapGroup1 = Array[(Int, Int)]()
    val arrEmpty1 = Array((0.0, 0.0, 0))
    for(i <- 0 until mapRegion1.size){
      for(j <- 0 until mapAge.size){
        mapGroup1 :+= (mapRegion1(i) -> mapAge(j))
      }
    }
    val callRegionAge = mapGroup1.map(x=> (x._1, x._2, if(trendRegionAge.filter(y=> y._1 == x._1).filter(y=> x._2 == AgeGroupUtil.getAgeIdByName(y._2)).length ==0) arrEmpty1 else trendRegionAge.filter(y=> y._1 == x._1)
      .filter(y=> x._2 == AgeGroupUtil.getAgeIdByName(y._2)).map(x=> (x._3, x._4, x._5))))
      .flatMap(x=> x._3.map(y=> (x._1, x._2) -> y))
      .map(x=> (x._1._1, x._1._2, x._2._1, x._2._2, x._2._3))
    logger.info("t6: "+(System.currentTimeMillis() - t6))
    logger.info("Time: "+(System.currentTimeMillis() - t0))
    logger.info("========END CALLOG SERVICE=========")
    CallogResponse(whoCallIn, churnCates, callInRegionAge, trendCallIn, callInRegion, (mapMonthRegion, callRegionMonth), callRegionAge)
  }
}