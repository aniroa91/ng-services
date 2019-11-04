package churn.utils

import com.sksamuel.elastic4s.http.search.SearchResponse

import scala.collection.immutable.Map

object CommonUtil {

  val PAGE_ID = Map(
    0 -> "Overview",
    1 -> "Age",
    2 -> "Package"
  )

  val STATUS_MAP = Map(
    "BINH THUONG" -> 0,
    "HUY DICH VU" -> 1,
    "NVLDTT" -> 2,
    "CTBDV" -> 3,
    "KH dang chuyen dia diem".toUpperCase() -> 4,
    "Cho thanh ly".toUpperCase() -> 5/*,
    "Ngung vi tranh chap cuoc".toUpperCase() -> 6*/
  )

  val REGION = Map(
    "VUNG 1" -> 1,
    "VUNG 2" -> 2,
    "VUNG 3" -> 3,
    "VUNG 4" -> 4,
    "VUNG 5" -> 5,
    "VUNG 6" -> 6,
    "VUNG 7" -> 7
  )

  def filterCommon(field: String): String = "!(region:0) AND !("+field+": \"FTTH - TV ONLY\") AND !("+field+": \"ADSL - TV ONLY\") AND !("+field+": \"ADSL - TV GOLD\") AND !("+field+": \"FTTH - TV GOLD\")"

  def getFullQueryHaveNested(queries: String, groupContract: String, maintainParam: String, cateParam: String, causeParam: String) = {
    var cate = ""
    var maintain = ""
    var cause = ""
    cate = if (cateParam == "") "" else
      s"""
                                 ,{
                                   "nested": {
                                      "path": "calllog",
                                      "query": {
                                        "query_string": {
                                            "query": "calllog.cate:${cateParam.replace("\"", "\\\"")}"
                                        }
                                      }
                                   }
                                 }
      """
    cause = if (causeParam == "") "" else
      s"""
                                ,{
                                   "nested": {
                                      "path": "checklist",
                                      "query": {
                                        "query_string": {
                                            "query": "checklist.lydo:${causeParam.replace("\"", "\\\"")}"
                                        }
                                      }
                                   }
                                 }
      """
    maintain = if (maintainParam == "") "" else
      s"""
                                 ,{
                                   "nested": {
                                      "path": "checklist",
                                      "query": {
                                        "query_string": {
                                            "query": "checklist.tobaotri:${maintainParam.replace("\"", "\\\"")}"
                                        }
                                      }
                                   }
                                 }
      """

    val full_queries = groupContract match {
      case "_Call Yes" =>
        s"""
          "query": {
                               "bool": {
                                    "filter": [
                                        {
                                           "query_string" : {
                                                "query": "${queries.replace("\"", "\\\"")}"
                                           }
                                        }
                                        ${cate}
                                        ${maintain}
                                        ${cause}
                                    ],
                                    "must": [
                                        {
                                          "nested": {
                                              "path": "calllog",
                                              "query": {
                                                   "bool": {
                                                      "must": [
                                                          {
                                                            "exists": {
                                                               "field": "calllog"
                                                            }
                                                          }
                                                      ]
                                                   }
                                              }
                                          }
                                        }
                                    ]
                               }
                       }
        """
      case "Call Yes_Checklist Yes" =>
        s"""
             "query": {
                     "bool": {
                          "filter": [
                              {
                                 "query_string" : {
                                      "query": "${queries.replace("\"", "\\\"")}"
                                 }
                              }
                              ${cate}
                              ${maintain}
                              ${cause}
                          ],
                          "must": [
                              {
                                "nested": {
                                    "path": "calllog",
                                    "query": {
                                         "bool": {
                                            "must": [
                                                {
                                                  "exists": {
                                                     "field": "calllog"
                                                  }
                                                }
                                            ]
                                         }
                                    }
                                }
                              },
                              {
                                "nested": {
                                    "path": "checklist",
                                    "query": {
                                         "bool": {
                                            "must": [
                                                {
                                                  "exists": {
                                                      "field": "checklist"
                                                  }
                                                }
                                            ]
                                         }
                                    }
                                }
                              }
                          ]
                     }
             }
        """
      case "Call Yes_Checklist No" =>
        s"""
             "query": {
                     "bool": {
                          "filter": [
                              {
                                 "query_string" : {
                                      "query": "${queries.replace("\"", "\\\"")}"
                                 }
                              }
                              ${cate}
                              ${maintain}
                              ${cause}
                          ],
                          "must": [
                              {
                                "nested": {
                                    "path": "calllog",
                                    "query": {
                                         "bool": {
                                            "must": [
                                                {
                                                  "exists": {
                                                     "field": "calllog"
                                                  }
                                                }
                                            ]
                                         }
                                    }
                                }
                              }
                          ],
                          "must_not": [
                             {
                               "nested": {
                                   "path": "checklist",
                                   "query": {
                                       "bool": {
                                           "must": [
                                              {
                                               "exists": {
                                                  "field": "checklist"
                                               }
                                              }
                                           ]
                                       }
                                   }
                               }
                             }
                          ]
                     }
             }
        """
      case "_Call No" =>
        s"""
          "query": {
                               "bool": {
                                    "filter": [
                                        {
                                           "query_string" : {
                                                "query": "${queries.replace("\"", "\\\"")}"
                                           }
                                        }
                                        ${cate}
                                        ${maintain}
                                        ${cause}
                                    ],
                                    "must_not": [
                                        {
                                          "nested": {
                                              "path": "calllog",
                                              "query": {
                                                   "bool": {
                                                      "must": [
                                                          {
                                                            "exists": {
                                                               "field": "calllog"
                                                            }
                                                          }
                                                      ]
                                                   }
                                              }
                                          }
                                        }
                                    ]
                               }
                       }
        """
      case "Call No_Checklist Yes" =>
        s"""
             "query": {
                     "bool": {
                          "filter": [
                              {
                                 "query_string" : {
                                      "query": "${queries.replace("\"", "\\\"")}"
                                 }
                              }
                             ${cate}
                             ${maintain}
                             ${cause}
                          ],
                          "must": [
                              {
                                "nested": {
                                    "path": "checklist",
                                    "query": {
                                         "bool": {
                                            "must": [
                                                {
                                                  "exists": {
                                                     "field": "checklist"
                                                  }
                                                }
                                            ]
                                         }
                                    }
                                }
                              }
                          ],
                          "must_not": [
                             {
                               "nested": {
                                   "path": "calllog",
                                   "query": {
                                       "bool": {
                                           "must": [
                                              {
                                               "exists": {
                                                  "field": "calllog"
                                               }
                                              }
                                           ]
                                       }
                                   }
                               }
                             }
                          ]
                     }
             }
        """
      case "Call No_Checklist No" =>
        s"""
             "query": {
                     "bool": {
                          "filter": [
                              {
                                 "query_string" : {
                                      "query": "${queries.replace("\"", "\\\"")}"
                                 }
                              }
                              ${cate}
                              ${maintain}
                              ${cause}
                          ],
                          "must_not": [
                              {
                                "nested": {
                                    "path": "calllog",
                                    "query": {
                                         "bool": {
                                            "must": [
                                                {
                                                  "exists": {
                                                     "field": "calllog"
                                                  }
                                                }
                                            ]
                                         }
                                    }
                                }
                              },
                              {
                                "nested": {
                                    "path": "checklist",
                                    "query": {
                                         "bool": {
                                            "must": [
                                                {
                                                  "exists": {
                                                      "field": "checklist"
                                                  }
                                                }
                                            ]
                                         }
                                    }
                                }
                              }
                          ]
                     }
             }
        """
      case _ =>
        s"""
          "query": {
                               "bool": {
                                    "filter": [
                                        {
                                           "query_string" : {
                                                "query": "${queries.replace("\"", "\\\"")}"
                                           }
                                        }
                                        ${cate}
                                        ${maintain}
                                        ${cause}
                                    ]
                               }
                       }
        """
    }
    full_queries
  }

  def getChurnRateAndPercentage(response: SearchResponse, term1: String, term2: String, term3: String): Array[(String, String, String, Int, Int, Int)] = {
    if (response.aggregations != null) {
      response.aggregations
        .getOrElse(term1, Map[String, AnyRef]()).asInstanceOf[Map[String, AnyRef]]
        .getOrElse("buckets", List).asInstanceOf[List[AnyRef]]
        .map(x => x.asInstanceOf[Map[String, AnyRef]])
        .flatMap(x => {
          val term1Key = x.getOrElse("key", "key").toString()
          val term1Count = x.getOrElse("doc_count", "0").toString().toInt
          x.getOrElse(term2, Map[String, AnyRef]()).asInstanceOf[Map[String, AnyRef]]
            .getOrElse("buckets", List).asInstanceOf[List[AnyRef]]
            .map(y => y.asInstanceOf[Map[String, AnyRef]])
            .flatMap(y => {
              val term2Key = y.getOrElse("key", "key").toString()
              val term2Count = y.getOrElse("doc_count", "0").toString().toInt
              y.getOrElse(term3, Map[String, AnyRef]()).asInstanceOf[Map[String, AnyRef]]
                .getOrElse("buckets", List).asInstanceOf[List[AnyRef]]
                .map(z => z.asInstanceOf[Map[String, AnyRef]])
                .map(z => {
                  val term3Key = z.getOrElse("key", "key").toString()
                  val term3Count = z.getOrElse("doc_count", "0").toString().toInt
                  (term1Key, term2Key, term3Key, term1Count, term2Count, term3Count)
                })
            })
        }).toArray
    } else {
      Array[(String, String, String, Int, Int, Int)]()
    }
  }
}