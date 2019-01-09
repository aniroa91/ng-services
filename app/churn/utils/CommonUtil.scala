package churn.utils

object CommonUtil {

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

}