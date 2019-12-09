package service

import com.sksamuel.elastic4s.http.ElasticDsl.{RichFuture, RichString, SearchHttpExecutable, SearchShow, percentilesAggregation, query, rangeAggregation, search, termsAgg, termsAggregation, _}
import churn.utils.{AgeGroupUtil, CommonUtil, ProvinceUtil}
import play.api.mvc.{AnyContent, Request}
import churn.models.CLRegionResponse
import org.elasticsearch.search.sort.SortOrder
import play.api.Logger
import scalaj.http.Http
import services.Configure
import services.domain.CommonService
import play.api.libs.json.JsArray
import play.api.libs.json.JsNumber
import play.api.libs.json.JsString
import play.api.libs.json.Json
import service.ChecklistService.{getFilterGroup, getQueryNested, getTopCause}
import service.OverviewAgeService.getTopAgeByMonth
import service.OverviewService.{checkLocation, getCommentChart}
import scala.concurrent.{Await, Future}
import scala.concurrent.ExecutionContext.Implicits.global
import scala.async.Async.{async, await}
import scala.concurrent.duration.Duration

object  ChecklistRegionService{

  val client = Configure.client

  val logger = Logger(this.getClass())

  def getChurnCallInbyRegionChecklist(queryStr: String, queryNested: String, region: String) ={
    val location = checkLocation(region)
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
                     "region": {
                        "terms": {
                           "field": "${location}",
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
      .map(x => (x.\("key").get.asInstanceOf[JsString].toString().replace("\"",""),
        x.\("region").\("buckets").get.asInstanceOf[JsArray].value.toArray.map(y=> (y.\("key").get.asInstanceOf[JsNumber].toString(), y.\("doc_count").get.asInstanceOf[JsNumber].toString().toLong))))

    array.flatMap(x=> x._2.map(y=> x._1 -> y)).map(x=> (x._1, x._2._1, x._2._2)).filter(x=> x._1 != CommonService.getCurrentMonth()).sortWith((x, y) => x._1 > y._1)
  }

  def getChecklistRegionMonth(queryStr: String, queryNested: String, field: String) = {
    val queries = queryStr + " AND " +CommonUtil.filterCommon("tenGoi")
    val location = checkLocation(field)
    val groupField = if(field == "tobaotri") """ "checklist": {
                                                                     "nested": {
                                                                       "path": "checklist"
                                                                     },
                                                                     "aggs": {
                                                                       "tobaotri": {
                                                                         "terms": {
                                                                           "field": "checklist.tobaotri",
                                                                           "size":1000
                                                                         }
                                                                       }
                                                                     }
                                                                   } """.stripMargin
                      else
                        s"""
                           "$location": {
                                                          "terms": {
                                                             "field": "$location",
                                                             "size": 1000
                                                          }
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
                           $groupField
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
    val array = if(field != "tobaotri") json.\("aggregations").\("month").\("buckets").get.asInstanceOf[JsArray].value.toArray
      .map(x => (x.\("key").get.asInstanceOf[JsString].toString().replace("\"",""), x.\("doc_count").get.asInstanceOf[JsNumber].toString().toLong,
        x.\("status").\("buckets").get.asInstanceOf[JsArray].value.toArray
          .map(y=> (y.\("key").get.asInstanceOf[JsNumber].toString(), y.\("doc_count").get.asInstanceOf[JsNumber].toString().toInt,
            y.\(s"$location").\("buckets").get.asInstanceOf[JsArray].value.toArray.map(k=> (k.\("key").get.asInstanceOf[JsNumber].toString(), k.\("doc_count").get.asInstanceOf[JsNumber].toString().toInt))))))
    else json.\("aggregations").\("month").\("buckets").get.asInstanceOf[JsArray].value.toArray
      .map(x => (x.\("key").get.asInstanceOf[JsString].toString().replace("\"",""), x.\("doc_count").get.asInstanceOf[JsNumber].toString().toLong,
        x.\("status").\("buckets").get.asInstanceOf[JsArray].value.toArray
          .map(y=> (y.\("key").get.asInstanceOf[JsNumber].toString(), y.\("doc_count").get.asInstanceOf[JsNumber].toString().toInt,
            y.\("checklist").\("tobaotri").\("buckets").get.asInstanceOf[JsArray].value.toArray.map(k=> (k.\("key").get.asInstanceOf[JsString].toString().replaceAll("\"",""), k.\("doc_count").get.asInstanceOf[JsNumber].toString().toInt))))))
    array.flatMap(x=> x._3.map(y=> (x._1, x._2) -> y)).map(x=> (x._1._1, x._1._2, x._2._1, x._2._2, x._2._3)).flatMap(x=> x._5.map(y=> (x._1, x._2, x._3, x._4) -> y))
      .map(x=> (x._1._1, x._1._3, x._2._1, x._1._4, x._2._2))

  }

  def getChurnChecklistbyRegionAll(queries: String, region: String) ={
    val location = checkLocation(region)
    val request = search("churn-contract-info-*" / "docs") query(queries + " AND "+CommonUtil.filterCommon("package_name")) aggregations (
      termsAggregation("month")
        .field("month")
        .subaggs(
          termsAggregation("region")
            .field(s"$location") size 1000
        ) size 15
      ) size 0 sortBy( fieldSort("month") order SortOrder.DESC)
    val rs = client.execute(request).await
    CommonService.getSecondAggregations(rs.aggregations.get("month"), "region")
      .flatMap(x=> x._2.map(y=> x._1 -> y))
      .map(x=> (x._1, x._2._1, x._2._2)).filter(x=> x._1 != CommonService.getCurrentMonth()).sortWith((x, y) => x._1 > y._1)
  }

  def calChurnCLRateAndPercentagebyRegion(clRegion: Array[(String, String, Long)], allRegion: Array[(String, String, Long)], province: String) = {
    val catesMonth  = allRegion.map(x=> x._1).distinct.sortWith((x, y) => x > y).slice(0,13)
    val res = clRegion.filter(x=> catesMonth.indexOf(x._1) >= 0).map(x=> (x._1, x._2, x._3, allRegion.filter(y=> y._1 == x._1).filter(y=> y._2 == x._2).map(y=> y._3).sum))
      .map(x=> (x._1, x._2, CommonService.format2Decimal(x._3 * 100.00 / x._4), x._3)).sorted
    val intRegex = """(\d+)""".r
    val rs = province match {
      case ""   => res.map(x=> (x._1, "Vung "+x._2, x._3, x._4))
      case intRegex(province) => res.map(x=> ( x._1, CommonService.toTitleCase(ProvinceUtil.getProvince(x._2.toInt)), x._3, x._4))
      case _       => res
    }
    (rs.map(x=>x._1).distinct.sorted, rs)
  }

  def calChurnRateAndPercentageForTBTMonth(array: Array[(String, String, String, Int, Int)], status: String) = {
    val rsArr = if(status == "" || status == "13") array.filter(x=> x._2.toInt == 1 || x._2.toInt == 3).groupBy(x=> x._1 -> x._3).map(x=> (x._1._1, 111, x._1._2, x._2.map(y=> y._4).sum, x._2.map(y=> y._5).sum)).toArray.sorted
    else array.filter(x => x._2.toInt == status.toInt).sorted
    val sumByRegionMonth       = rsArr.map(x=> (x._1, x._3, x._5, CommonService.format2Decimal(x._5 * 100.0 / x._4)))
    val sumByRegionMonthStatus = array.groupBy(x=> x._1-> x._3).map(x=> (x._1._1, x._1._2, x._2.map(y=> y._5).sum)).toArray
    val res = sumByRegionMonth.map(x=> (x._2, x._1, CommonService.format2Decimal(x._3 * 100.0 / sumByRegionMonthStatus.filter(y=> y._1 == x._1).filter(y=> y._2 == x._2)
      .map(y=> y._3).sum), x._4, x._3))
    res
  }

  def getTrendTbtMonth(month:String, queries:String, queryNested:String, status:String) = {
    val arrMonthTBT = getChecklistRegionMonth(queries, queryNested, "tobaotri")
    val trendMonthTBT = calChurnRateAndPercentageForTBTMonth(arrMonthTBT, status).filter(x=> x._2 != CommonService.getCurrentMonth()).sorted
    /* get top To bao tri */
    val topTBTByPert = getTopAgeByMonth(arrMonthTBT, status, month, "percent", 10)
    val sizeTopTBT = topTBTByPert.length +1
    val mapTBT = (1 until sizeTopTBT).map(x=> topTBTByPert(sizeTopTBT-x-1) -> x).toMap
    /* get top month */
    val topLast12monthTBT = trendMonthTBT.map(x=> x._2).distinct.sortWith((x, y) => x > y).filter(x=> x != CommonService.getCurrentMonth()).slice(0,15).sorted
    val topMonthTBT = if(topLast12monthTBT.length >= 15) 16 else topLast12monthTBT.length+1
    val mapMonthTBT   = (1 until topMonthTBT).map(x=> topLast12monthTBT(x-1) -> x).toMap
    val rsTBTMonth = trendMonthTBT.filter(x=> topLast12monthTBT.indexOf(x._2) >=0).filter(x=> topTBTByPert.indexOf(x._1) >=0)
      .map(x=> (mapTBT.get(x._1).get, mapMonthTBT.get(x._2).get, x._3, x._4, x._5))

    // sparkline table
    val topTBTByF1 = getTopAgeByMonth(arrMonthTBT, status, month, "f1", 10)
    val tbTBT = trendMonthTBT.filter(x=> topLast12monthTBT.indexOf(x._2) >=0).filter(x=> topTBTByF1.indexOf(x._1) >=0).map(x=> (x._1, x._2, x._3,
      CommonService.format2Decimal(2*x._3*x._4/(x._3+x._4)), x._5))
    Future{
      (mapTBT, mapMonthTBT,rsTBTMonth, topTBTByF1, tbTBT)
    }
  }

  def getTrendRegionMonth(month: String, queries: String, queryNested: String, region: String, status: String) = {
    val arrMonth = getChecklistRegionMonth(queries, queryNested, region)
    val trendRegionMonth = OverviewService.calChurnRateAndPercentageForRegionMonth(arrMonth, status, region).filter(x=> x._2 != CommonService.getCurrentMonth()).sorted
    /* get top location */
    val topLocationByPert = OverviewService.getTop10OltByMonth(arrMonth, status, month, region, "percent")
    val sizeTopLocation = topLocationByPert.length +1
    val mapRegionMonth = (1 until sizeTopLocation).map(x=> topLocationByPert(sizeTopLocation-x-1) -> x).toMap
    /* get top month */
    val topLast12month = trendRegionMonth.map(x=> x._2).distinct.sortWith((x, y) => x > y).filter(x=> x != CommonService.getCurrentMonth()).slice(0,15).sorted
    val topMonthRegion = if(topLast12month.length >= 15) 16 else topLast12month.length+1
    val mapMonth   = (1 until topMonthRegion).map(x=> topLast12month(x-1) -> x).toMap
    val rsRegionMonth = trendRegionMonth.filter(x=> topLast12month.indexOf(x._2) >=0).filter(x=> topLocationByPert.indexOf(x._1) >=0)
      .map(x=> (mapRegionMonth.get(x._1).get, mapMonth.get(x._2).get, x._3, x._4, x._5))
    Future{
      (mapRegionMonth, mapMonth,rsRegionMonth)
    }
  }

  def getInternet(user: String, request: Request[AnyContent]) = async{
    logger.info("========START CHECKLIST REGION TAB SERVICE=========")
    val t0 = System.currentTimeMillis()
    val groupCL = request.body.asFormUrlEncoded.get("groupCL").head
    val status = request.body.asFormUrlEncoded.get("status").head
    val age = request.body.asFormUrlEncoded.get("age").head
    val timeCL = request.body.asFormUrlEncoded.get("timeCL").head.toInt
    var position = request.body.asFormUrlEncoded.get("position").head
    var cause = request.body.asFormUrlEncoded.get("cause").head
    val region = request.body.asFormUrlEncoded.get("region").head
    val month = request.body.asFormUrlEncoded.get("month").head
    val packages = request.body.asFormUrlEncoded.get("package").head
    val queries = getFilterGroup(timeCL, age, region, packages, month, 1)
    //println(queries)
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

    // Number of Contracts That Have Checklist(s) by Region (%)
    val checklistRegion = Await.result(Future{ calChurnCLRateAndPercentagebyRegion(getChurnCallInbyRegionChecklist(queries, queryNested, region),
        getChurnChecklistbyRegionAll(queries, region), region) }, Duration.Inf)
    logger.info("t0: "+(System.currentTimeMillis() -t0))
    val t1 = System.currentTimeMillis()

    // Trend region and month
    val trendRegionMonth = Await.result(getTrendRegionMonth(month, queries, queryNested, region, status), Duration.Inf)
    logger.info("t1: "+(System.currentTimeMillis() - t1))

    val t2 = System.currentTimeMillis()
    // comments content
    val cmtChart = Await.result(Future{ getCommentChart(user, CommonUtil.PAGE_ID.get(1).get+"_tabRegion") }, Duration.Inf)
    logger.info("t2: "+(System.currentTimeMillis() - t2))

    val t3 = System.currentTimeMillis()
    // Trend To bao tri and month
    val trendTbtMonth = Await.result(getTrendTbtMonth(month, queries, queryNested, status), Duration.Inf)
    logger.info("t3: "+(System.currentTimeMillis() - t3))

    logger.info("Time: "+(System.currentTimeMillis() -t0))
    logger.info("========END CHECKLIST REGION TAB SERVICE=========")
    await(
      Future{
        CLRegionResponse(trendRegionMonth, checklistRegion, cmtChart, (trendTbtMonth._1, trendTbtMonth._2, trendTbtMonth._3), (trendTbtMonth._4, trendTbtMonth._5), month)
      }
    )
  }
}