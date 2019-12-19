package service

import churn.models.{AgeResponse, MonthResponse}
import churn.utils.DateTimeUtil.YMD
import com.sksamuel.elastic4s.http.ElasticDsl.{RichFuture, RichString, SearchHttpExecutable, rangeAggregation, search, termsAggregation, _}
import churn.utils.{AgeGroupUtil, CommonUtil, DateTimeUtil, ProvinceUtil}
import org.elasticsearch.search.aggregations.bucket.histogram.DateHistogramInterval
import org.joda.time.{DateTime, DateTimeZone, Days, Months}
import play.api.Logger
import play.api.mvc.{AnyContent, Request}
import service.OverviewService.{checkLocation, getCommentChart}
import services.Configure
import services.domain.CommonService

import scala.concurrent.ExecutionContext.Implicits.global
import scala.async.Async.{async, await}
import scala.concurrent.{Await, Future}
import scala.concurrent.duration.Duration

object OverviewMonthService{

  val client = Configure.client

  val logger = Logger(this.getClass())

  def getRealChurnContract(queries: String, status: String, month: String) = {
    val arrMonth = CommonService.getRangeMonthInYearByLimit(month, 9)
    val res = client.execute(
      search(s"churn-contract-info-*" / "docs") query (s"$status AND "+s"$arrMonth AND "+CommonUtil.filterCommon("package_name"))
        aggregations (
        termsAggregation("month")
          .field("month")
          .subaggs(
            dateHistogramAggregation("daily")
              .field("change_date")
              .interval(DateHistogramInterval.DAY)
              .timeZone(DateTimeZone.forID(DateTimeUtil.TIMEZONE_HCM))
          )
        ) size 0
    ).await
    CommonService.getAggregationsKeyString(res, "month", "daily").map(x => (x._1,  CommonService.getDayFromES5(x._2), x._3))
  }

  def getRealTableChurnContract(queries: String, status: String, month: String, region: String) = {
    val location = checkLocation(region)
    val arrMonth = CommonService.getRangeMonthInYearByLimit(month, 6)
    val res = client.execute(
      search(s"churn-contract-info-*" / "docs") query (s"$queries AND $status AND "+s"$arrMonth AND "+CommonUtil.filterCommon("package_name"))
        aggregations (
        termsAggregation("month")
          .field("month")
          .subaggs(
            termsAggregation(s"$location")
              .field(s"$location")
              .subaggs(
                termsAggregation("status")
                  .field("status")
                  .subaggs(
                    dateHistogramAggregation("daily")
                      .field("change_date")
                      .interval(DateHistogramInterval.DAY)
                      .timeZone(DateTimeZone.forID(DateTimeUtil.TIMEZONE_HCM))
                  )
              ) size 100
          )
        ) size 0
    ).await
    val rs = CommonService.getMultiAggregations4Level(res.aggregations.get("month"), location, "status", "daily")
    val arrRes = rs.flatMap(x=> x._2.map(y=> (x._1, y._1, y._2))).flatMap(x=> x._3.map(y=> (x._1, x._2, y._1, y._2))).flatMap(x=> x._4.map(y=> (x._1, x._2, x._3, CommonService.getDayFromES5(y._1), y._2)))
    val intRegex = """(\d+)""".r
    region match {
      case ""   => arrRes.map(x=> (x._1, "Vung "+x._2, x._3, x._4, x._5))
      case intRegex(region) => arrRes.map(x=> (x._1, CommonService.toTitleCase(ProvinceUtil.getProvince(x._2.toInt)), x._3, x._4, x._5))
      case _       => arrRes
    }
  }

  def getRange6LastMonth(array: Array[(String, String, Long)], arrCurr: Array[(String, Long)], month: String) = {
    val res = arrCurr.map(x => (x._1, x._2, array.filter(y => y._1 < CommonService.getLastNumMonth(month, 2))
       .filter(y => y._2.matches(".*" + x._1.substring(x._1.lastIndexOf("-"))))))
    res.map(x => (x._1, x._2, if(x._3.map(y => y._3).length >0) x._3.map(y => y._3).min else 0, if(x._3.map(y => y._3).length >0) x._3.map(y => y._3).max else 0))
  }

  def getDataPoint(dataPoint: Array[(String, Long, Long, Long)], month:String, inteval: Int) = {
    val lastDayofMonth = DateTimeUtil.getLastDayOfMonth(month).toString(YMD)
    val intevalDaily = DateTimeUtil.getDaysOfMonthByInterval(month, inteval).sorted
    val mapInteval = (0 until intevalDaily.length).map(x=> (x, if(x == (intevalDaily.length-1) && intevalDaily(x) != lastDayofMonth) lastDayofMonth else intevalDaily(x))).toMap
    val res = mapInteval.toArray.sorted.slice(1, mapInteval.size).map(x=> (x._2, dataPoint.filter(y=> y._1 <= x._2 && y._1 > mapInteval.get(x._1-1).get).map(y=> y._2).sum,
      dataPoint.filter(y=> y._1 <= x._2 && y._1 > mapInteval.get(x._1-1).get).map(y=> y._3).sum, dataPoint.filter(y=> y._1 <= x._2 && y._1 > mapInteval.get(x._1-1).get).map(y=> y._4).sum)).sorted
    res
  }

  def avg(xs: Array[Long]) = xs.sum / xs.length

  def compareDate(date1: String, date2: String) = Days.daysBetween(new DateTime(date1), new DateTime(date2)).getDays

  def zipper(map1: Map[String, Long], map2: Map[String, Long], map3: Map[String, Long], map4: Map[String, Long], map5: Map[String, Long]) = {
    for(key <- map1.keys ++ map2.keys ++ map3.keys ++ map4.keys ++ map5.keys)
      yield (key, map1.get(key).getOrElse(0L), map2.get(key).getOrElse(0L), map3.get(key).getOrElse(0L), map4.get(key).getOrElse(0L), map5.get(key).getOrElse(0L))
  }

  def calTbRoiMang(res: Array[(String, String, Int, String, Long)], month: String, inteval:Int) = {
    val currMonth = new DateTime(month).getMonthOfYear
    val currQuarter = if(currMonth % 3 == 0) currMonth/3 else currMonth/3 + 1
    val firstMonth = if ((3*(currQuarter - 1)+ 1) < 10) "0"+(3*(currQuarter - 1)+ 1).toString else (3*(currQuarter - 1)+ 1)
    val firstQuarter = new DateTime(month).getYear + "-" + firstMonth
    val endQuarter = new DateTime(firstQuarter).plusMonths(2).toString("yyyy-MM")

    val lastDayofMonth = DateTimeUtil.getLastDayOfMonth(month).toString(YMD)
    val intevalDaily = DateTimeUtil.getDaysOfMonthByInterval(month, inteval).sorted
    val mapInteval = (0 until intevalDaily.length).map(x=> (x, if(x == (intevalDaily.length-1) && intevalDaily(x) != lastDayofMonth) lastDayofMonth else intevalDaily(x))).toMap

    val sumHuyDV = res.filter(x=> x._3 == 1).filter(x=> x._1 == month).groupBy(x=> x._2).map(x=> x._1-> x._2.map(y=> y._5).sum).toArray.sorted
    var sumHuyLastPoint = res.filter(x=> x._3 == 1).filter(x=> x._4 == CommonService.getPreviousDay()).map(x=> x._2-> x._5).sorted
    var tbcHuyDataPoint = res.filter(x=> x._3 == 1).filter(x=> x._4 != CommonService.getPreviousDay())
      .filter(x => x._4.matches(".*" + CommonService.getPreviousDay().substring(CommonService.getPreviousDay().lastIndexOf("-"))))
        .groupBy(x=> x._2).map(x=> x._1 -> avg(x._2.map(y=> y._5))).toArray.sorted
    // inteval > 1 p
    if(inteval != 1) {
      val arrHuyDv = mapInteval.toArray.sorted.slice(1, mapInteval.size).map(x => (x._2, res.filter(x => x._3 == 1).filter(y => y._4 <= x._2
        && y._4 > mapInteval.get(x._1 - 1).get).map(y => y._2 -> y._5)))
        .flatMap(x => x._2.map(y => (x._1, y._1, y._2))).groupBy(x => x._1 -> x._2).map(x => (x._1._1, x._1._2, x._2.map(y => y._3).sum)).toArray.sorted
      val arrPoints = arrHuyDv.map(x=> x._1).sorted
      sumHuyLastPoint = arrHuyDv.filter(x=> arrPoints(arrPoints.length-1) == x._1).map(x=> x._2 -> x._3).toArray.sorted
      tbcHuyDataPoint = arrHuyDv.filter(x=> arrPoints(arrPoints.length-1) != x._1).groupBy(x=> x._2).map(x=> x._1 -> avg(x._2.map(y=> y._3))).toArray.sorted
    }

    val sumCTBDV = res.filter(x=> x._3 == 3).filter(x=> x._1 == month).groupBy(x=> x._2).map(x=> x._1-> x._2.map(y=> y._5).sum).toArray.sorted
    val rmQuarter = res.filter(x=> x._1 >= firstQuarter && x._1 <= endQuarter).groupBy(x=> x._2).map(x=> x._1-> x._2.map(y=> y._5).sum).toArray.sorted
    val arrSumLoc = zipper(sumHuyDV.toMap, sumHuyLastPoint.toMap, tbcHuyDataPoint.toMap, sumCTBDV.toMap, rmQuarter.toMap).map(x=> (x._1, x._2, x._3, x._4, x._5, x._2+x._5, x._6, x._2 *1.00/x._3))
      .toArray.sortWith((x, y) => x._8 > y._8)

    (currQuarter, arrSumLoc)
  }


  def getInternet(user: String, request: Request[AnyContent]) = async{
    logger.info("========START THIS MONTH SERVICE=========")
    val t0 = System.currentTimeMillis()
    val age = request.body.asFormUrlEncoded.get("age").head
    val province = request.body.asFormUrlEncoded.get("province").head
    val packages = request.body.asFormUrlEncoded.get("package").head
    val combo = request.body.asFormUrlEncoded.get("combo").head
    val month = request.body.asFormUrlEncoded.get("month").head
    val status = request.body.asFormUrlEncoded.get("status").head
    val point = request.body.asFormUrlEncoded.get("dataPoint").head.toInt
    val queries = OverviewService.getFilterGroup(age, province, packages, combo)
    //println(queries)
    logger.info("t0: "+(System.currentTimeMillis() - t0))
    val t1 = System.currentTimeMillis()
    // Arearange line
    val rsStatus = if(status == "" || status == "13") "(status:1 OR status:3)" else s"(status:$status)"
    val arrReal6LastMonth = Await.result(Future{ getRealChurnContract(queries, rsStatus, month)}, Duration.Inf)
    val arrReal3LastMonth = arrReal6LastMonth.filter(x=> x._1 >= CommonService.getLastNumMonth(month, 2)).map(x=> x._2 -> x._3).sorted
    val arrRangeDaily = if(point == 1) getRange6LastMonth(arrReal6LastMonth, arrReal3LastMonth, month)
        else getDataPoint(getRange6LastMonth(arrReal6LastMonth, arrReal3LastMonth, month), month, point)
    logger.info("t1: "+(System.currentTimeMillis() - t1))
    // table trending
    val t2 = System.currentTimeMillis()
    val tbRes = Await.result(Future{ getRealTableChurnContract(queries, rsStatus, month, province)}, Duration.Inf)
    val arrTbRm = calTbRoiMang(tbRes, month, point)
    val sumRm = Array((arrTbRm._2.map(y=> y._2).sum, arrTbRm._2.map(y=> y._3).sum, arrTbRm._2.map(y=> y._4).sum,
      arrTbRm._2.map(y=> y._5).sum, arrTbRm._2.map(y=> y._6).sum, arrTbRm._2.map(y=> y._7).sum, 0.0))
    logger.info("t2: "+(System.currentTimeMillis() - t2))
    // comments content
    val t3 = System.currentTimeMillis()
    val cmtChart = if(point == 1) Await.result(Future{ getCommentChart(user, CommonUtil.PAGE_ID.get(0).get+"_tabMonth") }, Duration.Inf) else ""
    logger.info("t3: "+(System.currentTimeMillis() - t3))

    logger.info("Time: "+(System.currentTimeMillis() - t0))
    logger.info("========END THIS MONTH SERVICE=========")
    await(
      Future{
        MonthResponse((arrTbRm._1, arrTbRm._2, sumRm), arrRangeDaily, cmtChart, month)
      }
    )
  }
}
