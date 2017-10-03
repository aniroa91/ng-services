package services.user

import com.sksamuel.elastic4s.http.ElasticDsl._
import services.Configure
import com.ftel.bigdata.utils.ESUtil
import com.sksamuel.elastic4s.searches.SearchDefinition
import services.ElasticUtil
import services.domain.AbstractService
//import model.paytv.InternetContract
import com.ftel.bigdata.utils.DateTimeUtil
//import model.paytv.PayTVContract
//import model.user.Response
import model.user.PayTVSegment
import model.user.PayTVVector
import model.user.InternetSegment
import services.Bucket
//import utils.Session
//import model.user.Bill
import services.BucketDouble
import services.Bucket2
import model.user.ProfileResponse
import model.user.PayTVContract
import model.user.PayTVResponse
import model.user.PayTVBox
import model.user.InternetResponse
import model.user.InternetContract
import model.user.Session

object ProfileService extends AbstractService {
  //val client = Configure.client
  val SIZE_DEFAULT = 100

  private def getHourly(boxId: String): SearchDefinition = {
    search(s"paytv-weekly-hourly-2017-10-02" / "docs") query { must(termQuery("customer", boxId)) } aggregations (
      termsAggregation("top")
      .field("hour")
      .subaggs(
        sumAgg("sum", "value")) size SIZE_DEFAULT)
  }

  private def getHourlyInMonth(boxId: String): SearchDefinition = {
    search(s"paytv-weekly-hourly-*" / "docs") query { must(termQuery("customer", boxId)) } aggregations (
      termsAggregation("top")
      .field("hour")
      .subaggs(
        sumAgg("sum", "value")) size SIZE_DEFAULT)
  }

  private def getApp(boxId: String): SearchDefinition = {
    search(s"paytv-weekly-daily-2017-10-02" / "docs") query { must(termQuery("customer", boxId)) } aggregations (
      termsAggregation("top")
      .field("app")
      .subaggs(
        sumAgg("sum", "value")) size SIZE_DEFAULT)
  }

  private def getDayOfWeek(boxId: String): SearchDefinition = {
    search(s"paytv-weekly-daily-2017-10-02" / "docs") query { must(termQuery("customer", boxId)) } aggregations (
      termsAggregation("top")
      .field("dayOfWeek")
      .subaggs(
        sumAgg("sum", "value")) size SIZE_DEFAULT)
  }

  private def getIPTV(boxId: String): SearchDefinition = {
    search(s"paytv-weekly-iptv-2017-10-02" / "docs") query { must(termQuery("customer", boxId)) } aggregations (
      termsAggregation("top")
      .field("cate")
      .subaggs(
        sumAgg("sum", "value")) size SIZE_DEFAULT)
  }

  private def getAppHourly(boxId: String): SearchDefinition = {
    search(s"paytv-weekly-hourly-2017-10-02" / "docs") query { must(termQuery("customer", boxId)) } aggregations (
      termsAggregation("top").field("app")
      .subaggs(termsAggregation("sub").field("hour")
        .subaggs(sumAgg("sum", "value")) size SIZE_DEFAULT) size SIZE_DEFAULT) limit SIZE_DEFAULT
  }

  private def getAppDayOfWeek(boxId: String): SearchDefinition = {
    search(s"paytv-weekly-daily-2017-10-02" / "docs") query { must(termQuery("customer", boxId)) } aggregations (
      termsAggregation("top")
      .field("app")
      .subaggs(
        termsAggregation("sub").field("dayOfWeek").subaggs(
          sumAgg("sum", "value")) size SIZE_DEFAULT) size SIZE_DEFAULT) limit (SIZE_DEFAULT * 10)

  }

  //  private def getVOD(to: String, boxId: String): SearchDefinition = {
  //    search(s"vod_cate" / "docs") query { must(termQuery("customer", boxId)) } aggregations (
  //      termsAggregation("top")
  //      .field("cate")
  //      .subaggs(
  //        sumAgg("sum", "value")) size SIZE_DEFAULT)
  //  }
  //  
  //  private def getVODthieunhi(to: String, boxId: String): SearchDefinition = {
  //    search(s"vod_thieu" / "docs") query { must(termQuery("customer", boxId)) } aggregations (
  //      termsAggregation("top")
  //      .field("cate")
  //      .subaggs(
  //        sumAgg("sum", "value")) size SIZE_DEFAULT)
  //  }
  //  
  //  private def getVODgiaitri(to: String, boxId: String): SearchDefinition = {
  //    search(s"vod_giaitri" / "docs") query { must(termQuery("customer", boxId)) } aggregations (
  //      termsAggregation("top")
  //      .field("cate")
  //      .subaggs(
  //        sumAgg("sum", "value")) size SIZE_DEFAULT)
  //  }

  private def getDaily(month: Int, boxId: String): SearchDefinition = {
    search(s"paytv-weekly-daily-*" / "docs") query { must(termQuery("customer", boxId), termQuery("month", month)) } aggregations (
      termsAggregation("top")
      .field("day")
      .subaggs(
        sumAgg("sum", "value")) size SIZE_DEFAULT)
  }

  def get(contract: String): ProfileResponse = {
    ProfileResponse(getInternetResponse(contract), getPayTVResponse(contract))
  }

  private def getInternetResponse(contract: String): InternetResponse = {
    val internetRes = ESUtil.get(client, "user-contract-internet", "docs", contract)
    val internetSource = internetRes.source
    val internetInfo = InternetContract(
      getValueAsString(internetSource, "contract"),
      getValueAsString(internetSource, "object_id"),
      getValueAsString(internetSource, "name"),
      getValueAsString(internetSource, "profile"),
      getValueAsString(internetSource, "profile_type"),
      getValueAsInt(internetSource, "upload_lim").toLong,
      getValueAsInt(internetSource, "download_lim").toLong,
      getValueAsString(internetSource, "status"),
      getValueAsString(internetSource, "mac_address"),
      DateTimeUtil.create(getValueAsLong(internetSource, "start_date") / 1000),
      DateTimeUtil.create(getValueAsLong(internetSource, "active_date") / 1000),
      DateTimeUtil.create(getValueAsLong(internetSource, "change_date") / 1000),
      getValueAsString(internetSource, "location"),
      getValueAsString(internetSource, "region"),
      getValueAsString(internetSource, "point_set"),
      getValueAsString(internetSource, "host"),
      getValueAsInt(internetSource, "port"),
      getValueAsInt(internetSource, "slot"),
      getValueAsInt(internetSource, "onu"),
      getValueAsString(internetSource, "cable_type"),
      getValueAsInt(internetSource, "life_time"))
    val internetSegmentRes = ESUtil.get(client, "segment-internet", "docs", contract)
    val internetSegmentSource = internetSegmentRes.source
    val segment = InternetSegment(
      getValueAsString(internetSegmentSource, "Contract"),
      getValueAsString(internetSegmentSource, "City"),
      getValueAsString(internetSegmentSource, "Region"),
      getValueAsString(internetSegmentSource, "InternetLifeToEnd"),
      getValueAsString(internetSegmentSource, "Session_Count"),
      getValueAsString(internetSegmentSource, "ssOnline_Mean"),
      getValueAsString(internetSegmentSource, "DownUpload"),
      getValueAsString(internetSegmentSource, "AttendNew"),
      getValueAsString(internetSegmentSource, "InternetAvgFee"),
      getValueAsString(internetSegmentSource, "LoaiKH"),
      getValueAsString(internetSegmentSource, "Nhom_CheckList"),
      getValueAsString(internetSegmentSource, "So_checklist"),
      getValueAsString(internetSegmentSource, "LifeToEndFactor"),
      getValueAsString(internetSegmentSource, "Nhom_Tuoi"),
      getValueAsString(internetSegmentSource, "AvgFeeFactor"),
      getValueAsString(internetSegmentSource, "Nhom_Cuoc"),
      getValueAsString(internetSegmentSource, "KetnoiFactor"),
      getValueAsString(internetSegmentSource, "Nhom_Ket_Noi"),
      getValueAsString(internetSegmentSource, "NCSDFactor"),
      getValueAsString(internetSegmentSource, "Nhom_Nhu_Cau"),
      getValueAsString(internetSegmentSource, "So_Lan_Loi_Ha_Tang"),
      getValueAsString(internetSegmentSource, "So_Ngay_Loi_Ha_Tang"))

    val downupRes = ESUtil.get(client, "downup", "docs", contract)
    val durationRes = ESUtil.get(client, "duration", "docs", contract)
    val downupSource = downupRes.source
    //    downupSource.keySet.filter(x => x.contains("Download")).foreach(println)
    val download = downupSource.keySet.filter(x => x.contains("Download"))
      .map(x => x.substring(4).replace("Download", "") -> getValueAsString(downupSource, x))
      .map(x => (x._1.toInt + 1) -> (if (x._2.toDouble < 0) 0 else x._2.toDouble * 1024))
      .toArray
      .map(x => x._1 -> BigDecimal(x._2).setScale(3, BigDecimal.RoundingMode.HALF_UP).toDouble)
      .sortBy(x => x._1)

    val upload = downupSource.keySet.filter(x => x.contains("Upload"))
      .map(x => x.substring(4).replace("Upload", "") -> getValueAsString(downupSource, x))
      .map(x => (x._1.toInt + 1) -> (if (x._2.toDouble < 0) 0 else x._2.toDouble * 1024))
      .toArray
      .map(x => x._1 -> BigDecimal(x._2).setScale(3, BigDecimal.RoundingMode.HALF_UP).toDouble)
      .sortBy(x => x._1)
    val duration = durationRes.source.keySet.filter(x => x.contains("Session"))
      .map(x => x.replace("Session", "") -> getValueAsString(durationRes.source, x))
      .map(x => (x._1.toInt + 1) -> (x._2.toDouble / 60))
      .toArray
      .map(x => x._1 -> BigDecimal(x._2).setScale(3, BigDecimal.RoundingMode.HALF_UP).toDouble)
      .sortBy(x => x._1)
      .take(28)
      
    val pon = client.execute(search(s"pon" / "docs") query { must(termQuery("contract.keyword", contract)) } limit 1000).await
    val suyhoutSource = if (pon.totalHits <= 0) {
      client.execute(search(s"adsl" / "docs") query { must(termQuery("contract.keyword", contract)) } limit 1000).await
    } else pon

    //println(suyhout.totalHits)
    val suyhout = suyhoutSource.hits.hits.map(x => x.sourceAsMap)
      .map(x => (getValueAsLong(x, "date") / 1000) -> getValueAsString(x, "passed"))
      .map(x => DateTimeUtil.create(x._1).toString(DateTimeUtil.YMD) -> x._2)

    val errorRes = client.execute(search(s"inf" / "docs") query { must(termQuery("contract.keyword", contract)) } limit 1000).await
    val error = errorRes.hits.hits.map(x => x.sourceAsMap)
      .map(x => (getValueAsLong(x, "date") / 1000) -> (getValueAsInt(x, "time"), getValueAsString(x, "error"), getValueAsString(x, "n_error")))
      .map(x => DateTimeUtil.create(x._1).toString(DateTimeUtil.YMD) -> x._2)
    val module = error.filter(x => x._2._2 == "module/cpe error").map(x => x._1 -> x._2._3.toInt).groupBy(x => x._1).map(x => x._1 -> x._2.map(y => y._2).sum).toArray
    val disconnet = error.filter(x => x._2._2 == "disconnect/lost IP").map(x => x._1 -> x._2._3.toInt).groupBy(x => x._1).map(x => x._1 -> x._2.map(y => y._2).sum).toArray

    val sessionRes = ESUtil.get(client, "session", "docs", contract)
    val session: Session = if (sessionRes.exists) {
      val map = sessionRes.source
      Session(contract,
        getValueAsInt(map, "Session_Count"),
        getValueAsInt(map, "ssOnline_Min"),
        getValueAsInt(map, "ssOnline_Max"),
        getValueAsDouble(map, "ssOnline_Mean"),
        getValueAsDouble(map, "ssOnline_Std"))
    } else null
    val checkListRes = ESUtil.get(client, "check-list", "docs", contract)
    val checkList = if (checkListRes.exists) {
      val map = checkListRes.source
      getValueAsString(map, "Nhom_CheckList") -> getValueAsInt(map, "So_checklist")
    } else null
    val internetBillRes = ESUtil.get(client, "bill-internet", "docs", contract)
    val internetBill = if (internetBillRes.exists) getValueAsInt(internetBillRes.source, "SoTienDaThanhToan") else 0
    InternetResponse(internetInfo, segment, download, upload, duration, suyhout, error, module, disconnet, session, checkList, internetBill)
  }

  private def getPayTVResponse(contract: String): PayTVResponse = {
    val payTVRes = ESUtil.get(client, "user-contract-paytv", "docs", contract)
    if (payTVRes.exists) {
      // payTV contract
      val paytvSource = payTVRes.source
      val payTVContract = PayTVContract(
        getValueAsString(paytvSource, "contract"),
        getValueAsInt(paytvSource, "box_count"),
        getValueAsString(paytvSource, "status"),
        DateTimeUtil.create(getValueAsLong(paytvSource, "start_date") / 1000),
        DateTimeUtil.create(getValueAsLong(paytvSource, "active_date") / 1000),
        DateTimeUtil.create(getValueAsLong(paytvSource, "change_date") / 1000))

      // Box
      val boxRes = client.execute(search(s"user-contract-box" / "docs") query { must(termQuery("contract.keyword", contract)) } limit 10).await
      val boxs = boxRes.hits.hits.map(x => x.sourceAsMap)
        .map(x => PayTVBox(getValueAsString(x, "customer_id"),
          getValueAsString(x, "contract"),
          getValueAsString(x, "status"),
          DateTimeUtil.create(getValueAsLong(paytvSource, "change_date") / 1000),
          getValueAsString(x, "mac_address")))

      // Get Segment
      val segments = boxs.map(x => x -> ESUtil.get(client, "user-segment-paytv-2017-10-02", "docs", x.id))
        .map(x => x._1 -> x._2.source)
        .map(x => x._1.id -> PayTVSegment(
          getValueAsString(x._2, "cluster_app"),
          getValueAsString(x._2, "cluster_hourly"),
          getValueAsString(x._2, "cluster_daily"),
          getValueAsString(x._2, "cluster_lifeoemd"),
          getValueAsString(x._2, "cluster_sum"),
          getValueAsString(x._2, "cluster_iptv"),
          getValueAsString(x._2, "cluster_vod"),
          getValueAsString(x._2, "cluster_vod_giaitri"),
          getValueAsString(x._2, "cluster_vod_thieunhi"),
          x._1.status)).toMap
      // Vector
      val vectors = boxs.map(x => x.id).map(x => {
        val hourly = getHourly(x)
        val hourlyInMonth = getHourlyInMonth(x)
        val app = getApp(x)
        val dayOfWeek = getDayOfWeek(x)
        val iptv = getIPTV(x)
        val appHourly = getAppHourly(x)
        //println(client.show(appHourly))
        val appDaily = getAppDayOfWeek(x)
        val daily = getDaily(8, x)
        val multiSearchResponse = client.execute(multi(hourly, app, dayOfWeek, iptv, appHourly, appDaily, daily, hourlyInMonth)).await

        //multiSearchResponse.responses(4).aggregations.foreach(println)

        val hourlyBucket = ElasticUtil.getBucketDoubleTerm(multiSearchResponse.responses(0), "top", "sum")
        val appBucket = ElasticUtil.getBucketDoubleTerm(multiSearchResponse.responses(1), "top", "sum")

        val dayOfWeekBucket = ElasticUtil.getBucketDoubleTerm(multiSearchResponse.responses(2), "top", "sum")
          .sortBy(x => x.key.toInt)
          .map(x => BucketDouble(dayOfWeekNumberToLabel(x.key.toInt), x.count, x.value))
        val iptvBucket = ElasticUtil.getBucketDoubleTerm(multiSearchResponse.responses(3), "top", "sum")
        val appHourlyBucket = ElasticUtil.getBucketTerm2(multiSearchResponse.responses(4), "top", "sum")
        val appDailyBucket = ElasticUtil.getBucketTerm2(multiSearchResponse.responses(5), "top", "sum")
        val dailyBucket = ElasticUtil.getBucketDoubleTerm(multiSearchResponse.responses(6), "top", "sum").sortBy(x => x.key.toInt)
        val hourlyInMonthBucket = ElasticUtil.getBucketDoubleTerm(multiSearchResponse.responses(7), "top", "sum")

        val vodRes = ESUtil.get(client, "vod_cate", "docs", x)
        val vodthieuRes = ESUtil.get(client, "vod_thieu", "docs", x)
        val vodgiaitriRes = ESUtil.get(client, "vod_giaitri", "docs", x)

        val vod = if (vodRes.exists) {
          val source = vodRes.source
          source.keySet.filter(x => x != "ds" && x != "contract" && x != "customer_id" && x != "vec_type").map(x => Bucket(x, 0, getValueAsInt(source, x))).toArray
        } else null

        val vodthieu = if (vodthieuRes.exists) {
          val source = vodthieuRes.source
          source.keySet.filter(x => x != "ds" && x != "contract" && x != "customer_id" && x != "vec_type").map(x => Bucket(x, 0, getValueAsInt(source, x))).toArray
        } else null

        val vodgiaitri = if (vodgiaitriRes.exists) {
          val source = vodgiaitriRes.source
          source.keySet.filter(x => x != "ds" && x != "contract" && x != "customer_id" && x != "vec_type").map(x => Bucket(x, 0, getValueAsInt(source, x))).toArray
        } else null

        def group(array: Array[Bucket2], key: String): Array[(String, Double)] = {
          array.filter(a => a.key == key).map(a => a.term -> a.value)
            .groupBy(a => a._1).map(a => a._1 -> a._2.map(b => b._2).sum)
            .toArray
            .sortBy(a => a._1.toInt)
            .map(x => x._1 -> BigDecimal(x._2).setScale(3, BigDecimal.RoundingMode.HALF_UP).toDouble)
        }
        def group2(array: Array[Bucket2], key: String): Array[(String, Double)] = {
          array.filter(a => a.key == key).map(a => a.term -> a.value)
            .groupBy(a => a._1).map(a => a._1 -> a._2.map(b => b._2).sum)
            .toArray
            .sortBy(a => a._1.toInt)
            .map(x => dayOfWeekNumberToLabel(x._1.toInt) -> BigDecimal(x._2).setScale(3, BigDecimal.RoundingMode.HALF_UP).toDouble)
        }
        def groupHourAndDayOfWeek(array: Array[Bucket2]): Array[(String, Array[Double])] = {
          array.groupBy(x => x.key)
            .map(x => x._1 -> x._2.map(y => y.term -> y.value).sortBy(y => y._1).map(y => y._2))
            .toArray
        }

        //val b = a.groupBy(x => x._1).map(x => x._1 -> x._2.map(y => y._2).sum).toArray
        x -> PayTVVector(hourlyBucket, hourlyInMonthBucket, appBucket, dayOfWeekBucket, iptvBucket, appHourlyBucket, appDailyBucket,
          groupHourAndDayOfWeek(appHourlyBucket),
          groupHourAndDayOfWeek(appDailyBucket),
          group(appHourlyBucket, "IPTV"),
          group(appHourlyBucket, "VOD"),
          group2(appDailyBucket, "IPTV"),
          group2(appDailyBucket, "VOD"),
          vod, vodthieu, vodgiaitri, dailyBucket)
      }).toMap

      val payTVBillRes = ESUtil.get(client, "user-bill-paytv-2017-09", "docs", contract)
      val bill = if (payTVBillRes.exists) getValueAsInt(payTVBillRes.source, "BillFee") else 0

      PayTVResponse(payTVContract, boxs, segments, vectors, bill)
    } else null
  }

  def main(args: Array[String]) {
    val time0 = System.currentTimeMillis()
    val response = ProfileService.get("SGB000000")
    val a = response.paytv.vectors.get("395238").get
    a.appHourly2.foreach(println)
    val time1 = System.currentTimeMillis()
    println(time1 - time0)
    client.close()
  }

  def dayOfWeekNumberToLabel = (i: Int) => {
    i match {
      case 0 => "Mon"
      case 1 => "Tue"
      case 2 => "Wed"
      case 3 => "Thu"
      case 4 => "Fri"
      case 5 => "Sat"
      case 6 => "Sun"
      case _ => "???"
    }
  }
}