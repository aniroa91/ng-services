package services.domain

import java.text.DecimalFormat
import java.time.format.DateTimeFormatter

import com.ftel.bigdata.utils.DateTimeUtil
import org.elasticsearch.search.sort.SortOrder
import org.joda.time.{DateTime, Days}

import scala.collection.mutable
import scala.util.control.Breaks.{break, breakable}

//import com.ftel.bigdata.dns.parameters.Label
import com.ftel.bigdata.utils.DateTimeUtil
import com.ftel.bigdata.utils.WhoisUtil
import com.ftel.bigdata.whois.Whois
import com.sksamuel.elastic4s.http.ElasticDsl.IndexHttpExecutable
import com.sksamuel.elastic4s.http.ElasticDsl._
import com.sksamuel.elastic4s.http.search.SearchResponse

import model.MainDomainInfo
import scala.util.Try
import services.Configure
import services.Bucket2
import com.ftel.bigdata.utils.FileUtil
import com.ftel.bigdata.utils.HttpUtil
import org.elasticsearch.search.aggregations.bucket.terms.Terms
import org.apache.http.HttpHost
import scalaj.http.Http
import play.api.libs.json.Json
import scala.concurrent.duration.Duration
import scala.concurrent.duration._

import java.util.concurrent.TimeUnit
import org.jsoup.Jsoup
import org.jsoup.select.Elements
import play.api.libs.json.JsObject
import com.ftel.bigdata.utils.StringUtil
import org.joda.time.format.DateTimeFormat
import org.joda.time.DateTime
import java.time.format.DateTimeFormatter

object CommonService extends AbstractService {

  val SIZE_DEFAULT = 20
  val RANK_HOURLY = "0,1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23"
  /**
   * Service for Get Information about day
   */

  def normalizeArray(months: Array[String], array: Array[(String, String, Int)]): Array[(String, String, Int)] = {
    val terms = array.map(x => x._2).distinct
    val map = array.map(x => (x._1, x._2) -> x._3).toMap
    months.flatMap(x => terms.map(y => (x,y)))
      .map(x => x -> map.getOrElse(x, 0))
      .map(x => (x._1._1, x._1._2, x._2) )

  }

  def getLongValueByKey(arr: Array[(String,Long)], key:String):Int = {
    var value = 0;
    breakable{for(i <- 0 until arr.length){
      if(arr(i)._1 == key) {
        value = arr(i)._2.toInt
        break
      }
    }}
    return value;
  }
  def getIntValueByKey(arr: Array[(Int,Int)], key:Int):Int = {
    var value = 0;
    breakable{for(i <- 0 until arr.length){
      if(arr(i)._1 == key) {
        value = arr(i)._2
        break
      }
    }}
    return value;
  }

  def getLatestDay(): String = {
    val response = client.execute(
      search("dns-marker" / "docs") sortBy { fieldSort("day") order SortOrder.DESC } limit 1).await(Duration(30, SECONDS))
    response.hits.hits.head.id//.sourceAsMap.getOrElse("day", "").toString()
  }

  def getPreviousDay(day: String): String = {
    val prev = DateTimeUtil.create(day, DateTimeUtil.YMD)
    prev.minusDays(1).toString(DateTimeUtil.YMD)
  }

  def getNextDay(day: String): String = {
    val prev = DateTimeUtil.create(day, DateTimeUtil.YMD)
    prev.plusDays(1).toString(DateTimeUtil.YMD)
  }

  def getCurrentDay(): String = {
    val date = new DateTime()
    date.toString(DateTimeFormat.forPattern("yyyy-MM-dd"))
  }

  def getCurrentMonth(): String = {
    val date = new DateTime()
    date.toString(DateTimeFormat.forPattern("yyyy-MM"))
  }

  def getpreviousMinutes(times: Int): String = {
    val date = new DateTime()
    date.minusMinutes(times).toString()
  }

  def getAggregations(aggr: Option[AnyRef], hasContract: Boolean): Array[(String, Long, Long, Long, Long, Long)] = {
    aggr.getOrElse("buckets", Map[String, AnyRef]()).asInstanceOf[Map[String, AnyRef]]
      .getOrElse("buckets", List).asInstanceOf[List[AnyRef]]
      .map(x => x.asInstanceOf[Map[String, AnyRef]])
      .map(x => {
        val key = x.getOrElse("key", "0L").toString
        val count = x.getOrElse("doc_count", 0L).toString().toLong
        val contract = if (hasContract) x.get("contract").get.asInstanceOf[Map[String, Integer]].get("value").get.toLong else 0L
        val download = x.get("download").get.asInstanceOf[Map[String, Double]].get("value").get.toLong
        val upload = x.get("upload").get.asInstanceOf[Map[String, Double]].get("value").get.toLong
        val duration = x.get("duration").get.asInstanceOf[Map[String, Double]].get("value").get.toLong
        (key, contract, count, download, upload, duration)
      })
      .toArray
  }

  def getAggregationsSiglog(aggr: Option[AnyRef]): Array[(String, Long)] = {
    aggr.getOrElse("buckets", Map[String, AnyRef]()).asInstanceOf[Map[String, AnyRef]]
      .getOrElse("buckets", List).asInstanceOf[List[AnyRef]]
      .map(x => x.asInstanceOf[Map[String, AnyRef]])
      .map(x => {
        val key = x.getOrElse("key", "0L").toString
        val count = x.getOrElse("doc_count", 0L).toString().toLong
        (key, count)
      })
      .toArray
  }

  def getMultiAggregations(aggr: Option[AnyRef]):  Array[(String, Array[(String, Array[(String, Long)])])] = {
    aggr.getOrElse("buckets", Map[String, AnyRef]()).asInstanceOf[Map[String, AnyRef]]
      .getOrElse("buckets", List).asInstanceOf[List[AnyRef]]
      .map(x => x.asInstanceOf[Map[String, AnyRef]])
      .map(x => {
        val key = x.getOrElse("key", "0L").toString
        val map = x.getOrElse("card",Map[String,AnyRef]()).asInstanceOf[Map[String,AnyRef]]
           .getOrElse("buckets",List).asInstanceOf[List[AnyRef]]
             .map(x => x.asInstanceOf[Map[String,AnyRef]])
               .map(x => {
                 val keyCard = x.getOrElse("key","0L").toString
                 val map = x.getOrElse("port",Map[String,AnyRef]()).asInstanceOf[Map[String,AnyRef]]
                             .getOrElse("buckets",List).asInstanceOf[List[AnyRef]]
                               .map(x=> x.asInstanceOf[Map[String,AnyRef]])
                                  .map(x=> {
                                    val keyPort = x.getOrElse("key","0L").toString
                                    val count = x.getOrElse("doc_count",0L).toString.toLong
                                    (keyPort,count)
                                  }).toArray
                 (keyCard,map)
               }).toArray
        (key, map)
      })
      .toArray
  }

  def getSecondAggregations(aggr: Option[AnyRef],secondField: String):  Array[(String, Array[(String, Long)])] = {
    aggr.getOrElse("buckets", Map[String, AnyRef]()).asInstanceOf[Map[String, AnyRef]]
      .getOrElse("buckets", List).asInstanceOf[List[AnyRef]]
      .map(x => x.asInstanceOf[Map[String, AnyRef]])
      .map(x => {
        val key = x.getOrElse("key", "0L").toString
        val map = x.getOrElse(s"$secondField",Map[String,AnyRef]()).asInstanceOf[Map[String,AnyRef]]
          .getOrElse("buckets",List).asInstanceOf[List[AnyRef]]
          .map(x => x.asInstanceOf[Map[String,AnyRef]])
          .map(x => {
            val keyCard = x.getOrElse("key","0L").toString
            val count = x.getOrElse("doc_count",0L).toString.toLong
            (keyCard,count)
          }).toArray
        (key, map)
      })
      .toArray
  }

  def getSecondSumAggregations(aggr: Option[AnyRef],secondField: String):  Array[(String, Array[(String, Long)])] = {
    aggr.getOrElse("buckets", Map[String, AnyRef]()).asInstanceOf[Map[String, AnyRef]]
      .getOrElse("buckets", List).asInstanceOf[List[AnyRef]]
      .map(x => x.asInstanceOf[Map[String, AnyRef]])
      .map(x => {
        val key = x.getOrElse("key", "0L").toString
        val map = x.getOrElse(s"$secondField",Map[String,AnyRef]()).asInstanceOf[Map[String,AnyRef]]
          .getOrElse("buckets",List).asInstanceOf[List[AnyRef]]
          .map(x => x.asInstanceOf[Map[String,AnyRef]])
          .map(x => {
            val keyCard = x.getOrElse("key","0L").toString
            val sum = x.getOrElse("sum",Map[String,AnyRef]()).asInstanceOf[Map[String,AnyRef]]
              .map(x=> x._2.toString.toDouble).toList(0).toLong
            (keyCard,sum)
          }).toArray
        (key, map)
      })
      .toArray
  }

  def getPreviousDay(day: String, num: Int): String = {
    val prev = DateTimeUtil.create(day, DateTimeUtil.YMD)
    prev.minusDays(num).toString(DateTimeUtil.YMD)
  }

  def getRangeDay(day: String): String = {
    var from = day.split("/")(0)
    val to = day.split("/")(1)
    val prev = DateTimeUtil.create(from, DateTimeUtil.YMD)
    val next = DateTimeUtil.create(to, DateTimeUtil.YMD)
    val numDays = Days.daysBetween(prev, next).getDays()
    for (f<- 1 to numDays) {
      from += ","+prev.plusDays(f).toString(DateTimeUtil.YMD)
    }
    return from
  }

  def isDayValid(day: String): Boolean = {
    Try(DateTimeUtil.create(day, DateTimeUtil.YMD)).isSuccess
  }

  def getWhoisInfo(domain: String): Whois = {
    //new Whois()
    if (redis == null) new Whois()
    else {
      val map = redis.hgetall1(domain).getOrElse(Map[String, String]())
      if (!map.isEmpty) {
        Whois(
          domain,
          getValueAsString(map, "registrar"),
          getValueAsString(map, "whoisServer"),
          getValueAsString(map, "referral"),
          getValueAsString(map, "nameServer").split(","),
          getValueAsString(map, "status"),
          getValueAsString(map, "create"),
          getValueAsString(map, "update"),
          getValueAsString(map, "expire"))
      } else new Whois()
    }
  }
  
  /**
   * Get Whois From Web
   */
  @deprecated
  def getWhoisInfo(whoisResponse: SearchResponse, domain: String, label: String, malware: String): Whois = {
    if (whoisResponse != null) {
      println("Whois: " + whoisResponse + "-" + whoisResponse.totalHits)
    if (whoisResponse.totalHits > 0) {
      val map = whoisResponse.hits.hits.head.sourceAsMap
      val whois = Whois(
        map.getOrElse("domain", "").toString(),
        map.getOrElse("registrar", "").toString(),
        map.getOrElse("whoisServer", "").toString(),
        map.getOrElse("referral", "").toString(),
        map.getOrElse("servername", "").toString().split(" "),
        map.getOrElse("status", "").toString(),
        map.getOrElse("create", "").toString(),
        map.getOrElse("update", "").toString(),
        map.getOrElse("expire", "").toString())//,
        //map.getOrElse("label", "").toString(),
        //map.getOrElse("malware", "").toString())
      whois
    } else {
      CommonService.backgroupJob(
          getWhoisFromWeb(domain, label, malware),
          "Download Whois for " + domain)
      //getWhoisFromWeb(domain, label, malware)
      new Whois()
    } } else  new Whois()
  }
  
  private def getWhoisFromWeb(domain: String, label: String, malware: String): Whois = {
    val esIndex = s"dns-service-domain-whois"
    val esType = "whois"
    try {
      val whois = WhoisUtil.whoisService(domain, Configure.PROXY_HOST, Configure.PROXY_PORT)
      //println(whois)
      if (whois.isValid()) {
        indexWhois(esIndex, esType, whois)
        whois
      } else new Whois()
    } catch {
      case e: Exception => e.printStackTrace(); new Whois()
    }
  }
  
  private def indexWhois(esIndex: String, esType: String, whois: Whois) {
    client.execute(
      indexInto(esIndex / esType) fields (
        "domain" -> whois.domainName,
        "registrar" -> whois.registrar,
        "whoisServer" -> whois.whoisServer,
        "referral" -> whois.referral,
        "servername" -> whois.nameServer.mkString(" "),
        "status" -> whois.status,
        "create" -> whois.create.substring(0, 10),
        "update" -> whois.update.substring(0, 10),
        "expire" -> (if (whois.expire.isEmpty()) "2999-12-31" else whois.expire.substring(0, 10)),
        "label" -> "",
        "malware" -> "")
        id whois.domainName).await(Duration(30, SECONDS))
  }

  /**
   * Get Category
   */
  def getCategory(domain: String): String = {
    val getResponse = client.execute(com.sksamuel.elastic4s.http.ElasticDsl.get(domain) from "dns-category/docs").await(Duration(30, SECONDS))
    println(domain -> getResponse.sourceAsMap)
    val category = getResponse.sourceAsMap.getOrElse("category", "N/A").toString()
    if (category == "N/A") {
      CommonService.backgroupJob(indexCategory(domain),"Download Category for " + domain)
    }
    category
  }
  
  def indexCategory(domain: String) {
    //val category = getCategorySitereviewBluecoatCom(domain)
    val category = getCategoryFromApiXforceIbmcloud(domain)
//    println(category)
    if (StringUtil.isNotNullAndEmpty(category)) {
      client.execute( indexInto("dns-category" / "docs") fields ("category" -> category) id domain).await(Duration(30, SECONDS))//(Duration.apply(10, TimeUnit.SECONDS))
    }
  }
  
  def getCategorySitereviewBluecoatCom(domain: String): String = {
    
    val req = Http("http://sitereview.bluecoat.com/rest/categorization")
                .proxy(Configure.PROXY_HOST, Configure.PROXY_PORT)
                .postForm(Seq("url" -> domain))
                
    val res = req.asString.body
    //println(res)
    val json = Json.parse(res)
    val option = json.\("categorization")
    if (option.isEmpty) {
      null
    } else {
      val doc = Jsoup.parse(option.get.toString())
      val elements = doc.body().select("a")
      val seq = 0 until elements.size()
      seq.map(x => elements.get(x))
         .map(x => x.text())
         .mkString(" AND ")

//      elements.
//      for (e <- elements.toArray(Elements)) {
//        println("1" + e.text())
//      }
      
//      val endIndex = option.get.toString().lastIndexOf("</a>")
//      val beginIndex = option.get.toString().substring(0, endIndex).lastIndexOf("\\\">") + 3
//      option.get.toString().substring(beginIndex, endIndex)
      
    }
  }
  
  def getCategoryFromApiXforceIbmcloud(domain: String): String = {
    val req = Http("https://api.xforce.ibmcloud.com/url/" + domain)
                .proxy(Configure.PROXY_HOST, Configure.PROXY_PORT)
                .header("Accept", "application/json")
                .header("Authorization", "Basic YTdiYzdiMjctMWRlYy00NTAyLTliM2YtYjVmMGQ3NzNmYjU3OjgyN2RlZWY5LWRkZjUtNDc2MS05ZTkyLTNhYmY5YzVkNDlmYQ==")
    val res = req.asString.body
    //println(res)
    val json = Json.parse(res)
    val cats = json.\\("cats")
    cats.map(x => x.asInstanceOf[JsObject].keys.mkString("/")).distinct.mkString(" AND ")
  }
  
  //curl -X GET --header 'Accept: application/json' --header 'Authorization: Basic YTdiYzdiMjctMWRlYy00NTAyLTliM2YtYjVmMGQ3NzNmYjU3OjgyN2RlZWY5LWRkZjUtNDc2MS05ZTkyLTNhYmY5YzVkNDlmYQ==' 'https://api.xforce.ibmcloud.com/url/vnexpress.net'
  
  /**
   * Get Top
   */
//  def getTopBlackByNumOfQuery(day: String): Array[MainDomainInfo] = {
//    val response = client.execute(
//      search(s"dns-second-${day}" / "docs") query {
//        boolQuery()
//          .must(termQuery("day", day))
//          .not(termQuery("malware", "none"), termQuery("malware", "null"))
//      } sortBy {
//        fieldSort("queries") order (SortOrder.DESC)
//      } limit MAX_SIZE_RETURN).await
//    getMainDomainInfo(response)
//  }

  def getTopByNumOfQuery(day: String, label: String): Array[MainDomainInfo] = {
    val response = client.execute(
      search(s"dns-second-${day}" / "docs") query {
        boolQuery().must(termQuery("day", day), termQuery("label", label))
      } sortBy {
        fieldSort("queries") order (SortOrder.DESC)
      } limit MAX_SIZE_RETURN).await(Duration(30, SECONDS))
    println(s"Time(${day} ${label}): " + response.took)
    getMainDomainInfo(response)
  }

  def getTopByNumOfQueryWithRange(fromDay: String, endDay: String): Array[MainDomainInfo] = {
    val response = client.execute(
      search(s"dns-second-*" / "docs") query {
        boolQuery()
          .must(
              rangeQuery("day").gte(fromDay).lte(endDay),
              rangeQuery("rank").gt(0).lte(MAX_SIZE_RETURN * 10)
              )
      } aggregations (
          termsAggregation("top")
            .field("second")
            .subagg(sumAgg("sum", "queries")) order(Terms.Order.aggregation("sum", false)) size MAX_SIZE_RETURN * 1000
      ) sortBy {
        fieldSort("queries") order (SortOrder.DESC)
      } limit MAX_SIZE_RETURN).await(Duration(30, SECONDS))
    println(s"Time(${fromDay} ${endDay}): " + response.took)
    //response.aggregations.foreach(println)
    getMainDomainInfo2(response)
    //println(response)
    //null
  }
  
  def getTopRank(from: Int, day: String): Array[MainDomainInfo] = {
    val response = client.execute(
      search(s"dns-second-${day}" / "docs") query {
        boolQuery().must(rangeQuery(RANK_FIELD).gt(from - 1).lt(MAX_SIZE_RETURN), termQuery(DAY_FIELD, day))
      } sortBy {
        fieldSort(RANK_FIELD)
      } limit MAX_SIZE_RETURN).await(Duration(30, SECONDS))
    getMainDomainInfo(response)
  }

  /**
   * Utils
   */
//  def formatNumber(number: Int): String = {
//    val formatter = java.text.NumberFormat.getIntegerInstance
//    formatter.format(number)
//  }
  
  def formatNumber(number: Long): String = {
    val formatter = java.text.NumberFormat.getIntegerInstance
    if (number > 1000000000) {
      BigDecimal(number / (1000000000 * 1.0)).setScale(3, BigDecimal.RoundingMode.HALF_UP).toDouble + " B"
    } else if (number > 1000000) {
      BigDecimal(number / (1000000 * 1.0)).setScale(3, BigDecimal.RoundingMode.HALF_UP).toDouble + " M"
    } else if (number > 1000) {
      BigDecimal(number / (1000 * 1.0)).setScale(3, BigDecimal.RoundingMode.HALF_UP).toDouble + " K"
    } else {
      number.toString
    }
    //formatter.format(number)
    
    //BigDecimal(value).setScale(3, BigDecimal.RoundingMode.HALF_UP).toDouble
  }
  
  @deprecated
  def formatNumberOld(number: Long): String = {
    val formatter = java.text.NumberFormat.getIntegerInstance
    formatter.format(number)
    
    //BigDecimal(value).setScale(3, BigDecimal.RoundingMode.HALF_UP).toDouble
  }

  def formatPattern(number: Int): String ={
    val frnum = new DecimalFormat("###,###.###");
    frnum.format(number);
  }

  def formatPatternDouble(number: Double): String ={
    val frnum = new DecimalFormat("###,###.###");
    frnum.format(number);
  }

  def divided(number: Int, prev: Int): Int = {
    val value = number / prev
    value
  }
  
  def percent(number: Long, prev: Long): Double = {
    val value = ((number - prev) / (prev * 1.0)) * 100.0
    BigDecimal(value).setScale(2, BigDecimal.RoundingMode.HALF_UP).toDouble
  }

  def format3Decimal(number: Double): Double = {
    BigDecimal(number).setScale(3, BigDecimal.RoundingMode.HALF_UP).toDouble
  }
  
  def percentInfor(number: Long, total: Long): Double = {
    val value = number/ (total * 1.0) * 100.0
    BigDecimal(value).setScale(2, BigDecimal.RoundingMode.HALF_UP).toDouble
  }

  def formatNumHour(number: Double):Double = {
    val value = number/ 3600 * 1.00
    BigDecimal(value).setScale(2, BigDecimal.RoundingMode.HALF_UP).toDouble
  }

  def getOthers(orgs: Array[(String, Int)], total: Long): Int ={

    total.toInt - orgs.map(x=>x._2).sum
    //percentInfor(orgQueries,total)
  }

  def formatDateYYMMDD( date : DateTime) : String = {
    date.toString(DateTimeFormat.forPattern("yyyy/MM/dd"))
  }

  def formatDateDDMMYY( date : String) : String = {
    val formatter = DateTimeFormat.forPattern("yyyy-mm-dd")
    val dateTime = DateTime.parse(date, formatter)
    dateTime.toString(DateTimeFormat.forPattern("dd/mm/yyyy"))
  }

  def formatUTC(date: String): String = {
    val ES_5_DATETIME_FORMAT = "yyyy-MM-dd'T'HH:mm:ss.SSSZZ"
    val formatter = DateTimeFormat.forPattern(ES_5_DATETIME_FORMAT)
    val dateTime = DateTime.parse(date, formatter)
    dateTime.toString(DateTimeFormat.forPattern("yyyy-MM-dd HH:mm:ss"))
  }

  def formatMilisecondToDate(second: Long): Int = {
   /* val date = DateTime(second,DateTimeUtil.TIMEZONE_HCM)
    date.getHours()*/
    1
  }

  def formatStringToMillisecond(date: String):Long = {
    val formatter = DateTimeFormat.forPattern("yyyy-MM-dd")
    val dateTime = DateTime.parse(date, formatter)
    dateTime.getMillis()
  }

  def formatYYmmddToUTC(date: String): String = {
    val ES_5_DATETIME_FORMAT = "yyyy-MM-dd'T'HH:mm:ss.SSSZZ"
    val formatter = DateTimeFormat.forPattern("yyyy-MM-dd")
    val dateTime = DateTime.parse(date, formatter)
    dateTime.toString(DateTimeFormat.forPattern(ES_5_DATETIME_FORMAT))
  }

  def formatStringToUTC(date: String): String = {
    val ES_5_DATETIME_FORMAT = "yyyy-MM-dd'T'HH:mm:ss.SSSZZ"
    val formatter = DateTimeFormat.forPattern("yyyy-MM-dd HH:mm:ss")
    val dateTime = DateTime.parse(date, formatter)
    dateTime.toString(DateTimeFormat.forPattern(ES_5_DATETIME_FORMAT))
  }

  def formatYYYYmmddHHmmss( date : String) : String = {
    val strTime = date.substring(0,date.indexOf(".")+3)
    val formatter = DateTimeFormat.forPattern("yyyy-MM-dd HH:mm:ss.SSS")
    val dateTime = DateTime.parse(strTime, formatter)
    dateTime.toString(DateTimeFormat.forPattern("yyyy-MM-dd HH:mm:ss"))
  }
  
  def formatSecond(seconds: Double) : String = {
    val minutes = (seconds.toInt / 60)
    val hours = (minutes / 60)
    val days = (hours / 24)
    s"${days}d ${hours%24}h ${minutes%60}m ${seconds.toInt%60}s"
  }

  def getHoursFromMiliseconds(miliseconds: Long): Int = {
    new DateTime(miliseconds).getHourOfDay()
  }
  
  /**
   * Create html tag
   */
  def getImageTag(domain: String): String = {
    val logo = getLogo(domain, false)
    //"<a href=\"/search?q=" + domain + "\"><img src=\"" + logo + "\" width=\"30\" height=\"30\"></a>"
    //<img id="currentPhoto" src="SomeImage.jpg" onerror="this.src='Default.jpg'" width="100" height="120">
    "<a href=\"/search?ct=" + domain + "\"><img src=\"" + logo + "\" onerror=\"this.src='/assets/images/logo/default.png'\" width=\"30\" height=\"30\"></a>"
  }
  
  def getImageTag2(domain: String): String = {
    val logo = getLogo(domain, false)
    //"<a href=\"/search?q=" + domain + "\"><img src=\"" + logo + "\" width=\"30\" height=\"30\"></a>"
    //<img id="currentPhoto" src="SomeImage.jpg" onerror="this.src='Default.jpg'" width="100" height="120">
    "<a href=\"/search?ct=" + domain + "\"><img src=\"" + logo + "\" onerror=\"this.src='/assets/images/logo/default.png'\" width=\"20\" height=\"20\"></a>"
  }
  
  def getLinkTag(domain: String): String = {
    "<a href=\"/search?ct=" + domain + "\" class=\"titDomain\">" + domain + "</a>"
  }

  /**
   * Download image
   */
//  def downloadLogo(secondDomain: String): String = {
//    val logoUrl = Configure.LOGO_API_URL + secondDomain
//    val path = Configure.LOGO_PATH + secondDomain + ".png"
//    val logo = "../extassets/" + secondDomain + ".png"
//    if (!FileUtil.isExist(path)) {
//      println("Download logo to " + path)
//      Try(HttpUtil.download(logoUrl, path, Configure.PROXY_HOST, Configure.PROXY_PORT))
//    }
//    if (FileUtil.isExist(path)) {
//      logo
//    } else Configure.LOGO_DEFAULT
//  }

  def getLogo(secondDomain: String, download: Boolean): String = {
    val logoUrl = Configure.LOGO_API_URL + secondDomain
    val path = Configure.LOGO_PATH + secondDomain + ".png"
    val logo = "/extassets/" + secondDomain + ".png"
    if (download) {
      if (!FileUtil.isExist(path)) {
        println("Download logo to " + path)
        Try(HttpUtil.download(logoUrl, path, Configure.PROXY_HOST, Configure.PROXY_PORT))
      }
    }
    if (FileUtil.isExist(path)) {
      logo
    } else {
      Configure.LOGO_DEFAULT
    }
  }

  /**
   * ********************************************************************************
   * ********************************************************************************
   * ********************************************************************************
   */

  def backgroupJob(f: => Unit, msg: String) {
    val thread = new Thread {
      override def run {
        val time0 = System.currentTimeMillis()
        println("Start " +  msg)
        //all.map(x => x.name).map(x => CommonService.getLogo(x, true))
        f
        val time1 = System.currentTimeMillis()
        println("End " +  msg + s" [${time1 -time0}]")
        
      }
    }
    thread.start()
  }
}