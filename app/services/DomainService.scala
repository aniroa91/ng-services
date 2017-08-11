///*
// * package services
//
//
//import scala.concurrent.Await
//import scala.concurrent.duration.Duration
//
//import org.elasticsearch.search.sort.SortOrder
//
//import com.ftel.bigdata.utils.HttpUtil
//import com.sksamuel.elastic4s.ElasticsearchClientUri
//import com.sksamuel.elastic4s.http.ElasticDsl._
//import com.sksamuel.elastic4s.http.HttpClient
//import com.sksamuel.elastic4s.http.search.SearchHit
//import com.sksamuel.elastic4s.http.search.SearchResponse
//import com.typesafe.config.Config
//import com.typesafe.config.ConfigFactory
//
//import slick.driver.PostgresDriver.api._
//import slick.lifted.Query
//import slick.lifted.TableQuery
//import utils.SearchReponseUtil
//import com.ftel.bigdata.dns.model.table.MalwareTable
////import com.ftel.bigdata.dns.model.table.WhoisObject
//import com.ftel.bigdata.dns.model.table.LabelTable
//import com.ftel.bigdata.dns.model.table.ServerNameTable
//import com.ftel.bigdata.dns.model.table.DomainTable
//import com.ftel.bigdata.dns.model.table.DomainServerNameTable
//import com.ftel.bigdata.dns.model.table.RegistrarTable
//import com.ftel.bigdata.dns.model.table.StatusTable
//import model.BasicInfo
//import model.Response
//import com.ftel.bigdata.dns.parameters.Label
//import com.ftel.bigdata.utils.FileUtil
//import model.StatsResponse
////import model.LabelResponse
////import model.MalwareResponse
////import model.DomainResponse
//import com.ftel.bigdata.utils.DateTimeUtil
////import model.DailyResponse
//import model.DashboardResponse
////import model.SecondResponse
//import com.ftel.bigdata.utils.WhoisUtil
//import com.ftel.bigdata.whois.Whois
//import scala.util.Try
//
////import com.ftel.bigdata.dns.utils.WhoisParserUtil
//
//
//object DomainService {
//
//  val ES_INDEX = "dns-service-domain-"
//  val ES_INDEX_ALL = ES_INDEX + "*"
//  
//  val SIZE_DAY = 30
//  private val MAX_SIZE_RETURN = 100
////
////  val client = HttpClient(ElasticsearchClientUri(Configure.ES_HOST, Configure.ES_PORT))
////  //val db = DatabaseConfig.forConfig[JdbcProfile]("slick.dbs.default").db
////  
////  def close() = client.close()
////
////  def getDomainInfo(domain: String): Response = {
////    val multiSearchResponse = client.execute(
////      multi(
////        search(ES_INDEX_ALL / "second") query { must(termQuery("second", domain)) } sortBy { fieldSort("day") order SortOrder.DESC } limit SIZE_DAY,
////        search(ES_INDEX_ALL / "answer") query { must(termQuery("second", domain)) } limit 1000,
////        search(ES_INDEX_ALL / "domain") query { must(termQuery("second", domain)) } aggregations (
////          cardinalityAgg("num_of_domain", "domain")),
////        search((ES_INDEX + "whois") / "whois") query { must(termQuery("domain", domain)) })).await
////    val secondResponse = multiSearchResponse.responses(0)
////    val answerResponse = multiSearchResponse.responses(1)
////    val domainResponse = multiSearchResponse.responses(2)
////    val whoisResponse = multiSearchResponse.responses(3)
////    if (secondResponse.totalHits > 0) {
////      def searchHit2BasicInfo = (x: SearchHit) => {
////        val map = x.sourceAsMap
////        val numOfClient = map.getOrElse("number_of_ip", "0").toString().toInt
////        val second = map.getOrElse("second", "").toString()
////        val rankAlexa = map.getOrElse("rank_alexa", "0").toString().toInt
////        val rankFtel = map.getOrElse("rank_ftel", "0").toString().toInt
////        val numOfQuery = map.getOrElse("number_of_record", "0").toString().toInt
////        val malware = map.getOrElse("malware", "").toString()
////        val day = map.getOrElse("day", "").toString()
////        new BasicInfo(day, numOfQuery, numOfClient, malware, rankFtel, rankAlexa)
////      }
////      
////      //println("==============")
////      val history = secondResponse.hits.hits.map(searchHit2BasicInfo)
////      val current = secondResponse.hits.hits.head
////      val basicInfo = searchHit2BasicInfo(current)
////
////      //val whois = getWhoisInfo(db, domain)
////      val whois = getWhoisInfo(whoisResponse, domain, basicInfo.label, basicInfo.malware)
////      //println(whois)
////
////      val answers = answerResponse.hits.hits.map(x => x.sourceAsMap.getOrElse("answer", "").toString()).filter(x => x != "")
////      Response(whois, basicInfo, answers, history, SearchReponseUtil.getCardinality(domainResponse, "num_of_domain"))
////    } else null
////  }
////  
////  
/////*
////  def getWhoisInfo(db: Database, domain: String): Whois = {
////    val domains = TableQuery[DomainTable]
////    val domainServerName = TableQuery[DomainServerNameTable]
////    val label = TableQuery[LabelTable]
////    val malware = TableQuery[MalwareTable]
////    val registrar = TableQuery[RegistrarTable]
////    val serverName = TableQuery[ServerNameTable]
////    val status = TableQuery[StatusTable]
////    
////    val q = for {
////      d <- domains if d.name === domain
////      ds <- domainServerName if d.id === ds.domainId
////      l <- label if d.labelId === l.id
////      m <- malware if d.malwareId === m.id
////      r <- registrar if d.registrarId === r.id
////      s <- serverName if ds.serverId === s.id
////      st <- status if d.statusId === st.id
////    } yield (d.name, r.name, r.server, r.url, s.name, st.name, d.create, d.update, d.expire, l.name, m.name)
////    val result = Await.result(db.run(q.result), Duration.Inf)
////    val servers = result.map(x => x._5).toArray
////    val head = result.head
////    val whois = Whois(head._1, head._2, head._3, head._4, servers, head._6, head._8.toString().split(" ")(0), head._7.toString().split(" ")(0), head._9.toString(), head._10, head._11)
////    whois
////  }
////*/
////  
////  def getWhoisInfo(whoisResponse: SearchResponse, domain: String, label: String, malware: String): Whois = {
////    //println(whoisResponse.totalHits)
////    if (whoisResponse.totalHits > 0) {
////      
////      val map = whoisResponse.hits.hits.head.sourceAsMap
////      //println(map)
////      val whois = Whois(
////        map.getOrElse("domain", "").toString(),
////        map.getOrElse("registrar", "").toString(),
////        map.getOrElse("whoisServer", "").toString(),
////        map.getOrElse("referral", "").toString(),
////        map.getOrElse("servername", "").toString().split(" "),
////        map.getOrElse("status", "").toString(),
////        map.getOrElse("update", "").toString(),
////        map.getOrElse("create", "").toString(),
////        map.getOrElse("expire", "").toString(),
////        map.getOrElse("label", "").toString(),
////        map.getOrElse("malware", "").toString())
////      whois
////    } else {
////      
////      getWhoisFromWeb(domain, label, malware)
////    }
////  }
////
////  def formatNumber(number: Int): String = {
////    val formatter = java.text.NumberFormat.getIntegerInstance
////    formatter.format(number)
////  }
////  
////  def percent(number: Int, prev: Int): Double = {
////    val value = ((number - prev) / (prev * 1.0)) / 100.0
////    BigDecimal(value).setScale(3, BigDecimal.RoundingMode.HALF_UP).toDouble
////  }
////
////  def getTopRank(from: Int, day: String): Array[(String, BasicInfo)] = {
////    val response = client.execute(
////        search(ES_INDEX_ALL / "second") query {
////          boolQuery().must(rangeQuery("rank_ftel").gt(from-1).lt(MAX_SIZE_RETURN),termQuery("day", day))
////          } sortBy { fieldSort("rank_ftel") 
////        } limit MAX_SIZE_RETURN
////    ).await
////    convert(response)
////  }
////
////  def getWhoisFromWeb(domain: String, label: String, malware: String): Whois = {
////    val esIndex = s"dns-service-domain-whois"
////    val esType = "whois"
////    val whois = WhoisUtil.whoisService(domain, label, malware, "172.30.45.220", 80)
////    if (whois.isValid()) {
////      indexWhois(esIndex, esType, whois)
////      whois
////    } else new Whois()
////  }
////
////  def getTopByNumOfQuery(day: String, label: String): Array[(String, BasicInfo)] = {
////    val malware = if (label == Label.White) Label.None else if (label == Label.Unknow) "null" else ???
////    val response = client.execute (
////        search(ES_INDEX_ALL / "second") query {
////          boolQuery().must(termQuery("day", day), termQuery("malware", malware))
////        }  sortBy {
////          fieldSort("number_of_record") order(SortOrder.DESC)
////        } limit MAX_SIZE_RETURN).await
////    convert(response)
////  }
////  
////  def getTopBlackByNumOfQuery(day: String): Array[(String, BasicInfo)] = {
////    val response = client.execute (
////        search(ES_INDEX_ALL / "second") query {
////          boolQuery()
////            .must(termQuery("day", day))
////            .not(termQuery("malware", "none"),termQuery("malware", "null"))
////        }  sortBy {
////          fieldSort("number_of_record") order(SortOrder.DESC)
////        } limit MAX_SIZE_RETURN).await
////    convert(response)
////  }
////  
////  private def convert(response: SearchResponse): Array[(String, BasicInfo)] = {
////    response.hits.hits.map(x => {
////      val map = x.sourceAsMap
////      val day: String = map.getOrElse("day", "").toString()
////      val numOfQuery: Int = map.getOrElse("number_of_record", "0").toString().toInt
////      val numOfClient: Int = map.getOrElse("number_of_ip", "0").toString().toInt
////      val malware: String = map.getOrElse("malware", "null").toString()
////      val label: String = Label.getLabelFrom(malware)
////      val rankFtel: Int = map.getOrElse("rank_ftel", "0").toString().toInt
////      val rankAlexa: Int = map.getOrElse("rank_alexa", "0").toString().toInt
////      val domain = map.getOrElse("second", "").toString()
////      domain -> new BasicInfo(day, numOfQuery, numOfClient, malware, rankFtel, rankAlexa)
////    })
////  }
////
////  def getLatestDay(): String = {
////    val response = client.execute(
////        search(ES_INDEX_ALL / "second") sortBy { fieldSort("day") order SortOrder.DESC } limit 1
////    ).await
////    response.hits.hits.head.sourceAsMap.getOrElse("day", "").toString()
////  }
////  
////  private val URL_DOMAIN_DEFAULT = "../assets/images/logo/domain.png"
////  val STORAGE_PATH = ConfigFactory.load().getString("storage") + "/"
////  private val LOGO_URL = "https://logo.clearbit.com/"
////  def getLogoPath(secondDomain: String): String = {
////    val logoUrl = LOGO_URL + secondDomain
////    val path = STORAGE_PATH + secondDomain + ".png"
////    val logo = "../extassets/" + secondDomain + ".png"
////    if (!FileUtil.isExist(path)) {
////      println("Don't exist " + path)
////      Try(HttpUtil.download(logoUrl, path, "172.30.45.220", 80))
////    }
////    if (FileUtil.isExist(path)) {
////      logo
////    } else URL_DOMAIN_DEFAULT
////  }
////  
////  def getImgTag(domain: String): String = {
////    val logo = getLogoPath(domain)
////    "<a href=\"/search?value=" + domain + "\"><img src=\"" + logo + "\" width=\"30\" height=\"30\"></a>"
////  }
////  
////  def getLinkTag(domain: String): String = {
////    "<a href=\"/search?value=" + domain + "\">" + domain + "</a>"
////  }
////  //  def toArray(pair: (String, BasicInfo)): Array[String] = {
////  //    val info = pair._2
////  //    Array(info.rankFtel, pair._1, 
////  //  }
////
////  def getStatsByDay(day: String): StatsResponse = {
////    val prev = getPreviousDay(day)
////
////    val multiSearchResponse = client.execute(
////      multi( // label:black AND _type:top
////        search(s"dns-stats-${day}" / "count") query { not(termQuery("label", "total")) },
////        search(s"dns-stats-${day}" / "malware") query { must(termQuery("label", "black")) } sortBy { fieldSort("number_of_record") order SortOrder.DESC } limit 100,
////        search(s"dns-stats-${day}" / "top") query { must(termQuery("label", "black")) } sortBy { fieldSort("number_of_record") order SortOrder.DESC } limit 1000,
////        search(s"dns-stats-${prev}" / "count") query { not(termQuery("label", "total")) },
////        search(s"dns-overview-*" / "overview") sortBy { fieldSort("day") order SortOrder.DESC } limit 30
////
////        //search(ES_INDEX_ALL / "domain") query { must(termQuery("second", domain)) } aggregations (
////        //  cardinalityAgg("num_of_domain", "domain")),
////        //search((ES_INDEX + "whois") / "whois") query { must(termQuery("domain", domain)) }
////        )).await
////    val labelResponse = multiSearchResponse.responses(0)
////    val malwareResponse = multiSearchResponse.responses(1)
////    val domainResponse = multiSearchResponse.responses(2)
////    val labelResponsePrev = multiSearchResponse.responses(3)
////    val dailyResponse = multiSearchResponse.responses(4)
////    
//////    println("=========")
//////    println(labelResponse.hits)
//////    println("=========")
////    if (labelResponse.hits == null) {
////      getStatsByDay(getLatestDay())
////    } else {
////      val arrayLabelResponse = labelResponse.hits.hits.map(x => {
////        val map = x.sourceAsMap
////        val label = map.getOrElse("label", "").toString()
////        val malwares = if (label == Label.Black) map.getOrElse("number_of_malware", "0").toString().toInt else 0
////        LabelResponse(
////          label,
////          map.getOrElse("number_of_record", "0").toString().toInt,
////          map.getOrElse("number_of_domain", "0").toString().toInt,
////          map.getOrElse("number_of_ip", "0").toString().toInt,
////          malwares,
////          map.getOrElse("success", "0").toString().toInt,
////          map.getOrElse("failed", "0").toString().toInt,
////          map.getOrElse("number_of_second_domain", "0").toString().toInt)
////      })
////
////      val arrayMalwareResponse = malwareResponse.hits.hits.map(x => {
////        val map = x.sourceAsMap
////        MalwareResponse(
////          map.getOrElse("malware", "").toString(),
////          map.getOrElse("number_of_record", "0").toString().toInt,
////          map.getOrElse("number_of_domain", "0").toString().toInt,
////          map.getOrElse("number_of_ip", "0").toString().toInt)
////      })
////
////      val arrayDomainResponse = domainResponse.hits.hits.map(x => {
////        val map = x.sourceAsMap
////        //println(map)
////        DomainResponse(
////          map.getOrElse("domain", "").toString(),
////          map.getOrElse("malware", "0").toString(),
////          map.getOrElse("number_of_record", "0").toString().toInt,
////          map.getOrElse("number_of_ip", "0").toString().toInt)
////      })
////
////      val total = getTotalFrom(labelResponse)
////      val totalPrev = getTotalFrom(labelResponsePrev)
////
////      val daily = dailyResponse.hits.hits.reverse.map(x => {
////        val map = x.sourceAsMap
////         DailyResponse(
////          map.getOrElse("day", "").toString(),
////          map.getOrElse("number_of_record", "0").toString().toInt,
////          map.getOrElse("number_of_domain", "0").toString().toInt,
////          map.getOrElse("number_of_ip", "0").toString().toInt)
////      })
////      
////      val secondBlack = getTopBlackByNumOfQuery(day)
////      val arraySecondBlackResponse = secondBlack.map(x => SecondResponse(x._1, x._2.label, x._2.malware, x._2.numOfQuery, 0, x._2.numOfClient))
////      val second = getTopRank(1, day)
////      val arraySecondResponse = second.map(x => SecondResponse(x._1, x._2.label, x._2.malware, x._2.numOfQuery, 0, x._2.numOfClient))
////      StatsResponse(day, total, totalPrev, arrayLabelResponse, arrayMalwareResponse, arrayDomainResponse)
////    }
////  }
////  
////  def getDashboard(day: String): DashboardResponse = {
////    val prev = getPreviousDay(day)
////
////    val multiSearchResponse = client.execute(
////      multi( // label:black AND _type:top
////        search(s"dns-stats-${day}" / "count") query { not(termQuery("label", "total")) },
////        search(s"dns-stats-${day}" / "malware") query { must(termQuery("label", "black")) } sortBy { fieldSort("number_of_record") order SortOrder.DESC } limit 100,
////        search(s"dns-stats-${day}" / "top") query { must(termQuery("label", "black")) } sortBy { fieldSort("number_of_record") order SortOrder.DESC } limit 1000,
////        search(s"dns-stats-${prev}" / "count") query { not(termQuery("label", "total")) },
////        search(s"dns-overview-*" / "overview") sortBy { fieldSort("day") order SortOrder.DESC } limit 30
////
////        //search(ES_INDEX_ALL / "domain") query { must(termQuery("second", domain)) } aggregations (
////        //  cardinalityAgg("num_of_domain", "domain")),
////        //search((ES_INDEX + "whois") / "whois") query { must(termQuery("domain", domain)) }
////        )).await
////    val labelResponse = multiSearchResponse.responses(0)
////    val malwareResponse = multiSearchResponse.responses(1)
////    val domainResponse = multiSearchResponse.responses(2)
////    val labelResponsePrev = multiSearchResponse.responses(3)
////    val dailyResponse = multiSearchResponse.responses(4)
////    
//////    println("=========")
//////    println(labelResponse.hits)
//////    println("=========")
////    if (labelResponse.hits == null) {
////      getDashboard(getLatestDay())
////    } else {
////      val arrayLabelResponse = labelResponse.hits.hits.map(x => {
////        val map = x.sourceAsMap
////        val label = map.getOrElse("label", "").toString()
////        val malwares = if (label == Label.Black) map.getOrElse("number_of_malware", "0").toString().toInt else 0
////        LabelResponse(
////          label,
////          map.getOrElse("number_of_record", "0").toString().toInt,
////          map.getOrElse("number_of_domain", "0").toString().toInt,
////          map.getOrElse("number_of_ip", "0").toString().toInt,
////          malwares,
////          map.getOrElse("success", "0").toString().toInt,
////          map.getOrElse("failed", "0").toString().toInt,
////          map.getOrElse("number_of_second_domain", "0").toString().toInt)
////      })
////
////      val arrayMalwareResponse = malwareResponse.hits.hits.map(x => {
////        val map = x.sourceAsMap
////        MalwareResponse(
////          map.getOrElse("malware", "").toString(),
////          map.getOrElse("number_of_record", "0").toString().toInt,
////          map.getOrElse("number_of_domain", "0").toString().toInt,
////          map.getOrElse("number_of_ip", "0").toString().toInt)
////      })
////
////      val arrayDomainResponse = domainResponse.hits.hits.map(x => {
////        val map = x.sourceAsMap
////        //println(map)
////        DomainResponse(
////          map.getOrElse("domain", "").toString(),
////          map.getOrElse("malware", "0").toString(),
////          map.getOrElse("number_of_record", "0").toString().toInt,
////          map.getOrElse("number_of_ip", "0").toString().toInt)
////      })
////
////      val total = getTotalFrom(labelResponse)
////      val totalPrev = getTotalFrom(labelResponsePrev)
////
////      val daily = dailyResponse.hits.hits.reverse.map(x => {
////        val map = x.sourceAsMap
////         DailyResponse(
////          map.getOrElse("day", "").toString(),
////          map.getOrElse("number_of_record", "0").toString().toInt,
////          map.getOrElse("number_of_domain", "0").toString().toInt,
////          map.getOrElse("number_of_ip", "0").toString().toInt)
////      })
////      
////      val secondBlack = getTopBlackByNumOfQuery(day)
////      val arraySecondBlackResponse = secondBlack.map(x => SecondResponse(x._1, x._2.label, x._2.malware, x._2.numOfQuery, 0, x._2.numOfClient))
////      val second = getTopRank(1, day)
////      val arraySecondResponse = second.map(x => SecondResponse(x._1, x._2.label, x._2.malware, x._2.numOfQuery, 0, x._2.numOfClient))
////      DashboardResponse(day, total, totalPrev, arrayLabelResponse, arrayMalwareResponse, arraySecondBlackResponse, arraySecondResponse, daily)
////    }
////  }
////  
////  private def getTotalFrom(response: SearchResponse): LabelResponse = {
////    val arrayLabelResponse = response.hits.hits.map(x => {
////      val map = x.sourceAsMap
////      val label = map.getOrElse("label", "").toString()
////      val malwares = if (label == Label.Black) map.getOrElse("number_of_malware", "0").toString().toInt else 0
////      LabelResponse(
////          label,
////          map.getOrElse("number_of_record", "0").toString().toInt,
////          map.getOrElse("number_of_domain", "0").toString().toInt,
////          map.getOrElse("number_of_ip", "0").toString().toInt,
////          malwares,
////          map.getOrElse("success", "0").toString().toInt,
////          map.getOrElse("failed", "0").toString().toInt,
////          map.getOrElse("number_of_second_domain", "0").toString().toInt)
////    })
////    arrayLabelResponse.reduce((x,y) => LabelResponse("total",
////        x.queries + y.queries,
////        x.domains + y.domains,
////        x.clients + y.clients,
////        x.malwares + y.malwares,
////        x.success + y.success,
////        x.failed + y.failed,
////        x.seconds + y.seconds))
////    
////  }
////  
////  def getPreviousDay(day: String): String = {
////    val prev = DateTimeUtil.create(day, DateTimeUtil.YMD)
////    prev.minusDays(1).toString(DateTimeUtil.YMD)
////  }
////  
////  
////  def indexWhois(esIndex: String, esType: String, whois: Whois) {
////    // indexInto("bands" / "artists") doc Artist("Coldplay") refresh(RefreshPolicy.IMMEDIATE)
////    // domain:bmwsociety.com servername:ns18.worldnic.com ns17.worldnic.com label:white create:2002-01-11 referral:networksolutions.com registrar:network solutions, llc. expire:2020-01-11 update:2014-11-12 malware:none _id:bmwsociety.com 
////    client.execute(
////      indexInto(esIndex / esType) fields (
////        "domain" -> whois.domainName,
////        "registrar" -> whois.registrar,
////        "whoisServer" -> whois.whoisServer,
////        "referral" -> whois.referral,
////        "servername" -> whois.nameServer.mkString(" "),
////        "status" -> whois.status,
////        "create" -> whois.create.substring(0, 10),
////        "update" -> whois.update.substring(0, 10),
////        "expire" -> (if (whois.expire.isEmpty()) "2999-12-31" else whois.expire.substring(0, 10)),
////        "label" -> whois.label,
////        "malware" -> whois.malware)
////    id whois.domainName).await
////  }
////  
////  
////  def main(args: Array[String]) {
////    //DomainService.getDomainInfo("google.com")
////    //val db = DatabaseConfig.forConfig[JdbcProfile]("slick.dbs.default").db
////    //val whoisInfo = getWhoisInfo(db, "google.com")
////    
////    //println(whoisInfo)
////    //HttpUtil.download("https://logo.clearbit.com/" + "google.com", "public/images/" + "google.com" + ".png", "172.30.45.220", 80)
////    
//////    getTopDomain()
////    val latest = getLatestDay()
////    //println(latest)
////    //getTopRank(1, 1000, latest)
//////    getTopBlackByNumOfQuery(latest).foreach(x => println(x._1 -> x._2.day))
//////    getTopByNumOfQuery(latest, Label.White).take(100).foreach(println)
////    //getStatsByDay(latest)
////    val prev = getPreviousDay(latest)
////    
//////    println(prev)
////    println(getWhoisFromWeb("activum.nu", null, null))
////    close()
////  }
////  
//  
//}
//
//*/