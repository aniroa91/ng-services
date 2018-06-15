package services.domain

//import com.ftel.bigdata.dns.parameters.Label
import com.sksamuel.elastic4s.http.search.SearchResponse

import model.MainDomainInfo
import model.MalwareInfo
import model.TotalInfo
import services.Configure
import model.ClientInfo
import com.ftel.bigdata.utils.StringUtil
import model.Label
import com.redis.RedisClient

abstract class AbstractService {

  protected val LABEL_FIELD = "label"
  protected val BLACK_VALUE = Label.Black
  protected val TOTAL_VALUE = "total"
  protected val NUM_QUERY_FIELD = "queries"
  protected val NUM_DOMAIN_FIELD = "domains"
  protected val NUM_SECOND_FIELD = "seconds"
  protected val NUM_IP_FIELD = "clients"
  protected val NUM_MALWARE_FIELD = "malwares"
  protected val DAY_FIELD = "day"
  protected val RANK_FIELD = "rank"
  protected val RANK_ALEXA_FIELD = "rankAlexa"

  protected val MALWARE_FIELD = "malware"
  protected val DOMAIN_FIELD = "domain"
  protected val SECOND_FIELD = "second"
  protected val CLIENT_FIELD = "client"
  protected val TIMESTAMP_FIELD = "timeStamp"

  val ES_INDEX = "dns-service-domain-"
  val ES_INDEX_ALL = ES_INDEX + "*"

  val SIZE_DAY = 30
  val MAX_SIZE_RETURN = 100

  
  val client = Configure.client // HttpClient(ElasticsearchClientUri(Configure.ES_HOST, Configure.ES_PORT))
  
  val redis: RedisClient = Configure.redis // HttpClient(ElasticsearchClientUri(Configure.ES_HOST, Configure.ES_PORT))
  
   /*******************************
    * 
    * REFACTOR
    * 
    * ****************************/
  def getTotalInfo(searchResponse: SearchResponse): Array[(String, TotalInfo)] = {
    searchResponse.hits.hits.map(x => getTotalInfo(x.sourceAsMap))
  }

  def getTotalInfo(map: Map[String, AnyRef]): (String, TotalInfo) = {
    val label = map.getOrElse("label", "").toString()
    val malwares = if (label == BLACK_VALUE) map.getOrElse(NUM_MALWARE_FIELD, "0").toString().toInt else 0
    label -> TotalInfo(
      getValueAsInt(map, NUM_QUERY_FIELD),
      getValueAsInt(map, NUM_DOMAIN_FIELD),
      getValueAsInt(map, NUM_IP_FIELD),
      malwares,
      getValueAsInt(map, "success"),
      getValueAsInt(map, "failed"),
      getValueAsInt(map, NUM_SECOND_FIELD))
  }
  
  def getTotalInfoDaily(searchResponse: SearchResponse): Array[(String, TotalInfo)] = {
    searchResponse.hits.hits.reverse.map(x => {
      val sourceAsMap = x.sourceAsMap
      getValueAsString(sourceAsMap, DAY_FIELD) -> TotalInfo(
        getValueAsInt(sourceAsMap, NUM_QUERY_FIELD),
        getValueAsInt(sourceAsMap, NUM_DOMAIN_FIELD),
        getValueAsInt(sourceAsMap, NUM_IP_FIELD),
        0, 0, 0, 0)
    })
  }
  
  def getMalwareInfo(searchResponse: SearchResponse): Array[MalwareInfo] = {
    searchResponse.hits.hits.map(x => {
      val sourceAsMap = x.sourceAsMap
      println()
      MalwareInfo(
          getValueAsString(sourceAsMap, MALWARE_FIELD),
          getValueAsInt(sourceAsMap, NUM_QUERY_FIELD),
          getValueAsInt(sourceAsMap, NUM_DOMAIN_FIELD),
          getValueAsInt(sourceAsMap, NUM_IP_FIELD))
    })
  }
  
  def getMalwareInfo2(searchResponse: SearchResponse): Array[MalwareInfo] = {
    val terms = getTerm(searchResponse, "top", Array("sum", "unique-second", "unique-client"))
    terms.map(x => 
      MalwareInfo(
        x._1,
        x._2.getOrElse("sum", 0L).toInt,
        x._2.getOrElse("unique-second", 0L).toInt,
        x._2.getOrElse("unique-client", 0L).toInt)
      ).sortWith((x,y) => x.queries > y.queries)
  }

  def getMainDomainInfo(searchResponse: SearchResponse): Array[MainDomainInfo] = {
    searchResponse.hits.hits.map(x => {
      val sourceAsMap = x.sourceAsMap
      new MainDomainInfo(
        getValueAsString(sourceAsMap, DAY_FIELD),
        getValueAsString(sourceAsMap, SECOND_FIELD),
        //getValueAsString(sourceAsMap, LABEL_FIELD),
        getValueAsString(sourceAsMap, MALWARE_FIELD),
        getValueAsInt(sourceAsMap, NUM_QUERY_FIELD),
        //getValueAsInt(sourceAsMap, NUM_DOMAIN_FIELD),
        getValueAsInt(sourceAsMap, NUM_IP_FIELD),
        getValueAsInt(sourceAsMap, RANK_FIELD),
        getValueAsInt(sourceAsMap, RANK_ALEXA_FIELD))
    })
  }

  def getMainDomainInfo2(searchResponse: SearchResponse): Array[MainDomainInfo] = {
    val mapLabel = searchResponse.hits.hits.map(x => x.sourceAsMap).map(x => x.getOrElse("second", "") -> x.getOrElse("label", "")).toMap
    val terms = getTerm(searchResponse, "top", "sum")
    terms.map(x => MainDomainInfo("day", x._1, mapLabel.getOrElse(x._1, "label").toString(), "malware", x._2.toLong, 1, 1, 1, 1))
      .sortWith((x,y) => x.queries > y.queries)
      .zipWithIndex.map { case (x,i) => MainDomainInfo(x.day, x.name, x.label, x.malware, x.queries, 1, 1, (i+1), 0)}
      
  }
  
  //success:26,633,777  rank:1 seconds:3,374,760 failed:1,575,321 domains:6,676,954 valid:6,747,003 day:2017-08-25 malwares:43 _id:AV4b7KCr2f2KIIB2YFFT _type:docs _index:dns-client-2017-08-25 _score:11.689
  // (day: String, client: String, queries: Int, seconds: Int, domains: Int, success: Int, failed: Int, malwares: Int, valid: Int, rank: Int)
  def getClientInfo(searchResponse: SearchResponse, valid: Int): Array[ClientInfo] = {
    searchResponse.hits.hits.map(x => {
      val sourceAsMap = x.sourceAsMap
      ClientInfo(
        getValueAsString(sourceAsMap, DAY_FIELD),
        getValueAsString(sourceAsMap, "client"),
        getValueAsInt(sourceAsMap, NUM_QUERY_FIELD),
        getValueAsInt(sourceAsMap, NUM_SECOND_FIELD),
        getValueAsInt(sourceAsMap, NUM_DOMAIN_FIELD),
        getValueAsInt(sourceAsMap, "success"),
        getValueAsInt(sourceAsMap, "failed"),
        getValueAsInt(sourceAsMap, NUM_MALWARE_FIELD),
        valid,
        getValueAsInt(sourceAsMap, "rank"))
    })
  }
  
  def getValueAsString(map: Map[String, Any], key: String, default: String): String = {
    val result = map.getOrElse(key, default)
    if (result != null) result.toString() else default
  }
  
  def getValueAsString(map: Map[String, Any], key: String): String = {
    getValueAsString(map, key, "")
  }
  
  def getValueAsInt(map: Map[String, Any], key: String): Int = {
    getValueAsString(map, key, "0").toInt
  }

  def getValueAsLong(map: Map[String, Any], key: String): Long = {
    getValueAsString(map, key, "0").toLong
  }

  def getValueAsDouble(map: Map[String, Any], key: String): Double = {
    getValueAsString(map, key, "0").toDouble
  }
  
  def sumTotalInfo(totalInfoArrs: Array[TotalInfo]): TotalInfo = {
    totalInfoArrs.reduce((x, y) => {
      TotalInfo(
          x.queries  + y.queries,
          x.domains  + y.domains,
          x.clients  + y.clients,
          x.malwares + y.malwares,
          x.success  + y.success,
          x.failed   + y.failed,
          x.seconds  + y.seconds)
    })
  }
  
  def printTime(times: Long*) {
    val size = times.size
    if (size > 1) {
      for (i <- 0 until size-1) {
        println(s"Time[$i] - Time[${i + 1}]: " + (times(i + 1) - times(i)))
      }
    }
  }
  
  def getTerm(response: SearchResponse, nameTerm: String, nameSubTerm: String): Array[(String, Long)] = {
    if (response.aggregations != null) {
    response.aggregations
      .getOrElse(nameTerm, Map[String, AnyRef]()).asInstanceOf[Map[String, AnyRef]]
      .getOrElse("buckets", List).asInstanceOf[List[AnyRef]]
      .map(x => x.asInstanceOf[Map[String, AnyRef]])
      .map(x => x.getOrElse("key", "key").toString() -> x.getOrElse(nameSubTerm, Map[String, AnyRef]()).asInstanceOf[Map[String, AnyRef]])
      .map(x => x._1 -> x._2.get("value").getOrElse("0").asInstanceOf[Double])
      .map(x => x._1 -> x._2.toLong).sorted
      .toArray
    } else {
      Array[(String, Long)]()
    }
  }

  def getTerm(response: SearchResponse, nameTerm: String, nameSubTerms: Array[String]): Array[(String, Map[String, Long])] = {
    if (response.aggregations != null) {
    response.aggregations
      .getOrElse(nameTerm, Map[String, AnyRef]()).asInstanceOf[Map[String, AnyRef]]
      .getOrElse("buckets", List).asInstanceOf[List[AnyRef]]
      .map(x => x.asInstanceOf[Map[String, AnyRef]])
      .map(x => {
        val key = x.getOrElse("key", "key").toString()
        val value = nameSubTerms
          .map(y => y -> x.getOrElse(y, Map[String, AnyRef]()).asInstanceOf[Map[String, AnyRef]])
          .map(y => y._1 -> y._2.get("value").getOrElse("0").toString().toDouble)
          .map(y => y._1 -> y._2.toLong)
          .toMap
        key -> value
        })
      .toArray
    } else {
      Array[(String, Map[String, Long])]()
    }
  }
}