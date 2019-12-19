package services.domain

import com.sksamuel.elastic4s.http.search.SearchResponse
import services.Configure

abstract class AbstractService {

  val client = Configure.client // HttpClient(ElasticsearchClientUri(Configure.ES_HOST, Configure.ES_PORT))
  
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

  def getAggregationsKeyString(searchResponse: SearchResponse, name: String, subName: String): Array[(String, String, Long)] = {
    val buckets    = getBuckets(searchResponse.aggregations, name)
    buckets.flatMap(x => {
      val key     = x.getOrElse("key", "NA").toString()
      val array   = getBuckets(x, subName)
      array.map(y => {
        val key2  = y.getOrElse("key_as_string", "NA").toString()
        val count = y.getOrElse("doc_count", 0).toString().toLong
        (key, key2, count)
      })
    }).toArray
  }

  def getTerm1(searchResponse: SearchResponse, name: String, subName: String): Array[(String, String, Long)] = {
    val buckets    = getBuckets(searchResponse.aggregations, name)
    //def subBuckets = getBuckets(searchResponse, name)
    buckets.flatMap(x => {
      val key     = x.getOrElse("key", "NA").toString()
      val array   = getBuckets(x, subName)
      array.map(y => {
        val key2  = y.getOrElse("key", "NA").toString()
        val count = y.getOrElse("doc_count", 0).toString().toLong
        (key, key2, count)
      })
    }).toArray
  }

  def getBuckets(map: Map[String, AnyRef], name: String): List[Map[String, AnyRef]] = {
    map.getOrElse(name, Map[String, AnyRef]()).asInstanceOf[Map[String, AnyRef]]
      .getOrElse("buckets", List).asInstanceOf[List[AnyRef]]
      .map(x => x.asInstanceOf[Map[String, AnyRef]])
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