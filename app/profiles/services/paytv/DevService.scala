package service

import scala.collection.immutable.Map

import com.sksamuel.elastic4s.ElasticsearchClientUri
import com.sksamuel.elastic4s.TcpClient
import com.sksamuel.elastic4s.http.ElasticDsl.RichFuture
import com.sksamuel.elastic4s.http.ElasticDsl.RichString
import com.sksamuel.elastic4s.http.ElasticDsl.SearchHttpExecutable
import com.sksamuel.elastic4s.http.ElasticDsl.SearchShow
import com.sksamuel.elastic4s.http.ElasticDsl.percentilesAggregation
import com.sksamuel.elastic4s.http.ElasticDsl.rangeAggregation
import com.sksamuel.elastic4s.http.ElasticDsl.search
import com.sksamuel.elastic4s.http.ElasticDsl.termsAgg
import com.sksamuel.elastic4s.http.ElasticDsl.termsAggregation
import com.sksamuel.elastic4s.http.search.SearchResponse
import com.sksamuel.elastic4s.http.ElasticDsl._

import play.api.libs.json.JsArray
import play.api.libs.json.JsNumber
import play.api.libs.json.JsObject
import play.api.libs.json.JsString
import play.api.libs.json.JsValue
import play.api.libs.json.Json
import profile.model.PayTVResponse
import profile.utils.CommonUtil
import profile.utils.ProvinceUtil
import scalaj.http.Http
import services.Configure

case class Bubble(x: Double, y: Double, z: Double, name: String)

object DevService {

  val client = Configure.client

  //def getStatus(): Array[(String, String, Int)] = getStatus("*")

  def getMonth() = {
    val request = search(s"profile-paytv-*" / "docs") aggregations (
      termsAggregation("month")
        .field("month")
        .size(1000)
      )
    val reponse = client.execute(request).await
    reponse.aggregations
      .getOrElse("month", Map[String, AnyRef]()).asInstanceOf[Map[String, AnyRef]]
      .getOrElse("buckets", List).asInstanceOf[List[AnyRef]]
      .map(x => x.asInstanceOf[Map[String, AnyRef]])
      .map(x => x.get("key").get.toString() -> x.get("doc_count").get.toString().toInt)
      .toArray
      .sortBy(x => x._1)
    //.foreach(println)
  }

  def getStatus(queryString: String) = {
    val request = search(s"profile-paytv-*" / "docs") query(queryString) aggregations (
      termsAggregation("month")
        .field("month")
        .subaggs(
          termsAgg("status", "StatusCode")
        ) size 1000
      )
    val reponse = client.execute(request).await
    getBucketTerm(reponse, "month", "status")
      .map(x => (x._1, CommonUtil.getStatus(x._2.toInt), x._3))
  }

  def getTenGoi(queryString: String) = {
    val request = search(s"profile-paytv-*" / "docs") query(queryString) aggregations (
      termsAggregation("month")
        .field("month")
        .subaggs(
          termsAgg("TenGoi", "TenGoi") size 12
        ) size 1000
      )
    val reponse = client.execute(request).await
    getBucketTerm(reponse, "month", "TenGoi")
  }

  def getUsage(queryString: String) = {
    val request = search(s"profile-paytv-*" / "docs") query(queryString) aggregations (
      termsAggregation("month")
        .field("month")
        .subaggs(
          rangeAggregation("usage")
            .field("SumUsage")
            .range("no_usage", Double.MinValue, 0.0001)
            .range("usage", 0.0001, Double.MaxValue)
        ) size 1000
      )
    val reponse = client.execute(request).await
    getBucketTerm(reponse, "month", "usage")

  }

  def getProvince(queryString: String) = {
    val request = search(s"profile-paytv-*" / "docs") query(queryString) aggregations (
      termsAggregation("province")
        .field("ProvinceCode")
        .subaggs(
          termsAgg("month", "month") size 20
        ) size 5
      )
    val reponse = client.execute(request).await
    getBucketTerm(reponse, "province", "month").map(x => (x._2, ProvinceUtil.getProvince(x._1.toInt), x._3) )
  }

  private def getBucketTerm(response: SearchResponse, nameTerm: String, nameSubTerm: String): Array[(String, String, Int)] = {
    if (response.aggregations != null) {
      response.aggregations
        .getOrElse(nameTerm, Map[String, AnyRef]()).asInstanceOf[Map[String, AnyRef]]
        .getOrElse("buckets", List).asInstanceOf[List[AnyRef]]
        .map(x => x.asInstanceOf[Map[String, AnyRef]])
        .flatMap(x => {
          val key = x.getOrElse("key", "key").toString()
          val sub = x.getOrElse(nameSubTerm, Map[String, AnyRef]()).asInstanceOf[Map[String, AnyRef]]
            .getOrElse("buckets", List).asInstanceOf[List[AnyRef]]
            .map(y => y.asInstanceOf[Map[String, AnyRef]])
          sub.map(y => {
            (
              key,
              y.getOrElse("key", "key").toString(),
              y.getOrElse("doc_count", "0").toString().toInt)
            //y.getOrElse(nameSubTerm, Map[String, AnyRef]()).asInstanceOf[Map[String, AnyRef]].getOrElse("value", "0").toString().toDouble)
          })

        }).toArray
    } else {
      Array[(String, String, Int)]()
    }
  }

  def seriesForLineChart(array: Array[(String, String, Int)]): JsValue = {
    seriesForLineChart(array, "spline")
  }

  def seriesForLineChart(array: Array[(String, String, Int)], typeChart: String): JsValue = {
    val group = array.groupBy(x => x._2).map(x => x._1 -> x._2.map(y => (y._1, y._3)).sortBy(y => y._1) )
    val a = group.map(x => JsObject(Seq(
      "name" -> JsString(x._1),
      "type" -> JsString(typeChart),
      "data" -> JsArray(x._2.map(y => JsNumber(y._2)))
    )))
    Json.toJson(a)
  }

  def get(): PayTVResponse = get("*")

  def get(query: String): PayTVResponse = {
    val months = getMonth().map(x => x._1)

    val rs = PayTVResponse(getStatus(query), getTenGoi(query), getUsage(query), getProvince(query), getQuantile("SumUsage", query), getQuantile("LifeToEndC", query))
    //rs.usage.foreach(println)
    rs.normalize(months)
  }

  def getQuantile(field: String, queryString: String)  = {
    val query = s"""
        {
            "query": {
              "query_string": {
                "query": "${queryString.replace("\"", "\\\"")}"
              }
            },
            "size": 0,
            "aggs" : {
                "month" : {
                    "terms" : {
                        "field" : "month",
                        "size": 100
                    }, "aggs": {
                      "quantile" : {
                        "percentiles" : {
                          "field" : "${field}",
                          "percents": [
                            5,
                            25,
                            50,
                            75,
                            99
                          ]
                        }
                      }
                   }
                }
            }
        }
        """
    //println(query)
    val body = Http("http://172.27.11.156:9200/profile-paytv-*/docs/_search")
      .postData(query)
      .asString.body
    val json = Json.parse(body)
    val array = json.\("aggregations").\("month").\("buckets").get.asInstanceOf[JsArray].value.toArray
    array.map(x => (x.\("key").get.asInstanceOf[JsString], x.\("doc_count").get.asInstanceOf[JsNumber], x.\("quantile").get.\("values").get))
      .map(x => x._1.value -> JsArray(IndexedSeq(
        x._3.\("5.0").get.asInstanceOf[JsNumber],
        x._3.\("25.0").get.asInstanceOf[JsNumber],
        x._3.\("50.0").get.asInstanceOf[JsNumber],
        x._3.\("75.0").get.asInstanceOf[JsNumber],
        x._3.\("99.0").get.asInstanceOf[JsNumber])))
      .sortBy(x => x._1)
  }

  //  def getQuantileLifeToEndC()  = {
  //    val body = Http("http://172.27.11.156:9200/profile-paytv-*/docs/_search")
  //      .postData("""
  //
  //{
  //    "size": 0,
  //    "aggs" : {
  //        "month" : {
  //            "terms" : {
  //                "field" : "month",
  //                "size": 100
  //            }, "aggs": {
  //              "quantile" : {
  //                "percentiles" : {
  //                  "field" : "LifeToEndC",
  //                  "percents": [
  //                    5,
  //                    25,
  //                    50,
  //                    75,
  //                    99
  //                  ]
  //                }
  //              }
  //           }
  //        }
  //    }
  //}
  //        """)
  //      //.proxy("proxy.hcm.fpt.vn",80)
  //      .asString.body
  //    //println(body)
  //    val json = Json.parse(body)
  //    val array = json.\("aggregations").\("month").\("buckets").get.asInstanceOf[JsArray].value.toArray
  //    array.map(x => (x.\("key").get.asInstanceOf[JsString], x.\("doc_count").get.asInstanceOf[JsNumber], x.\("quantile").get.\("values").get))
  //         //.map(x => x._1 -> x._3)
  //
  //      .map(x => x._1.value -> JsArray(IndexedSeq(
  //             x._3.\("5.0").get.asInstanceOf[JsNumber],
  //             x._3.\("25.0").get.asInstanceOf[JsNumber],
  //             x._3.\("50.0").get.asInstanceOf[JsNumber],
  //             x._3.\("75.0").get.asInstanceOf[JsNumber],
  //             x._3.\("99.0").get.asInstanceOf[JsNumber])))
  //      .sortBy(x => x._1)
  //  }

  def main(args: Array[String]) {
    val reponse = get("NOT StatusCode:0")
    //    reponse.getTenGoiForSparkline().foreach(println)
    getMonth().foreach(println)
    client.close()
  }
}