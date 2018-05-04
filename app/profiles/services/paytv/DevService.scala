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



  def getChurn(month: String) = {
    val request = search(s"profile-internet-${month}" / "docs") aggregations (
      termsAggregation("Region")
        .field("RegionCode")
        .subaggs(
          termsAggregation("Age")
            .field("Age")
            .subaggs(
              termsAgg("Status", "StatusCode")) size 20))
    val reponse = client.execute(request).await
    //reponse.aggregations.foreach(println)
    getChurnRateAndPercentage(reponse, "Region", "Age", "Status")

    //getBucketTerm(reponse, "province", "month").map(x => (x._2, ProvinceUtil.getProvince(x._1.toInt), x._3) )
  }

  private def getBucketTerm3(response: SearchResponse, term1: String, term2: String, term3: String): Array[(String, String, String, Int)] = {
    if (response.aggregations != null) {
      response.aggregations
        .getOrElse(term1, Map[String, AnyRef]()).asInstanceOf[Map[String, AnyRef]]
        .getOrElse("buckets", List).asInstanceOf[List[AnyRef]]
        .map(x => x.asInstanceOf[Map[String, AnyRef]])
        .flatMap(x => {
          val term1Key = x.getOrElse("key", "key").toString()
          x.getOrElse(term2, Map[String, AnyRef]()).asInstanceOf[Map[String, AnyRef]]
            .getOrElse("buckets", List).asInstanceOf[List[AnyRef]]
            .map(y => y.asInstanceOf[Map[String, AnyRef]])
            .flatMap(y => {
              val term2Key = y.getOrElse("key", "key").toString()
              y.getOrElse(term3, Map[String, AnyRef]()).asInstanceOf[Map[String, AnyRef]]
                .getOrElse("buckets", List).asInstanceOf[List[AnyRef]]
                .map(z => z.asInstanceOf[Map[String, AnyRef]])
                .map(z => {
                  val term3Key = z.getOrElse("key", "key").toString()
                  (term1Key, term2Key, term3Key, z.getOrElse("doc_count", "0").toString().toInt)
                })
            })
        }).toArray
    } else {
      Array[(String, String, String, Int)]()
    }
  }

  private def getChurnRateAndPercentage(response: SearchResponse, term1: String, term2: String, term3: String): Array[(String, String, String, Int, Int, Int)] = {
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

  private def calChurnRateAndPercentage(array: Array[(String, String, String, Int)]): Array[(Int, Int, Int, Int)] = {
    // (region, age, churn_Val no_churn_val)
    val rs = array
      .map(x => (x._1.toInt, x._2.toInt) -> (if(x._3.toInt == 1) x._4.toInt else 0, if(x._3.toInt == 0) x._4.toInt else 0 ))
      .groupBy(x => x._1)
      .map(x => x._1 -> (x._2.map(y => y._2._1).sum, x._2.map(y => y._2._2).sum))
      .map(x => (x._1._1, x._1._2, x._2._1, x._2._2))
      .toArray

    rs
  }

  private def calChurnRateAndPercentageForCTBDV(array: Array[(String, String, String, Int, Int, Int)]) = {
    val ctbdv = array.filter(x => x._3.toInt == 3)
    val sumByRegion = ctbdv.map(x => x._1 -> x._6)
      .groupBy(x => x._1)
      .map(x => x._1 -> x._2.map(y => y._2).sum)
    // region, age, rate, percentage
    ctbdv.map(x => (x._1.toInt, x._2.toInt, x._6  * 1.0/ x._5, x._6 * 1.0 / (sumByRegion.getOrElse(x._1, 0)) ) )
  }

  private def calChurnRateAndPercentageForChurn(array: Array[(String, String, String, Int, Int, Int)]) = {
    val ctbdv = array.filter(x => x._3.toInt == 1)
    val sumByRegion = ctbdv.map(x => x._1 -> x._6)
      .groupBy(x => x._1)
      .map(x => x._1 -> x._2.map(y => y._2).sum)
    // region, age, rate, percentage
    ctbdv.map(x => (x._1.toInt, x._2.toInt, x._6  * 1.0/ x._5, x._6 * 1.0 / (sumByRegion.getOrElse(x._1, 0)) ) )
  }

  def getInternet(month: String) = {
    //calChurnRateAndPercentage()
    val response = getChurn(month)
    (calChurnRateAndPercentageForCTBDV(response),calChurnRateAndPercentageForChurn(response))
  }

  def bubbleHeatChart(array: Array[(Int, Int, Double, Double)]): (JsValue, JsValue) = {
    val color = Json.toJson(array.map(x => x._4).map(x => hsv2rgb(hsv(x * 1000))))
    //implicit val bubbleWrites = Json.writes[Bubble]

    val series = Json.toJson(array.map(x => JsObject(Seq(
      "x" -> JsNumber(x._1),
      "y" -> JsNumber(x._2),
      "z" -> JsNumber(x._3),
      "c" -> JsNumber(x._4))))
      .map(x => {
        JsObject(Seq(
          "data" -> JsArray(Seq(x)),
          "maxSize" -> JsNumber(50),
          "minSize" -> JsNumber(1)
        ))
      }))

    //val a = JsObject(Seq("x" -> JsNumber(0)))
    //"data" -> JsArray(Seq(a))

    (color, series)
  }

  //  def seriesForLineChart(array: Array[(String, String, Int)], typeChart: String): JsValue = {
  //    implicit val bubbleWrites = Json.writes[Bubble]
  //    Json.toJson(anomaly.points.map(x => Bubble(-x._2._2, x._2._1, 1.0, x._1)))
  //
  //    val group = array.groupBy(x => x._2).map(x => x._1 -> x._2.map(y => (y._1, y._3)).sortBy(y => y._1))
  //    val a = group.map(x => JsObject(Seq(
  //      "name" -> JsString(x._1),
  //      "type" -> JsString(typeChart),
  //      "data" -> JsArray(x._2.map(y => JsNumber(y._2))))))
  //    Json.toJson(a)
  //  }

  def hsv(number: Double): (Double, Double, Double) = {
    hsv(number, 1, 1000)
  }

  def hsv(number: Double, min: Double, max: Double): (Double, Double, Double) = {
    val h= Math.floor((max - number) * 1200 / max);
    val s = 1.0//Math.abs(number - min)/min;
    val v = 1.0;
    (h,s,v)
  }

  //  def hsv2rgb(hsv: (Double, Double, Double)): (Double, Double, Double) = {
  //    val colorCode = Color.HSBtoRGB(hsv._1.toFloat, hsv._2.toFloat, hsv._3.toFloat)
  //    println(colorCode)
  //    var rgbaBuffer: ListBuffer[Double] = List.fill(3)(0.0).to[ListBuffer]
  //    rgbaBuffer(0) = ((colorCode & 0x00ff0000) >> 16).toDouble // r
  //    rgbaBuffer(1) = ((colorCode & 0x0000ff00) >> 8).toDouble // g
  //    rgbaBuffer(2) = ((colorCode & 0x000000ff)).toDouble // b
  //    //  rgbaBuffer(3) = (((colorCode & 0xff000000) >> 24) & 0x000000ff).toDouble/255.0 // alpha
  //    (rgbaBuffer(0), rgbaBuffer(1), rgbaBuffer(2))
  //  }

  def hsv2rgb(hsv: (Double, Double, Double)) = {
    val v = hsv._3
    val s = hsv._2
    val rgb = if (s == 0) {
      (v, v, v)
    } else {
      val h = hsv._1 / 60
      val i = Math.floor(h)
      val data = (v*(1-s), v*(1-s*(h-i)), v*(1-s*(1-(h-i))))
      i match {
        case 0 => (v, data._3, data._1)
        case 1 => (data._2, v, data._1)
        case 2 => (data._1, v, data._3)
        case 3 => (data._1, data._2, v)
        case 4 => (data._3, data._1, v)
        case _ => (v, data._1, data._2)
      }
    }
    val r = "0" + Math.round(rgb._1 * 255).toHexString
    val g = "0" + Math.round(rgb._2 * 255).toHexString
    val b = "0" + Math.round(rgb._3 * 255).toHexString
    "#" + r.substring(r.length()-2) + g.substring(g.length()-2) + b.substring(b.length()-2)
  }


  def main(args: Array[String]) {
    val reponse = get("NOT StatusCode:0")
    //    reponse.getTenGoiForSparkline().foreach(println)
    getMonth().foreach(println)
    client.close()
  }
}