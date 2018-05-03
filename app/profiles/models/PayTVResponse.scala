package profile.model

import play.api.libs.json.JsArray
import com.ftel.bigdata.utils.DateTimeUtil
import org.joda.time.Months

case class PayTVResponse(
                          status: Array[(String, String, Int)],
                          tengoi: Array[(String, String, Int)],
                          usage: Array[(String, String, Int)],
                          province: Array[(String, String, Int)],
                          usageQuantile: Array[(String, JsArray)],
                          lteQuantile: Array[(String, JsArray)]) {
  def getTenGoiForSparkline(): Array[(String, Int, Array[Int])] = {
    tengoi.map(x => x._2 -> x)
      .groupBy(x => x._1)
      .map(x => x._1 -> x._2.map(y => (y._2._1, y._2._3)).sortBy(x => x._1))
      .map(x => (x._1, x._2.map(y => y._2)))
      .map(x => (x._1, x._2(x._2.length-1), x._2) )
      .toArray
  }

  //  def nomalize(from: String, to: String): PayTVResponse = {
  //    val start = DateTimeUtil.create(from, "yyyy-MM")
  //    val end = DateTimeUtil.create(to, "yyyy-MM")
  //    val number = Months.monthsBetween(start, end).getMonths
  //    null
  //  }

  def normalize(months: Array[String]): PayTVResponse = {
    PayTVResponse(
      normalize(months, status),
      normalize(months, tengoi),
      normalize(months, usage),
      normalize(months, province),
      usageQuantile,
      lteQuantile)
  }

  private def normalize(months: Array[String], array: Array[(String, String, Int)]): Array[(String, String, Int)] = {
    val terms = array.map(x => x._2).distinct
    val map = array.map(x => (x._1, x._2) -> x._3).toMap
    months.flatMap(x => terms.map(y => (x,y)))
      .map(x => x -> map.getOrElse(x, 0))
      .map(x => (x._1._1, x._1._2, x._2) )

  }
}