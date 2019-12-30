package utils

import org.joda.time.{DateTime, DateTimeZone}
import utils.FileUtil

object CommonUtil {
  val TIMEZONE_HCM = "Asia/Ho_Chi_Minh"
  val YMD          = "yyyy-MM-dd"

  def create(seconds: Long): DateTime = new DateTime(seconds).withZone(DateTimeZone.forID(TIMEZONE_HCM))

  def getRangeDay(month: String) = {
    var firstOfMonth = new DateTime(month).withDayOfMonth(1)
    val firstNextMonth = firstOfMonth.plusMonths(1)
    var arrDayOfMonth = new Array[String](0)
    while (firstOfMonth.isBefore(firstNextMonth)){
      arrDayOfMonth +:= firstOfMonth.toString(YMD)
      firstOfMonth = firstOfMonth.plusDays(1)
    }
    arrDayOfMonth
  }

  def formatDecimalByDigit(number: Double, digit: Int): Double = {
    BigDecimal(number).setScale(digit, BigDecimal.RoundingMode.HALF_UP).toDouble
  }

  /*val LOCATION = FileUtil.readResource("/resources/locationUP.csv")
   .filter(x => !x.startsWith("#"))
   .map(x => x.split("\t"))
   .map(x => x(2)).distinct.sorted*/
}
