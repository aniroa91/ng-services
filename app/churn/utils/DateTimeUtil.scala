package churn.utils

import org.joda.time.DateTime
import org.joda.time.format.DateTimeFormat
import org.joda.time.DateTimeZone
import services.domain.CommonService

object DateTimeUtil {
  val MONTH_PATTERN      = "yyyy-MM"
  val YMD                = "yyyy-MM-dd"
  val ES_FORMAT          = "yyyy-MM-dd'T'HH:mm:ss.SSS Z"
  val FULL_FORMAT        = "yyyy-MM-dd HH:mm:ss.SSS Z"
  val AMIRSHEMESH_FORMAT = "yyyy-MM-dd HH:mm:ss:SSS"
  val TIMEZONE_HCM       = "Asia/Ho_Chi_Minh"
  val GMT                = "GMT"
  val FULL_FORMAT2       = "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'"
  val FORMAT_ES5         = "yyyy-MM-dd'T'HH:mm:ss.SSSZZ"

  /**
   * Create Datetime from seconds
   */
  def create(seconds: Long): DateTime = new DateTime(seconds * 1000L).withZone(DateTimeZone.forID(TIMEZONE_HCM))

  /**
   * Create datatime from string format
   */
  def create(dateAsString: String, pattern: String = YMD): DateTime =
    DateTime.parse(dateAsString, DateTimeFormat.forPattern(pattern)).withZone(DateTimeZone.forID(TIMEZONE_HCM))

  /**
   * Get current date
   */
  def now = new DateTime()

  /**
   * Get current date
   */
  def today = now.dayOfYear().roundFloorCopy()

  /**
   * Get previous month
   * INPUT: month with format: yyyy-MM
   */
  def getPreviousMonth(month: String): String = {
    create(month, MONTH_PATTERN).minusMonths(1).toString(MONTH_PATTERN)
  }

  /**
   * Get previous month
   * INPUT: month with format: yyyy-MM
   */
  def getNextMonth(month: String): String = {
    create(month, MONTH_PATTERN).plusMonths(1).toString(MONTH_PATTERN)
  }

  /**
   * Get current month
   * OUTPUT: yyyy-MM
   */
  def getThisMonth(): String = {
    now.toString(MONTH_PATTERN)
  }

  /**
   * Check current month
   */
  def isThisMonth(month: String): Boolean = {
    getThisMonth() == month
  }

  /**
   * Convert day to month
   */
  def toMonth(day: String): String = create(day, YMD).toString(MONTH_PATTERN)

  /**
   * Get first day of month
   */
  def getFirstDayOfMonth(month: String): DateTime = create(month, MONTH_PATTERN).dayOfMonth().withMinimumValue()

  /**
   * Get latest day of month
   */
  def getLastDayOfMonth(month: String): DateTime = create(month, MONTH_PATTERN).dayOfMonth().withMaximumValue()

  /**
   * Get number of day in month
   */
  def getNumberDayOfMonth(month: String): Int = create(month, MONTH_PATTERN).dayOfMonth().getMaximumValue

  /**
   * Get all day in month
   */
  def getDaysOfMonth(month: String): Array[String] = {
    val first  = getFirstDayOfMonth(month)
    val number = getNumberDayOfMonth(month)
    (0 until number).toArray.map(x => first.plusDays(x).toString(YMD)).sorted
  }

  def getDaysOfMonthByInterval(month: String, inteval: Int): Array[String] = {
    val first  = getFirstDayOfMonth(CommonService.getLastNumMonth(month, 2))
    val sumDays = CommonService.getTopMonth(month, 3).map(x=> getNumberDayOfMonth(x)).sum
    val number = if(sumDays % inteval == 0) sumDays / inteval else sumDays / inteval + 1
    (0 until (number + 1)).toArray.map(x => first.plusDays(inteval*x -1).toString(YMD)).sorted
  }

  /*def getCurrDaysOfMonthByInterval(month: String, inteval: Int): Array[String] = {
    val first  = getFirstDayOfMonth(month)
    // 3
    val number = if(getNumberDayOfMonth(month) % inteval == 0) getNumberDayOfMonth(month)/ inteval else getNumberDayOfMonth(month)/ inteval + 1
    (0 until (number + 1)).toArray.map(x => first.plusDays(inteval*x -1).toString(YMD)).sorted
  }*/
  /**
   * Convert id of dayOfWeek to shortText
   */
  def dayOfWeekText(i: Int): String = {
    i match {
      case 1 => "Mon"
      case 2 => "Tue"
      case 3 => "Wed"
      case 4 => "Thu"
      case 5 => "Fri"
      case 6 => "Sat"
      case 7 => "Sun"
    }
  }

  /**
   * Get all day of month and fill value for them if any
   */
  def getArrayDaily[T](month: String, dailyMap: Map[String, T], default: T): Array[(String, T)] = {
    getDaysOfMonth(month).map(x => x -> dailyMap.getOrElse(x, default))
  }

  /**
   * Get all day of week and fill value for them if any
   */
  def getArrayDayOfWeek[T](dayOfWeekMap: Map[Int, T], default: T): Array[(String, T)] = {
    val days = (1 until 8).toArray
    days.map(x => dayOfWeekText(x) -> dayOfWeekMap.getOrElse(x, default))
  }

  //def numberOfDayInMonth(month: String): Int = create(month, "yyyy-MM").dayOfMonth().getMaximumValue
  /**
   * Convert seconds to hour and get 2 digit decimal
   */
  def secondToHour(seconds: Long): Double = BigDecimal(seconds / 60.0 / 60.0).setScale(2, BigDecimal.RoundingMode.HALF_UP).toDouble

  def test() {
    val timestamp = 1520272740254L
    val date = DateTimeUtil.create(timestamp / 1000)
    println(date)
    //println(date.toString("HH"))


    val dateString = "2018-03-06T12:05:43.588Z"

    val d1 = DateTimeUtil.create(dateString, FULL_FORMAT2)
    val t = d1.getMillis

    val d2 = DateTimeUtil.create(t / 1000)

    println(t) // 1520312743570
    println(d1)
    println(d2)

    println(today)
    println(now)
  }

  /**
   * Get data as String format yyyy-MM-dd
   * @param seconds, default is today
   */
  def toDayAsString(seconds: Long = (now.getMillis / 1000)) = create(seconds).toString(YMD)

  /**
   * Get Yesterday as String format yyyy-MM-dd
   */
  def yesterday() = now.minusDays(1).toString(YMD)

  //  /**
  //   * GEt millis second from date
  //   */
  //  def getMillis(day: String = toDay(), format: String = YMD) = create(day, format).getMillis

  def main(args: Array[String]) {
    test()
  }
}