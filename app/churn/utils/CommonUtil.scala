package profile.utils

object CommonUtil {
  val STATUS_MAP = Map(
      "HUY DICH VU" -> 1,
      "NVLDTT" -> 2,
      "CTBDV" -> 3,
      "BINH THUONG" -> 0)

  val RANGE_USAGE_MAP = Map(
    "0min/day" -> "<1",
    "5min/day" -> ">0_<=8400",
    "15min/day" -> ">8400_<=25200",
    ">15min/day" -> ">25200")
      
  def getStatus(code: Int): String = STATUS_MAP.find(x => x._2 == code).get._1
}