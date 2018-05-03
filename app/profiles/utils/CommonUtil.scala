package profile.utils

object CommonUtil {
  val STATUS_MAP = Map(
      "HUY DICH VU" -> 1,
      "NVLDTT" -> 2,
      "CTBDV" -> 3,
      "BINH THUONG" -> 0)
      
  def getStatus(code: Int): String = STATUS_MAP.find(x => x._2 == code).get._1
}