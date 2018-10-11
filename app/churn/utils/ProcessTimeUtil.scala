package churn.utils

object ProcessTimeUtil {

  val PROCESS_TIME = Map(
    "0-3 hours"   -> 3,
    "3-24 hours"  -> 24,
    "24-48 hours" -> 48,
    ">48 hours"   -> 60
  )

  val PROCESS_TIME_INDEX = Map(
    3  -> 0,
    24 -> 1,
    48 -> 2,
    60 -> 3
  )

  def getIndexById(id: Int) = PROCESS_TIME_INDEX.find(x=> x._1 == id).get._2

  def getNameById(id: Int) = PROCESS_TIME.find(x=> x._2 == id).get._1

}
