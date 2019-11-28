package churn.utils

object ProcessTimeUtil {

  val PROCESS_TIME = Map(
    "0-3 hours"   -> "0-3",
    "03-24 hours"  -> "3-24",
    "24-48 hours" -> "24-48",
    ">=48 hours"   -> ">=48"
  )

  def getIndexById(id: String) = PROCESS_TIME.find(x=> x._1 == id).get._2

}
