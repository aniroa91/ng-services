package profile.utils

import com.ftel.bigdata.utils.FileUtil

object ProvinceUtil {
  private val MAP = FileUtil.readResource("/resources/province.csv")
    .filter(x => !x.startsWith("#"))
    .map(x => x.split(","))
    .map(x => x(0).toUpperCase() -> x(4).toInt)
    .toMap
  private val REGION = Map(
      "VUNG 1" -> 1,
      "VUNG 2" -> 2,
      "VUNG 3" -> 3,
      "VUNG 4" -> 4,
      "VUNG 5" -> 5,
      "VUNG 6" -> 6,
      "VUNG 7" -> 7,
      "OTHER" -> 0)
 
      
  def getProvinceCode(key: String): Int =  MAP.getOrElse(key, 0)
  
  def getProvince(code: Int): String =  MAP.find(x => x._2 == code).get._1
  
  def getRegionCode(key: String): Int = REGION.getOrElse(key, 0)
  
  def main(args: Array[String]) {
    MAP.foreach(println)
  }
}