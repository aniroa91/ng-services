package churn.utils

object AgeGroupUtil {
  val AGE = Map(
     "0-6" -> 6,
     "06-12" -> 12,
     "12-18" -> 18,
     "18-24" -> 24,
     "24-30" -> 30,
     "30-36" -> 36,
     "36-42" -> 42,
     "42-48" -> 48,
     "48-54" -> 54,
     "54-60" -> 60,
     ">60" -> 66
  )

  val AGE_INDEX = Map(
     6 -> 0,
     12 -> 1,
     18 -> 2,
     24 -> 3,
     30 -> 4,
     36 -> 5,
     42 -> 6,
     48 -> 7,
     54 -> 8,
     60 -> 9,
     66 -> 10
  )

   val AGE_CALCULATE = Map(
      "0-6"   -> "age:<=6",
      "06-12" -> "age:>6 AND age:<=12",
      "12-18" -> "age:>12 AND age:<=18",
      "18-24" -> "age:>18 AND age:<=24",
      "24-30" -> "age:>24 AND age:<=30",
      "30-36" -> "age:>30 AND age:<=36",
      "36-42" -> "age:>36 AND age:<=42",
      "42-48" -> "age:>42 AND age:<=48",
      "48-54" -> "age:>48 AND age:<=54",
      "54-60" -> "age:>54 AND age:<=60",
      ">60"   -> "age:>60"
   )

  def getAgeById(id: Int): String = AGE.find(x=> x._2 == id).get._1

  def getAgeIdByName(name: String): Int = AGE.find(x=> x._1 == name).get._2

  def getCalAgeByName(name: String): String = AGE_CALCULATE.find(x=> x._1 == name).get._2

}
