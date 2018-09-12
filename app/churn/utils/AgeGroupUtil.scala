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
      "0-6"   -> "Age:<=6",
      "06-12" -> "Age:>6 AND Age:<=12",
      "12-18" -> "Age:>12 AND Age:<=18",
      "18-24" -> "Age:>18 AND Age:<=24",
      "24-30" -> "Age:>24 AND Age:<=30",
      "30-36" -> "Age:>30 AND Age:<=36",
      "36-42" -> "Age:>36 AND Age:<=42",
      "42-48" -> "Age:>42 AND Age:<=48",
      "48-54" -> "Age:>48 AND Age:<=54",
      "54-60" -> "Age:>54 AND Age:<=60",
      ">60"   -> "Age:>60"
   )

  def getAgeById(id: Int): String = AGE.find(x=> x._2 == id).get._1

  def getCalAgeByName(name: String): String = AGE_CALCULATE.find(x=> x._1 == name).get._2

}
