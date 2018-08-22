package profiles.utils

object AgeGroupUtil {
   private val AGE = Map(
     "0-6" -> 6,
     "6-12" -> 12,
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

  def getIdAge(id: Int): String = AGE.find(x=> x._2 == id).get._1

}
