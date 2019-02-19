package churn.models

case class CallogResponse (
         whoCallIn: Array[(String, Long, Double, Long)],
         churnCates: Array[(String, Double, Double,Long)],
         callInRegionAge: Array[(String, String, Double, Long)],
         trendCallIn: Array[(String, Double, Double, Long)],
         callInRegion: (Array[String], Array[(String, String, Double, Long)]),
         trendRegionMonth : (Map[String, Int], Array[(Int, Int, Double, Double, Int)]),
         callRegionAge: Array[(Int, Int, Double, Double, Int)],
         month: String
)

