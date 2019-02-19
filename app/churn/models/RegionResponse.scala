package churn.models

case class RegionResponse(
        churnRegion      : Array[(String, Double, Double, Long)],
        trendRegionMonth : (Map[String, Int], Array[(Int, Int, Double, Double, Int)]),
        churnProfile     : Array[(String, Double, Double, Long)],
        trendProfileMonth: (Map[String, Int], Map[String, Int], Array[(Int , Int, Double, Double, Int)]),
        trendAgeProfile  : (Map[String, Int], Array[(Int ,Int, Double, Double, Int)]),
        numberOfContracts: (Array[(String)], Array[(String, Int)], Array[(Int , Int, Int)]),
        month: String
)
