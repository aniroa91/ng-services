package churn.models

case class RegionResponse(
        churnRegion      : Array[(String, Double, Double)],
        trendRegionMonth : (Map[String, Int], Array[(Int, Int, Double, Double)]),
        churnProfile     : Array[(String, Double, Double)],
        trendProfileMonth: (Map[String, Int], Map[String, Int], Array[(Int , Int, Double, Double)]),
        trendAgeProfile  : (Map[String, Int], Array[(Int ,Int, Double, Double)]),
        numberOfContracts: (Array[(String)], Array[(String, Int)], Array[(Int , Int, Int)])
)
