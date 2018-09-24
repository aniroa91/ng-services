package churn.models

case class RegionResponse(
        churnRegion      : Array[(String, Double, Double)],
        trendRegionMonth : (Map[String, Int], Array[(Int, Int, Double, Double, Int)]),
        churnProfile     : Array[(String, Double, Double)],
        trendProfileMonth: (Map[String, Int], Map[String, Int], Array[(Int , Int, Double, Double, Int)]),
        trendAgeProfile  : (Map[String, Int], Array[(Int ,Int, Double, Double, Int)]),
        numberOfContracts: (Array[(String)], Array[(String, Int)], Array[(Int , Int, Int)])
)
