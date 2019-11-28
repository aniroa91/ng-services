package churn.models

case class CLRegionResponse(
                           trendRegionMonth : (Map[String, Int], Map[String, Int], Array[(Int, Int, Double, Double, Int)]),
                           checklistRegion: (Array[String], Array[(String, String, Double, Long)]),
                           comments: String,
                           trendTBTMonth : (Map[String, Int], Map[String, Int], Array[(Int, Int, Double, Double, Int)]),
                           tbTBT         : (Array[(String)], Array[(String, String, Double, Double, Int)]),
                           month: String
                         )

case class CLAgeResponse(
                        cLRegionAge: (Array[String], Array[(String, String, Double, Long)]),
                        trendAgeLocation : (Map[String, Int], Map[String, Int], Array[(Int, Int, Double, Double, Int)]),
                        comments: String,
                        month: String
                        )

case class CLTimeResponse(
                           cLRegionTime: (Array[String], Array[String], Array[(Int, Int, Long)]),
                           trendTimeLocation : (Map[String, Int], Map[String, Int], Array[(Int, Int, Double, Double, Int)]),
                           comments: String,
                           month: String
                         )

case class CLCauseResponse(
                            cLRegionCause: (Array[String], Array[String], Array[(Int, Int, Long)]),
                            trendCauseLocation : (Map[String, Int], Map[String, Int], Array[(Int, Int, Double, Double, Int)]),
                            cLRegionPosition: (Array[String], Array[String], Array[(Int, Int, Long)]),
                            trendPositionLocation : (Map[String, Int], Map[String, Int], Array[(Int, Int, Double, Double, Int)]),
                            comments: String,
                            month: String
                          )

case class ChecklistResponse (
                               numContract   : (ContractNumber, ContractNumber, RateNumber),
                               churnRate     : RateNumber,
                               ctCheckList: Array[(String, Long, Double, Long)],
                               trendChecklist: Array[(String, Double, Double, Long)],
                               month: String,
                               province      : Array[(String, String)],
                               packages      : Array[(String)],
                               causes        : Array[(String)],
                               positions     : Array[(String)]
                             )

case class ChecklistResponse22 (
     ctCheckList: Array[(String, Long, Double, Long)],
     trendChecklist: Array[(String, Double, Double, Long)],
     checklistRegion: (Array[String], Array[(String, String, Double, Long)]),
     trendRegionMonth : (Map[String, Int], Array[(Int, Int, Double, Double, Int)]),
     checklistRegionAge: Array[(Int, Int, Double, Double, Int)],
     numRegionAge: Array[(String, String, Double, Long)],
     processTime: (Array[Int], Array[Long], Array[(Int, Long, Long)]),
     typeRegion: (Array[(Int, String)], Array[Long], Array[(Int, Long, Long)]),
     trendTypeBubble: (Map[String, Int], Array[(Int, Int, Double, Double, Long)]),
     month: String
)
