package churn.models

case class Indicator(
                    infErrors: Array[(String, Double, Double)],
                    signin   : Array[(String, Double, Double)],
                    suyhao   : Array[(String, Double, Double)],
                    download : Array[(String, Double, Double)],
                    fee      : Array[(String, Double, Double)]
                    )

case class DetectResponse(
                         cardMetrics: (Int, Int, Int, Int, Int),
                         numCallChecklist: (Long, Long, Long, Long),
                         overall: (Array[(String, Long)], Array[(String, Long)], Array[(String, Long)]),
                         complain: Array[(String, Long)],
                         cates   : Array[(String, Long)],
                         topCallContent: Array[(String, String, String, String)],
                         medianHours: Array[(String, Double)],
                         numCauses: Array[(String, Long)],
                         topChecklistContent: Array[(String, String, String, String)],
                         medianMaintain: Array[(String, Double, Long)],
                         indicators: Indicator,
                         linkFilters: Map[String, String]
                         )
