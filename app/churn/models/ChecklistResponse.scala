package churn.models

case class ChecklistResponse (
     ctCheckList: Array[(String, Long, Double, Long)],
     trendChecklist: Array[(String, Double, Double)],
     checklistRegion: (Array[String], Array[(String, String, Double, Long)]),
     trendRegionMonth : (Map[String, Int], Array[(Int, Int, Double, Double, Int)]),
     checklistRegionAge: Array[(Int, Int, Double, Double, Int)],
     numRegionAge: Array[(String, String, Double, Long)],
     processTime: (Array[String], Array[Long], Array[(Int, Long, Long)]),
     typeRegion: (Array[(Int, String)], Array[Long], Array[(Int, Long, Long)]),
     trendTypeBubble: (Map[String, Int], Array[(Int, Int, Double, Double, Long)])
)
