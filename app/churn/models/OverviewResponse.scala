package churn.models

case class ContractNumber(curr: Int, prev: Int)

case class RateNumber(avg: Double, curr: Double)

case class PackageResponse(
                        trendPkgMonth : (Map[String, Int], Map[String, Int], Array[(Int, Int, Double, Double, Int)]),
                        tbPkg         : (Array[(String)], Array[(String, String, Double, Double, Int)]),
                        trendPkgLocation : (Map[String, Int], Map[String, Int], Array[(Int, Int, Double, Double, Int)]),
                        trendPkgAge : (Map[String, Int], Map[String, Int], Array[(Int, Int, Double, Double, Int)]),
                        comments      : String,
                        month         : String
                      )

case class AgeResponse(
                        trendAgeMonth : (Map[String, Int], Map[String, Int], Array[(Int, Int, Double, Double, Int)]),
                        tbAge         : (Array[(String)], Array[(String, String, Double, Double, Int)]),
                        trendAgeLocation : (Map[String, Int], Map[String, Int], Array[(Int, Int, Double, Double, Int)]),
                        comments      : String,
                        month         : String
                      )

case class OverviewResponse(
                           numContract   : (ContractNumber, ContractNumber, ContractNumber),
                           churnRate     : (RateNumber, RateNumber, RateNumber),
                           numofMonth    : Array[(String, String, Long)],
                           trendRatePert : Array[(String, Double, Double, Double)],
                           province      : Array[(String, String)],
                           packages      : Array[(String)],
                           trendRegionMonth : (Map[String, Int], Map[String, Int], Array[(Int, Int, Double, Double, Int)]),
                           tbChurn       : (Array[(String)], Array[(String, String, Double, Double, Int)]),
                           comments      : String,
                           month         : String
                         )

