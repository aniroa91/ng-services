package churn.models

case class ContractNumber(curr: Int, prev: Int)

case class RateNumber(avg: Double, curr: Double)

case class OverviewResponse(
                           numContract   : (ContractNumber, ContractNumber, ContractNumber),
                           churnRate     : (RateNumber, RateNumber, RateNumber),
                           numofMonth    : Array[(String, String, Long)],
                           trendRatePert : (Array[(String, Double, Double, Double)], Array[(String, Double, Double, Double)], Array[(String, Double, Double, Double)]),
                           province      : Array[(String, String)],
                           packages      : Array[(String)],
                           month         : String
                         )

