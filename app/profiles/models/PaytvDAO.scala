package model.paytv

import play.api.Play
import play.api.data.Form
import play.api.data.Forms._
import play.api.db.slick.DatabaseConfigProvider

import scala.concurrent.Future
import slick.driver.JdbcProfile
import com.sksamuel.elastic4s.http.ElasticDsl._
import slick.driver.PostgresDriver.api._
import org.joda.time.DateTime
import org.joda.time.format.DateTimeFormat
import profile.services.internet.HistoryService.client
import services.Configure
import services.domain.CommonService
import services.domain.CommonService.getAggregations
import org.elasticsearch.search.aggregations.bucket.histogram.DateHistogramInterval
import org.joda.time.DateTimeZone
import com.ftel.bigdata.utils.DateTimeUtil
import org.elasticsearch.search.sort.SortOrder
import profile.utils.CommonUtil
import service.BrasService.client

object PaytvDAO {

  val client = Configure.client

  val dbConfig = DatabaseConfigProvider.get[JdbcProfile](Play.current)


  def getContractResponse(code: String): PaytvResponse = {
    var arrCode = new Array[(String)](0)
    var nameValue = ""
    var provinceField = ""
    var statusField = ""
    var nameField = ""
    var provinceValue = ""
    var statusValue = ""
    if(code != null) {
       arrCode = code.split(',')
       statusField = arrCode(0).split(':')(0)
       provinceField = arrCode(1).split(':')(0)
       nameField = arrCode(2).split(':')(0)
       statusValue = arrCode(0).split(':')(1)
       provinceValue = arrCode(1).split(':')(1)
       nameValue = arrCode(2).split(':')(1)
    }
    val rs = if(code == null) {
      client.execute(
        multi(
          search(s"profile-paytv-*" / "docs")
            aggregations (
            termsAggregation("month")
              .field("month")
              .subAggregations(
                termsAggregation("status")
                  .field("StatusCode")
              ) size 1000
            ),
          search(s"profile-paytv-*" / "docs")
            aggregations (
            termsAggregation("month")
              .field("month")
              .subAggregations(
                termsAggregation("name")
                  .field("TenGoi")
              ) size 1000
            ),
          search(s"profile-paytv-*" / "docs")
            aggregations (
            termsAggregation("month")
              .field("month")
              .subAggregations(
                termsAggregation("province")
                  .field("ProvinceCode")
              ) size 1000
            )
        )
      ).await
    } else{
      client.execute(
        multi(
          search(s"profile-paytv-*" / "docs")
            query { must(if(nameValue == "-1") not(termQuery(s"$nameField", nameValue)) else termQuery(s"$nameField", nameValue),if(statusValue == "-1") not(termQuery(s"$statusField", statusValue)) else termQuery(s"$statusField", statusValue),if(provinceValue == "-1") not(termQuery(s"$provinceField", provinceValue)) else termQuery(s"$provinceField", provinceValue)) }
            aggregations(
            termsAggregation("month")
              .field("month")
              .subAggregations(
                termsAggregation("status")
                  .field("StatusCode")
              ) size 1000
            ),
          search(s"profile-paytv-*" / "docs")
            query { must(if(nameValue == "-1") not(termQuery(s"$nameField", nameValue)) else termQuery(s"$nameField", nameValue),if(statusValue == "-1") not(termQuery(s"$statusField", statusValue)) else termQuery(s"$statusField", statusValue),if(provinceValue == "-1") not(termQuery(s"$provinceField", provinceValue)) else termQuery(s"$provinceField", provinceValue)) }
            aggregations(
            termsAggregation("month")
              .field("month")
              .subAggregations(
                termsAggregation("name")
                  .field("TenGoi")
              ) size 1000
            ),
          search(s"profile-paytv-*" / "docs")
            query { must(if(nameValue == "-1") not(termQuery(s"$nameField", nameValue)) else termQuery(s"$nameField", nameValue),if(statusValue == "-1") not(termQuery(s"$statusField", statusValue)) else termQuery(s"$statusField", statusValue),if(provinceValue == "-1") not(termQuery(s"$provinceField", provinceValue)) else termQuery(s"$provinceField", provinceValue)) }
            aggregations(
            termsAggregation("month")
              .field("month")
              .subAggregations(
                termsAggregation("province")
                  .field("ProvinceCode")
              ) size 1000
            )
        )
      ).await
    }
    val stt = CommonService.getSecondAggregations(rs.responses(0).aggregations.get("month"),"status")
    val name = CommonService.getSecondAggregations(rs.responses(1).aggregations.get("month"),"name")
    val province = CommonService.getSecondAggregations(rs.responses(2).aggregations.get("month"),"province")

    val arrStatusPaytv = stt.flatMap(x => x._2.map(y => x._1 -> y))
      .map(x => (x._1, x._2._1,CommonUtil.getStatus(x._2._1.toInt)) -> x._2._2)
    val arrTenGoiPaytv = name.flatMap(x => x._2.map(y => x._1 -> y))
      .map(x => (x._1, x._2._1) -> x._2._2)
    val arrProvincePaytv = province.flatMap(x => x._2.map(y => x._1 -> y))
      .map(x => (x._1, x._2._1) -> x._2._2)

    PaytvResponse(arrStatusPaytv,arrTenGoiPaytv,arrProvincePaytv)
  }

}