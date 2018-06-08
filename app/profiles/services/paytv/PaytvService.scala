package service

import services.domain.{AbstractService, CommonService}
import com.sksamuel.elastic4s.http.ElasticDsl._

import scala.concurrent.Future
import org.elasticsearch.search.sort.SortOrder
import services.domain.CommonService.formatUTC
import org.joda.time.DateTime
import org.joda.time.DateTimeZone
import org.joda.time.format.DateTimeFormat
import org.elasticsearch.search.aggregations.bucket.histogram.DateHistogramInterval
import org.joda.time.DateTimeZone
import com.ftel.bigdata.utils.DateTimeUtil
import model.paytv.{PaytvDAO, PaytvResponse}

object PaytvService extends AbstractService{

  def getContractResponse(code: String): PaytvResponse = {
    PaytvDAO.getContractResponse(code)
  }

}