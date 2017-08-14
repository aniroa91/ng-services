package services.domain

import model.DashboardResponse

import org.elasticsearch.search.sort.SortOrder
import com.ftel.bigdata.utils.HttpUtil
import com.sksamuel.elastic4s.ElasticsearchClientUri
import com.sksamuel.elastic4s.http.ElasticDsl._
import com.sksamuel.elastic4s.http.HttpClient
import com.sksamuel.elastic4s.http.search.SearchHit
import com.sksamuel.elastic4s.http.search.SearchResponse
import services.CacheService
import com.ftel.bigdata.utils.DateTimeUtil
import scala.util.Try
import model.ReportResponse

object ReportService extends AbstractService {

  def get(day: String): ReportResponse = {
    val prev = CommonService.getPreviousDay(day)
    
    val time0 = System.currentTimeMillis()
    val multiSearchResponse = client.execute(
      multi(
        search(s"dns-stats-${day}" / "count") query { not(termQuery(LABEL_FIELD, TOTAL_VALUE)) },
        search(s"dns-stats-${prev}" / "count") query { not(termQuery(LABEL_FIELD, TOTAL_VALUE)) },
        search(s"dns-stats-${day}" / "malware") query { must(termQuery(LABEL_FIELD, BLACK_VALUE)) } sortBy { fieldSort(NUM_QUERY_FIELD) order SortOrder.DESC } limit 100,
        search(s"dns-stats-${day}" / "top") query { must(termQuery(LABEL_FIELD, BLACK_VALUE)) } sortBy { fieldSort(NUM_QUERY_FIELD) order SortOrder.DESC } limit 1000
        )).await
    val time1 = System.currentTimeMillis()
    
    val currentResponse = multiSearchResponse.responses(0)
    val previousResponse = multiSearchResponse.responses(1)
    val malwaresResponse = multiSearchResponse.responses(2)
    val mainDomainResponse = multiSearchResponse.responses(3)

    if (currentResponse.hits != null) {
      //println(currentResponse.hits.hits)
      val time2 = System.currentTimeMillis()
      val labelClassify = getTotalInfo(currentResponse)
      val current = sumTotalInfo(labelClassify.map(x => x._2))
      val previous = if (previousResponse.hits != null) {
        sumTotalInfo(getTotalInfo(previousResponse).map(x => x._2))
      } else current
      val malwares = getMalwareInfo(malwaresResponse)
      val blacks = CommonService.getTopBlackByNumOfQuery(day)
      val seconds = CommonService.getTopRank(1, day)
      val time3 = System.currentTimeMillis()
      ReportResponse(day, current, previous, labelClassify, malwares, blacks, seconds)
    } else null
  }
}
