package churn.utils

import churn.utils.FileUtil
import scalaj.http.Http

object HttpUtil {

  /**
   * Get content form url
   */
  def getContent(url: String): String = {
    //getContentWithProxy(url, null)
    Http(url).asString.body
  }

  /**
   * Get content from url with proxy
   */
  def getContent(url: String, proxyHost: String, proxyPort: Int): String = {
    //val proxy = new Proxy(Proxy.Type.HTTP, new InetSocketAddress(proxyHost, proxyPort))
    //getContentWithProxy(url, proxy)
    val response = Http(url).proxy(proxyHost, proxyPort).asString
    response.body
  }

  def download(url: String, out: String) {
    val response = Http(url).asBytes.body
    FileUtil.write(out, response)
  }

  def download(url: String, out: String, proxyHost: String, proxyPort: Int) {
    val response = Http(url).proxy(proxyHost, proxyPort).asBytes.body
    FileUtil.write(out, response)
  }
}
