package com.kunyan.bigv.parser.zhongjin

import java.util.regex.Pattern

import com.kunyan.bigv.db.LazyConnections
import com.kunyan.bigv.util.StringUtil
import org.apache.hadoop.hbase.client.Get
import org.jsoup.Jsoup

/**
 * Created by lcm on 2016/12/12.
 * 中金实时更新大V文章解析
 */
object ZhongJinUpdateParser {


  val articleHomePagePart = "http://blog.cnfol.com/index.php/article/blogarticlelist"

  def parse(url: String, html: String, lazyConn: LazyConnections, topic: String) = {

    if (url.startsWith(articleHomePagePart)) {
      parserArticleUrl(url, html, lazyConn, topic)
    }

    val pattern = Pattern.compile("http://blog.cnfol.com/.*article/(\\d+)")
    val result = pattern.matcher(url)

    if (result.find()) {
      ZhongJinHistoryParser.parserArticle(url, html, lazyConn, topic)
    }

  }

  /**
   * 解析文章列表页面的文章url
   * @param url 文章列表的url
   * @param html 文章列表的html
   * @param lazyConn 连接
   * @param topic kafka消息
   */
  def parserArticleUrl(url: String, html: String, lazyConn: LazyConnections, topic: String) = {

    try {

      val doc = Jsoup.parse(html)
      val pageNum = StringUtil.getMatch(url, "page=(\\d+)").toInt

      val articleUrlList = doc.select("a.ArtTit")

      import scala.util.control.Breaks._
      breakable {

        for (index <- 0 until articleUrlList.size()) {

          val article = articleUrlList.get(index)
          val topMark = article.select("i.Top").size()
          val articleUrl = article.attr("href")

          if (topMark == 1) {

            val table = lazyConn.getTable("bigv_article")
            val g = new Get(articleUrl.getBytes)
            val result = table.get(g)

            if (result.isEmpty) {
              lazyConn.sendTask(topic, StringUtil.getUrlJsonString(60007, articleUrl, 0))
            }
          } else {

            val table = lazyConn.getTable("bigv_article")
            val g = new Get(articleUrl.getBytes)
            val result = table.get(g)

            if (result.isEmpty) {
              lazyConn.sendTask(topic, StringUtil.getUrlJsonString(60007, articleUrl, 0))
            } else {
              break()
            }
          }
        }

        val nextPageNum = pageNum + 1
        val nextPageUrl = url.replace("page=" + pageNum, "page=" + nextPageNum)
        lazyConn.sendTask(topic, StringUtil.getUrlJsonString(60007, nextPageUrl, 0))

      }

    } catch {

      case exception: Exception =>
        exception.printStackTrace()

    }
  }

}
