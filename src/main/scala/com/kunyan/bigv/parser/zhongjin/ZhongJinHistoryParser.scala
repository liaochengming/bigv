package com.kunyan.bigv.parser.zhongjin

import java.util.regex.Pattern

import com.kunyan.bigv.db.LazyConnections
import com.kunyan.bigv.util.{DBUtil, StringUtil}
import org.jsoup.Jsoup

/**
 * Created by lcm on 2016/12/12.
 * 中金平台大V历史文章解析
 */
object ZhongJinHistoryParser {

  val articleHomePagePart = "http://blog.cnfol.com/index.php/article/blogarticlelist"

  def parse(url: String, html: String, lazyConn: LazyConnections, topic: String) = {

    if (url.startsWith(articleHomePagePart)) {
      parserArticleUrl(url, html, lazyConn, topic)
    }

    val pattern = Pattern.compile("http://blog.cnfol.com/.*article/(\\d+)")
    val result = pattern.matcher(url)

    if (result.find()) {
      parserArticle(url, html, lazyConn, topic)
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
      val pageNum = StringUtil.getMatch(url, "page=(\\d+)")

      if (pageNum == "1") {

        val maxPageNum = doc.select("i.CoRed").text().split("/")(1)

        for (num <- 2 to maxPageNum.toInt) {

          val nextPageUrl = url.replace("page=" + pageNum, "page=" + num)
          lazyConn.sendTask(topic, StringUtil.getUrlJsonString(60007, nextPageUrl, 0))

        }
      }

      val articleUrlList = doc.select("a.ArtTit")

      for (index <- 0 until articleUrlList.size()) {

        val article = articleUrlList.get(index)
        val articleUrl = article.attr("href")
        lazyConn.sendTask(topic, StringUtil.getUrlJsonString(60007, articleUrl, 0))

      }

    } catch {

      case exception: Exception =>
        exception.printStackTrace()

    }
  }

  /**
   * 解析大V的文章
   * @param html 文章的html
   * @param lazyConn 连接
   * @param topic kafka消息
   */
  def parserArticle(url: String, html: String, lazyConn: LazyConnections, topic: String) = {

    try{

      val platform = "60007"

      val doc = Jsoup.parse(html)
      val title = doc.select("h1.Head a").text()
      val publish_time = DBUtil.getLongTimeStamp(StringUtil.getMatch(doc.select("span.MBTime").text(),"\\[(.*)\\]"),"yyyy-MM-dd HH:mm:ss").toString
      val content = doc.select("p.MsoNormal b").text()

      DBUtil.insertHbase("zhongjin_bigv_article", url, content, publish_time, platform, title, lazyConn)

    }catch {
      case exception:Exception=>
        exception.printStackTrace()
    }
  }
}
