package com.kunyan.bigv.parser.moer

import com.kunyan.bigv.db.LazyConnections
import com.kunyan.bigv.util.{DBUtil, StringUtil}
import org.jsoup.Jsoup

/**
 * Created by lcm on 2016/11/28.
 * 此类用来解析摩尔金融大V100的
 * 历史文章数据
 */
object MoerBigVHistoryParser {

  val host = "http://www.moer.cn/"
  val baseBigVHomeUrl = "http://www.moer.cn/authorHome.htm?theId="
  val basePageUrl = "http://www.moer.cn/findArticlePageList.htm?theId=%s&returnAuPage=&page=%s&collectType=0"
  val basePageUrlPart = "http://www.moer.cn/findArticlePageList.htm"
  val baseArticleUrl = "http://www.moer.cn/articleDetails.htm?articleId="

  def parse(url: String, html: String, lazyConn: LazyConnections, topic: String) = {


    try {

      if (url.startsWith(baseBigVHomeUrl)) {
        parseArticlePageUrl(url, html, lazyConn, topic)
      } else if (url.startsWith(basePageUrlPart)) {
        parseArticleUrl(html, lazyConn, topic)
      } else if (url.startsWith(baseArticleUrl)) {
        parseArticle(url, html, lazyConn)
      }


    } catch {

      case exception: Exception =>
        exception.printStackTrace()

    }
  }

  //解析大V文章页面的url
  def parseArticlePageUrl(homeUrl: String, html: String, lazyConn: LazyConnections, topic: String) = {

    val arr = homeUrl.split("=")
    var userId: String = ""

    if (arr.length == 2) {
      userId = arr(1)
    }

    val doc = Jsoup.parse(html)

    //文章数
    val articleNum = doc.select("li[id=\"articleLeft\"] span").text()

    //页面数
    val pageNum = (articleNum.toInt + 9) / 10

    for (page <- 1 to pageNum) {

      //文章列表的url
      val articleListUrl = basePageUrl.format(userId, page)

      lazyConn.sendTask(topic, StringUtil.getUrlJsonString(60003, articleListUrl, 0))
    }
  }


  //解析大V文章的url
  def parseArticleUrl(html: String, lazyConn: LazyConnections, topic: String) = {

    val doc = Jsoup.parse(html)
    val elements = doc.select("a[title]")

    for (index <- 0 until elements.size()) {

      val element = elements.get(index)
      val href = element.attr("href")
      val url = host + href
      lazyConn.sendTask(topic, StringUtil.getUrlJsonString(60003, url, 0))

    }
  }

  //解析文章
  def parseArticle(url: String, html: String, lazyConn: LazyConnections) = {

    val platform = "60003"
    val doc = Jsoup.parse(html)
    val timestamp = DBUtil.getLongTimeStamp(doc.select("p.summary-footer i:matches(时间)").text.substring(3)).toString
    val title = doc.select("h2.article-title").text()
    var text: String = ""
    val elements = doc.select("div[class=\"article-daily article-daily-first\"] p:not([style],[class])")

    if (elements.size() == 0) {

      DBUtil.insertHbase("bigv_article", url, platform, "付费文章", title, timestamp, lazyConn)

    } else {

      for (index <- 0 until elements.size()) {
        text = text + elements.get(index).text() + " "
      }

      DBUtil.insertHbase("bigv_article", url, platform, text.toString, title, timestamp, lazyConn)
    }

  }
}
