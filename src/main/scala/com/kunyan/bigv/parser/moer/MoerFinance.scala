package com.kunyan.bigv.parser.moer

import com.kunyan.bigv.config.Platform
import com.kunyan.bigv.db.LazyConnections
import com.kunyan.bigv.logger.BigVLogger
import com.kunyan.bigv.util.{DBUtil, StringUtil}
import org.jsoup.Jsoup

/**
 * Created by niujiaojiao on 2016/11/16.
 *
 */
object MoerFinance {

  val firstUrl = "http://www.moer.cn/investment_findPageList.htm?onColumns=all&industrys=all&fieldColumn=all&price=all&authorType=1&sortType=time&page="
  val secondUrl = "http://www.moer.cn/"
  var articleUrl = "http://www.moer.cn/articleDetails.htm?articleId="

  //进入摩尔金融
  def parse(pageUrl: String, html: String, lazyConn: LazyConnections, topic: String) = {

    BigVLogger.warn("摩尔 select url => " + pageUrl)

    try {

      if (pageUrl == "https://www.moer.cn/investment_findPageList.htm?onColumns=all&industrys=all&fieldColumn=all&price=all&authorType=1&sortType=time") {
        sendFirstPatch(pageUrl, html, topic, lazyConn)
      } else if (pageUrl.startsWith(firstUrl)) {
        sendSecondPatch(pageUrl, html, lazyConn, topic)
      } else if (pageUrl.startsWith(articleUrl)) {
        parseArticle(pageUrl, html, topic, lazyConn)
      }

    } catch {

      case e: Exception =>
        e.printStackTrace()

    }

  }

  //文章的解析：获取文章信息
  def parseArticle(url: String, html: String, topic: String, lazyConn: LazyConnections): Unit = {

    try {

      val mysqlConnection = lazyConn.mysqlVipConn
      mysqlConnection.setAutoCommit(true)

      val articleSql = mysqlConnection.prepareStatement("insert into  moer_bigv_article_info(articleId,authorId,buy,view,time,likeInfo, priceInfo,profit) VALUES (?,?,?,?,?,?,?,?)")
      val authorSql = mysqlConnection.prepareStatement("insert into  moer_bigv_author_info(authorId,authorName,authorType,watchCount,fansCount,articleCount) VALUES (?,?,?,?,?,?)")
      val doc = Jsoup.parse(html, "UTF-8")
      val articleId = url.split("articleId=")(1)
      var authorId = ""
      var flag = true

      if (doc.toString.contains("author-info")) {
        authorId = doc.select("div.author-info h3  a[href]").attr("href").split("theId=")(1)
      } else {
        flag = false
      }

      if (flag) {

        var buyCnt = doc.select("div.left-content i.red").text().replace(" ","")

        if (null == buyCnt || buyCnt == "") {
          buyCnt = "0"
        }

        val tags = doc.select("p.article-other-info span")
        val readCnt = tags.get(1).text.substring(3).replaceAll("次", "")
        val timeStamp = (DBUtil.getTimeStamp(tags.get(0).text.substring(3), "yyyy年MM月dd日 HH:mm:ss") / 1000).toInt.toString
        val likeCnt = doc.getElementsByClass("article-handler").first().getElementsByTag("b").text
        var price = "0"
        val priceTag = doc.select("span.now-price")

        if (priceTag.size() != 0){
          price = priceTag.first().text()
        }

        val profit = "0"

        //文章信息
        DBUtil.insert(articleSql, articleId, authorId, buyCnt, readCnt, timeStamp, likeCnt, price, profit)

        val author = doc.select("div.author-info h3  a[href]").text()
        var authorType = 0

        if (doc.getElementsByClass("userv-red").size > 0 || doc.getElementsByClass("userv-blue").size > 0){
          authorType = 1
        }

        val numberTags = doc.select("div.author ul.stats-list li")
        var watchCount = ""
        var fansCount = ""
        var articleCount = ""

        if (numberTags != null) {

          watchCount = numberTags.get(0).select("span").text()
          fansCount = numberTags.get(1).select("span").text()
          articleCount = numberTags.get(2).select("i").text()

        }

        //作者信息
        DBUtil.insert(authorSql, authorId, author, authorType, watchCount, fansCount, articleCount)
      }
    } catch {

      case e: Exception =>
        BigVLogger.error("摩尔  select 解析文章信息和用户信息出错！url => " + url)
        e.printStackTrace()

    }

  }


  /**
   * 发送拼接url 入口的解析
   */
  def sendFirstPatch(url: String, html: String, topic: String, lazyConn: LazyConnections): Unit = {

    try {

      val doc = Jsoup.parse(html, "UTF-8")

      if (doc.toString.contains("commentPager") && doc.toString.contains("navList")) {

        val pageNum = doc.select("div#commentPager ul.navList li:last-child em").text().toInt
        for (i <- 1 to pageNum) {

          val url = firstUrl + i
          lazyConn.sendTask(topic, StringUtil.getUrlJsonString(Platform.MOER.id, url, 0))

        }
      } else {

        BigVLogger.error("摩尔 select this is html \n\n")
        BigVLogger.error("摩尔 select " + html + "\n" + "*****************\n*************************")

      }

    } catch {

      case e: Exception =>
        BigVLogger.error("摩尔 select 解析最大页面数出错 url => " + url)
        e.printStackTrace()

    }
  }

  //进行每一页的所有URL发送
  def sendSecondPatch(url: String, html: String, lazyConn: LazyConnections, topic: String): Unit = {

    try {

      val doc = Jsoup.parse(html, "UTF-8")
      val divTags = doc.select("div.item-main h3 a[href]")

      for (i <- 0 until divTags.size) {

        val href = divTags.get(i).attr("href")
        val url = secondUrl + href
        lazyConn.sendTask(topic, StringUtil.getUrlJsonString(Platform.MOER.id, url, 0))

      }
    } catch {

      case e: Exception =>
        BigVLogger.error("摩尔 select 解析的文章列表出错！ url => " + url)
        e.printStackTrace()

    }
  }
}
