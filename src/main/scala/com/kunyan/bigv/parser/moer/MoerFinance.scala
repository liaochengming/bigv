package com.kunyan.bigv.parser.moer

import com.kunyan.bigv.db.LazyConnections
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

    try {

      if (pageUrl == "http://www.moer.cn/investment_findPageList.htm?onColumns=all&industrys=all&fieldColumn=all&price=all&authorType=1&sortType=time") {
        println("enter first ")
        sendFirstPatch(pageUrl, html, topic, lazyConn)
      } else if (pageUrl.startsWith(firstUrl)) {
        println("enter second")
        sendSecondPatch(pageUrl, html, lazyConn, topic)
      } else if (pageUrl.startsWith(articleUrl)) {
        println("enter third")
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
      val mysqlConnection = lazyConn.mysqlConn
      mysqlConnection.setAutoCommit(true)
      val articlesql = mysqlConnection.prepareStatement("insert into  moer_bigv_article_info(articleId,authorId,buy,view,time,likeInfo, priceInfo,profit) VALUES (?,?,?,?,?,?,?,?)")
      val authorsql = mysqlConnection.prepareStatement("insert into  moer_bigv_author_info(authorId,authorName,authorType,watchCount,fansCount,articleCount) VALUES (?,?,?,?,?,?)")
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
        var buyCnt = doc.select("div.left-content i.red").text()
        if (null == buyCnt) {
          buyCnt = "0"
        }
        val tags = doc.getElementsByClass("summary-footer").first.getElementsByTag("span").first.getElementsByTag("i")
        val readCnt = tags.get(0).text.substring(3).replaceAll("次", "")
        val timeStamp = (DBUtil.getTimeStamp(tags.get(1).text.substring(3), "yyyy年MM月dd日 HH:mm:ss") / 1000).toInt.toString
        val likeCnt = doc.getElementsByClass("article-handler").first().getElementsByTag("b").text
        var price = "0"
        val priceTag = doc.getElementsByAttributeValue("class", "float-r goOrder").first
        if (priceTag != null)
          price = priceTag.getElementsByTag("strong").first.text
        var profit = "0"
        if (doc.toString.contains("total_yield")) {
          val total = doc.toString.split("total_yield\":")
          if ((total.length >= 2) && doc.toString.contains("article_type")) {
            profit = total(1).split("article_type")(0).split(",")(0).replace("\"", "")
            println("profit is: " + profit)
          }
        }
        //文章信息
        DBUtil.insert(articlesql, articleId, authorId, buyCnt, readCnt, timeStamp, likeCnt, price, profit)

        val author = doc.select("div.author-info h3  a[href]").text()
        var authorType = 0
        if (doc.getElementsByClass("userv-red").size > 0 || doc.getElementsByClass("userv-blue").size > 0)
          authorType = 1
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
        DBUtil.insert(authorsql, authorId, author, authorType, watchCount, fansCount, articleCount)
      }
    } catch {
      case e: Exception =>
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
        for (i <- 1 to 800) {

          val url = firstUrl + i
          //          val result = DBUtil.query("60003_detail", url, lazyConn)

          //          if (result == null || result._1.isEmpty || result._2.isEmpty) {
          lazyConn.sendTask(topic, StringUtil.getUrlJsonString(60003, url, 0))
          //          }

        }
      } else {
        println("this is html \n\n")
        println(html + "\n" + "*****************\n*************************")
      }

    } catch {
      case e: Exception =>
        e.printStackTrace()
    }


  }

  //进行每一页的所有URL发送
  //http://www.moer.cn/investment_findPageList.htm?onColumns=all
  // &industrys=all&fieldColumn=all&price=all&authorType=1&sortType=time&page=1
  def sendSecondPatch(url: String, html: String, lazyConn: LazyConnections, topic: String): Unit = {

    try {

      val doc = Jsoup.parse(html, "UTF-8")
      val divTags = doc.select("div.item-main h3 a[href]")

      for (i <- 0 until divTags.size) {

        val href = divTags.get(i).attr("href")
        val url = secondUrl + href

        //        val result = DBUtil.query("60003_detail", url, lazyConn)

        //        if (result == null || result._1.isEmpty || result._2.isEmpty) {
        lazyConn.sendTask(topic, StringUtil.getUrlJsonString(60003, url, 0))
        //        }

      }
    } catch {
      case e: Exception =>
        e.printStackTrace()
    }

  }

}
