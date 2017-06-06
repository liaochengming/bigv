package com.kunyan.bigv.parser.moer

import com.kunyan.bigv.config.Platform
import com.kunyan.bigv.db.LazyConnections
import com.kunyan.bigv.logger.BigVLogger
import com.kunyan.bigv.util.{DBUtil, StringUtil}
import com.kunyandata.nlpsuit.util.KunyanConf
import org.apache.hadoop.hbase.client.Get
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

  def parse(url: String,
            html: String,
            lazyConn: LazyConnections,
            topic: String,
            stopWords: Array[String],
            classModels: scala.Predef.Map[scala.Predef.String, scala.Predef.Map[scala.Predef.String, scala.Predef.Map[scala.Predef.String, java.io.Serializable]]],
            sentimentModels: scala.Predef.Map[scala.Predef.String, scala.Any],
            keyWordDict: scala.Predef.Map[scala.Predef.String, scala.Predef.Map[scala.Predef.String, scala.Array[scala.Predef.String]]],
            kyConf: KunyanConf,
            summaryExtraction: (String, Int)) = {

    BigVLogger.warn("摩尔 history url => " + url)

    try {

      if (url.startsWith(baseBigVHomeUrl)) {
        parseArticlePageUrl(url, html, lazyConn, topic)
      } else if (url.startsWith(basePageUrlPart)) {
        parseArticleUrl(html, lazyConn, topic)
      } else if (url.startsWith(baseArticleUrl)) {
        parseArticle(url,
          html,
          lazyConn,
          stopWords,
          classModels,
          sentimentModels,
          keyWordDict,
          kyConf,
          summaryExtraction)
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

    try {

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

        lazyConn.sendTask(topic, StringUtil.getUrlJsonString(Platform.MOER.id, articleListUrl, 0))
      }

    } catch {

      case e: Exception =>
        BigVLogger.error("摩尔 history 解析文章最大数出错 ！url => " + homeUrl)
        e.printStackTrace()
    }

  }


  //解析大V文章的url
  def parseArticleUrl(html: String, lazyConn: LazyConnections, topic: String) = {

    try{

      val doc = Jsoup.parse(html)
      val elements = doc.select("a[title]")

      for (index <- 0 until elements.size()) {

        val element = elements.get(index)
        val href = element.attr("href")
        val url = host + href
        lazyConn.sendTask(topic, StringUtil.getUrlJsonString(Platform.MOER.id, url, 0))

      }

    }catch {

      case e:Exception =>
        BigVLogger.error("摩尔 history 解析文章列表页出错! ")
        e.printStackTrace()

    }
  }

  //解析文章
  def parseArticle(url: String,
                   html: String,
                   lazyConn: LazyConnections,
                   stopWords: Array[String],
                   classModels: scala.Predef.Map[scala.Predef.String, scala.Predef.Map[scala.Predef.String, scala.Predef.Map[scala.Predef.String, java.io.Serializable]]],
                   sentimentModels: scala.Predef.Map[scala.Predef.String, scala.Any],
                   keyWordDict: scala.Predef.Map[scala.Predef.String, scala.Predef.Map[scala.Predef.String, scala.Array[scala.Predef.String]]],
                   kyConf: KunyanConf,
                   summaryExtraction: (String, Int)
                    ) = {

    try{

      val cstmt = lazyConn.mysqlVipConn.prepareCall("{call proc_InsertMoerNewArticle(?,?,?,?,?,?,?,?)}")

      val cstmtDigest = lazyConn.mysqlVipConn.prepareCall("{call proc_InsertDigestMoer(?,?,?,?,?,?)}")

      val newsMysqlStatement = lazyConn.mysqlNewsConn.prepareStatement("INSERT INTO news_info (n_id, type, platform, title, url, news_time, industry, section, stock, digest, summary, sentiment, updated_time, source)" +
        " VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)")



      val platform = Platform.MOER.id.toString
      val doc = Jsoup.parse(html)
      val uid = doc.select("a.follow").attr("uid")
      val timeStamp = DBUtil.getLongTimeStamp(doc.select("span:matches(时间)").text.substring(3), "yyyy年MM月dd日 HH:mm:ss").toString
      val title = doc.select("h2.article-title").text()
      val read = StringUtil.getMatch(doc.select("span:matches(浏览)").text(),"(\\d+)")
      var buyCount = 0
      var price = 0.0
      var text: String = ""
      val buy = doc.select("a.submit-btn").first().text()

      if (buy == "购买") {

        buyCount = doc.select("p.article-other-info i.red").text().replace(" ","").toInt
        price= doc.select("span.now-price").first().text().toDouble
        DBUtil.insertHbase("news_detail", url, "付费文章", timeStamp, platform, title, lazyConn)

      } else {

        text=doc.select("div[class=\"article-daily article-daily-first\"] p").text()

        if(text != ""){

          BigVLogger.warn("写入表的数据 => " + url + "  timeStamp => " + timeStamp)

          val table = lazyConn.getTable("news_detail")
          val g = new Get(url.getBytes)
          val result = table.get(g)

          if (result.isEmpty) {

            val insTrue = DBUtil.insertCall(cstmt, uid, title, read, buyCount, price,url, timeStamp, "")

            if(insTrue){

              DBUtil.inputDataToSql(lazyConn,
                cstmtDigest,
                newsMysqlStatement,
                url,
                title,
                timeStamp.toLong,
                text,
                Platform.OLD_MOER.id,
                Platform.OLD_MOER.toString,
                stopWords,
                classModels,
                sentimentModels,
                keyWordDict,
                kyConf,
                summaryExtraction)

              DBUtil.insertHbase("news_detail", url, text, timeStamp, platform, title, lazyConn)
            }
          }
        }
      }

    }catch {

      case e:Exception =>

        BigVLogger.error("摩尔 解析文章出错！url => " + url)
        e.printStackTrace()

    }

  }
}
