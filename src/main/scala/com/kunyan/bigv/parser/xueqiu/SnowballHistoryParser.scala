package com.kunyan.bigv.parser.xueqiu

import java.util.regex.Pattern

import com.kunyan.bigv.config.Platform
import com.kunyan.bigv.db.LazyConnections
import com.kunyan.bigv.logger.BigVLogger
import com.kunyan.bigv.util.{DBUtil, StringUtil}
import com.kunyandata.nlpsuit.util.KunyanConf
import org.apache.hadoop.hbase.client.Get
import org.json.JSONObject
import org.jsoup.Jsoup


/**
 * Created by lcm on 2016/12/6.
 * 解析雪球的top100大V历史文章
 */
object SnowballHistoryParser {

  val articleListUrlPart = "https://xueqiu.com/v4/statuses/user_timeline.json"

  def parse(url: String,
            html: String,
            lazyConn: LazyConnections,
            topic: String,
            stopWords: Array[String],
            classModels: scala.Predef.Map[scala.Predef.String, scala.Predef.Map[scala.Predef.String, scala.Predef.Map[scala.Predef.String, java.io.Serializable]]],
            sentimentModels: scala.Predef.Map[scala.Predef.String, scala.Any],
            keyWordDict: scala.Predef.Map[scala.Predef.String, scala.Predef.Map[scala.Predef.String, scala.Array[scala.Predef.String]]],
            kyConf: KunyanConf,
            summaryExtraction: (String, Int)
             ) = {

    BigVLogger.warn("雪球 history url => " + url)

    val pattern = Pattern.compile("https://xueqiu.com/(\\d+)$")
    val result = pattern.matcher(url)

    if (result.find()) {

      //解析出用户文章页面的最大页面值
      val userId = result.group(1)
      parserMaxPage(userId, html, lazyConn, topic)

    }

    if (url.startsWith(articleListUrlPart)) {

      //解析页面上的文章url
      parserArticleUrl(html, lazyConn, topic)
    }

    val pattern2 = Pattern.compile("https://xueqiu.com/(\\d+)/")
    val result2 = pattern2.matcher(url)

    if (result2.find()) {

      //解析出用户文章页面的最大页面值
      parserArticle(url,
        html,
        lazyConn,
        topic,
        stopWords,
        classModels,
        sentimentModels,
        keyWordDict,
        kyConf,
        summaryExtraction)

    }
  }

  //解析最大页面数
  def parserMaxPage(userId: String, html: String, lazyConn: LazyConnections, topic: String) = {

    try {

      val baseArticleListUrl = "https://xueqiu.com/v4/statuses/user_timeline.json?user_id=%s&page=%s"

      val doc = Jsoup.parse(html)
      val data = doc.body().toString
      val result = StringUtil.getMatch(data, "\"maxPage\":(\\d+)")

      if (null != result && result.toInt > 1) {

        var ArticleListUrl: String = ""

        for (num <- 1 to result.toInt) {

          ArticleListUrl = baseArticleListUrl.format(userId, num)
          lazyConn.sendTask(topic, StringUtil.getUrlJsonString(Platform.SNOW_BALL.id, ArticleListUrl, 0))

        }
      } else {
        lazyConn.sendTask(topic, StringUtil.getUrlJsonString(Platform.SNOW_BALL.id, baseArticleListUrl.format(userId, 1), 0))
      }

    } catch {

      case exception: Exception =>
        BigVLogger.error("雪球 history 解析最大页面数出错！ userId => " + userId)
        exception.printStackTrace()

    }
  }

  //解析文章列表页面的文章url
  def parserArticleUrl(html: String, lazyConn: LazyConnections, topic: String) = {

    val xueqiuHost = "https://xueqiu.com"

    try {

      val json = new JSONObject(html)
      val jsonArr = json.getJSONArray("statuses")

      for (index <- 0 until jsonArr.length()) {

        val articleInfo = jsonArr.getJSONObject(index)
        val target = articleInfo.getString("target")
        val articleUrl = xueqiuHost + target

        val checkUserSql = lazyConn.mysqlVipConn.prepareStatement("select * from snowball_article_black_list where url = ?")
        checkUserSql.setString(1, articleUrl)
        val result = checkUserSql.executeQuery()

        if (!result.next()) {
          lazyConn.sendTask(topic, StringUtil.getUrlJsonString(Platform.SNOW_BALL.id, articleUrl, 0))
        }

      }

    } catch {

      case exception: Exception =>
        BigVLogger.error("雪球 history 解析文章列表页面出错！")
        exception.printStackTrace()

    }
  }

  def parserArticle(url: String,
                    html: String,
                    lazyConn: LazyConnections,
                    topic: String,
                    stopWords: Array[String],
                    classModels: scala.Predef.Map[scala.Predef.String, scala.Predef.Map[scala.Predef.String, scala.Predef.Map[scala.Predef.String, java.io.Serializable]]],
                    sentimentModels: scala.Predef.Map[scala.Predef.String, scala.Any],
                    keyWordDict: scala.Predef.Map[scala.Predef.String, scala.Predef.Map[scala.Predef.String, scala.Array[scala.Predef.String]]],
                    kyConf: KunyanConf,
                    summaryExtraction: (String, Int)
                     ) = {

    val tableName = "news_detail"
    val platform = Platform.SNOW_BALL.id.toString

    val cstmt = lazyConn.mysqlVipConn.prepareCall("{call proc_InsertSnowBallNewArticle(?,?,?,?,?,?,?)}")

    val cstmtDigest = lazyConn.mysqlVipConn.prepareCall("{call proc_InsertDigestSnowBall(?,?,?,?)}")

    val newsMysqlStatement = lazyConn.mysqlNewsConn.prepareStatement("INSERT INTO news_info (n_id, type, platform, title, url, news_time, industry, section, stock, digest, summary, sentiment, updated_time, source)" +
      " VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)")

    val recordBlackUrl = lazyConn.mysqlVipConn.prepareStatement("insert into snowball_article_black_list(url) VALUES (?)")
    try {

      val doc = Jsoup.parse(html)
      val isRaw = doc.select("div.status-retweet").isEmpty
      val time = doc.select("div[class=\"subtitle\"] a").attr("data-created_at")
      val screeName = doc.select("a[class=\"avatar\"]").attr("data-screenname")
      val uid = doc.select("div.status-item").attr("data-uid")
      val retweet = doc.select("a.btn-repost em.em_number").text()
      val reply = doc.select("a[class=\"btn-status-reply last\"] em.em_number").text()
      var title = ""
      var content = ""
      var retweeted = ""

      if (isRaw) {

        //原创
        retweeted = "1"
        title = doc.select("div.status-content h1").text()
        content = doc.select("div.status-content div.detail").text()

      } else {

        //转载
        retweeted = "0"
        title = screeName + "：的观点"
        content = doc.select("div.status-content").get(0).text().replace("查看对话", "")

      }

      if (title == "" || title.length > 200) {
        title = screeName + ":的观点"
      }

      if (content != "") {

        BigVLogger.warn("写入表的数据 => " + url + "  timeStamp => " + time.toLong)

        val table = lazyConn.getTable(tableName)
        val g = new Get(url.getBytes)
        val result = table.get(g)

        if (result.isEmpty) {

          val insTrue = DBUtil.insertCall(cstmt, uid, title, retweet, reply, url, time.toLong, "")

          if (insTrue) {

            DBUtil.inputDataToSql(lazyConn,
              cstmtDigest,
              newsMysqlStatement,
              url,
              title,
              time.toLong,
              content,
              Platform.OLD_SNOW_BALL.id,
              Platform.OLD_SNOW_BALL.toString,
              stopWords,
              classModels,
              sentimentModels,
              keyWordDict,
              kyConf,
              summaryExtraction)

            DBUtil.insertHbase(tableName, url, content, time, platform, title, lazyConn)
          }
        }

      } else {

        DBUtil.insert(recordBlackUrl, url)
      }

    } catch {

      case exception: Exception =>
        BigVLogger.error("雪球 解析文章出错！url => " + url)
        exception.printStackTrace()

    }
  }
}
