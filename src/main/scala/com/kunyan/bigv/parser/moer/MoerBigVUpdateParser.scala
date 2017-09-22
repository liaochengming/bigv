package com.kunyan.bigv.parser.moer

import com.kunyan.bigv.config.Platform
import com.kunyan.bigv.db.LazyConnections
import com.kunyan.bigv.logger.BigVLogger
import com.kunyan.bigv.util.StringUtil
import com.nlp.util.{EasyParser, NewsProcesser}
import org.apache.hadoop.hbase.client.Get
import org.jsoup.Jsoup

/**
 * Created by lcm on 2016/11/30.
 * 更新大V的最新文章
 */
object MoerBigVUpdateParser {

  val host = "http://www.moer.cn/"
  val basePageUrlPart = "http://www.moer.cn/findArticlePageList.htm"
  val baseArticleUrl = "http://www.moer.cn/articleDetails.htm?articleId="


  def parse(url: String,
            html: String,
            lazyConn: LazyConnections,
            topic: String,
            newsProcesser:NewsProcesser,
            easyParser:EasyParser
             ) = {

    BigVLogger.warn("摩尔 update url => " + url)

    try {

      if (url.startsWith(basePageUrlPart)) {
        parseArticleUrl(url, html, lazyConn, topic)
      } else if (url.startsWith(baseArticleUrl)) {
        MoerBigVHistoryParser.parseArticle(url,
          html,
          lazyConn,
          newsProcesser,
          easyParser)
      }

    } catch {

      case exception: Exception =>
        exception.printStackTrace()

    }
  }

  //解析大V文章的url
  def parseArticleUrl(url: String, html: String, lazyConn: LazyConnections, topic: String): Unit = {

    var pageNum: Int = 0

    try{

      //排除接收的url pageNum为空或者过大
      val matchPageNum = StringUtil.getMatch(url, "page=(\\d+)")

      if (null == matchPageNum || matchPageNum == "") {
        return
      } else {
        pageNum = matchPageNum.toInt
      }

      if (pageNum > 40) {
        return
      }

      //匹配用户id
      val matchUserId = StringUtil.getMatch(url, "theId=(.*)&return")

      if (null == matchUserId || matchUserId == "") {
        return
      }


      val doc = Jsoup.parse(html)
      val elements = doc.select("a[title]")

      import scala.util.control.Breaks._
      breakable {

        if (elements.size() > 0){

          for (index <- 0 until elements.size()) {

            val element = elements.get(index)
            val href = element.attr("href")
            val articleUrl = host + href

            val table = lazyConn.getTable("news_detail")
            val g = new Get(articleUrl.getBytes)
            val result = table.get(g)

            if (result.isEmpty) {

              //将解析的文章页面的url发送给服务器
              lazyConn.sendTask(topic, StringUtil.getUrlJsonString(Platform.MOER.id, articleUrl, 0))
            } else {

              break()

            }

          }

          //将下页文章列表的url发送给服务器
          val nextPageNum = pageNum + 1
          val nextPageUrl = url.replace("page=" + pageNum,"page=" + nextPageNum)
          lazyConn.sendTask(topic, StringUtil.getUrlJsonString(Platform.MOER.id, nextPageUrl, 0))
        }

      }

    }catch {

      case e:Exception =>

        BigVLogger.error("摩尔 update 解析文章列表出错！url => " + url)
        e.printStackTrace()

    }
  }
}
