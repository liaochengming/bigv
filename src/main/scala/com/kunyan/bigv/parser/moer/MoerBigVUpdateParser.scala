package com.kunyan.bigv.parser.moer

import java.util.regex.Pattern

import com.kunyan.bigv.db.LazyConnections
import com.kunyan.bigv.util.StringUtil
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


  def parse(url: String, html: String, lazyConn: LazyConnections, topic: String) = {


    try {

      if (url.startsWith(basePageUrlPart)) {
        parseArticleUrl(url, html, lazyConn, topic)
      } else if (url.startsWith(baseArticleUrl)) {
        MoerBigVHistoryParser.parseArticle(url,html,lazyConn)
      }

    } catch {

      case exception: Exception =>
        exception.printStackTrace()

    }
  }

  //解析大V文章的url
  def parseArticleUrl(url: String, html: String, lazyConn: LazyConnections, topic: String) : Unit = {

    var pageNum: Int = 0

    val matchPageNum = StringUtil.getMatch(url,"page=(\\d+)")

    if(null == matchPageNum || matchPageNum == ""){
      return
    }else{
      pageNum = matchPageNum.toInt
    }

    if(pageNum > 40){
      return
    }

    //匹配用户id
    val matchUserId = StringUtil.getMatch(url,"theId=(.*)&return")

    if(null == matchUserId || matchUserId == ""){
      return
    }


    val doc = Jsoup.parse(html)
    val elements = doc.select("a[title]")

    import scala.util.control.Breaks._
    breakable {

      for (index <- 0 until elements.size()) {

        val element = elements.get(index)
        val href = element.attr("href")
        val url = host + href

        val table = lazyConn.getTable("bigv_article")
        val g = new Get(url.getBytes)
        val result = table.get(g)

        if (result.isEmpty) {

          //将解析的文章页面的url发送给服务器
          lazyConn.sendTask(topic, StringUtil.getUrlJsonString(60003, url, 0))
        } else {

          break()

        }

      }

      //将下页文章列表的url发送给服务器
      val nextPageNum = pageNum + 1
      val nextPageUrl = url.replace("page=" + pageNum,"page=" + nextPageNum)
      lazyConn.sendTask(topic, StringUtil.getUrlJsonString(60003, nextPageUrl, 0))

    }
  }


}
