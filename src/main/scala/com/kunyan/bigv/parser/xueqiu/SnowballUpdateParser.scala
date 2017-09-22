package com.kunyan.bigv.parser.xueqiu

import java.util.regex.Pattern

import com.kunyan.bigv.config.Platform
import com.kunyan.bigv.db.LazyConnections
import com.kunyan.bigv.logger.BigVLogger
import com.kunyan.bigv.util.StringUtil
import com.kunyan.nlp.task.NewsProcesser
import com.nlp.util.EasyParser
import org.apache.hadoop.hbase.client.Get
import org.json.JSONObject

/**
 * Created by lcm on 2016/12/7.
 * 雪球的大V文章更新
 */
object SnowballUpdateParser {

  val articleListUrlPart = "https://xueqiu.com/v4/statuses/user_timeline.json"

  def parse(url: String,
            html: String,
            lazyConn: LazyConnections,
            topic: String,
            newsProcesser:NewsProcesser,
            easyParser:EasyParser
             ) = {

    BigVLogger.warn("雪球 update url => " + url)

    if (url.startsWith(articleListUrlPart)) {
      parseArticleUrl(url, html, lazyConn, topic)
    }

    val pattern = Pattern.compile("https://xueqiu.com/(\\d+)/")
    val result = pattern.matcher(url)

    if(result.find()){

      //解析出用户文章页面的最大页面值
      SnowballHistoryParser.parserArticle(url,
        html,
        lazyConn,
        topic,
        newsProcesser,
        easyParser)

    }

  }

  //解析大V文章的url
  def parseArticleUrl(url: String, html: String, lazyConn: LazyConnections, topic: String): Unit = {

    val xueQiuHost = "https://xueqiu.com"

    var pageNum: Int = 0

    //排除接收的url pageNum为空或者过大
    val matchPageNum = StringUtil.getMatch(url,"page=(\\d+)")

    if(null == matchPageNum || matchPageNum == ""){
      return
    }else{
      pageNum = matchPageNum.toInt
    }

    if(pageNum > 40){
      return
    }

    try {

      val json = new JSONObject(html)
      val jsonArr = json.getJSONArray("statuses")

      import scala.util.control.Breaks._
      breakable {

        for (index <- 0 until jsonArr.length()) {

          val articleInfo = jsonArr.getJSONObject(index)
          val target = articleInfo.getString("target")

          val articleUrl = xueQiuHost + target

          val table = lazyConn.getTable("news_detail")
          val g = new Get(articleUrl.getBytes)
          val result = table.get(g)

          //置顶标签（1为置顶文章，0为非指定文章）
          val mark = articleInfo.getString("mark")

          if (mark == "1") {

            if (result.isEmpty) {

              lazyConn.sendTask(topic, StringUtil.getUrlJsonString(Platform.SNOW_BALL.id, articleUrl, 0))
            }

        }else{

            if (result.isEmpty) {
              lazyConn.sendTask(topic, StringUtil.getUrlJsonString(Platform.SNOW_BALL.id, articleUrl, 0))
            } else {
              break()
            }

          }
        }
        //将下页文章列表的url发送给服务器
        val nextPageNum = pageNum + 1
        val nextPageUrl = url.replace("page=" + pageNum,"page=" + nextPageNum)
        lazyConn.sendTask(topic, StringUtil.getUrlJsonString(Platform.SNOW_BALL.id, nextPageUrl, 0))

      }

    }catch {

      case exception:Exception =>
        BigVLogger.error("雪球 update 解析大V文章列表出错！ url => " + url)
        exception.printStackTrace()

    }
  }
}
