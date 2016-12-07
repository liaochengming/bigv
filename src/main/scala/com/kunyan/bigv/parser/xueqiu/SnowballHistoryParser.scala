package com.kunyan.bigv.parser.xueqiu

import java.util.regex.Pattern

import com.kunyan.bigv.db.LazyConnections
import com.kunyan.bigv.util.StringUtil
import org.json.JSONObject
import org.jsoup.Jsoup


/**
 * Created by lcm on 2016/12/6.
 * 解析雪球的top100大V历史文章
 */
object SnowballHistoryParser {

  val articleListUrlPart = "https://xueqiu.com/v4/statuses/user_timeline.json"

  def parse(url: String, html: String, lazyConn: LazyConnections, topic: String) = {

    val pattern = Pattern.compile("https://xueqiu.com/(\\d+)$")
    val result = pattern.matcher(url)

    if(result.find()){

      //解析出用户文章页面的最大页面值
      val userId = result.group(1)
      parserMaxPage(userId,html,lazyConn,topic)

    }

    if(url.startsWith(articleListUrlPart)){

      //解析页面上的文章url
      parserArticleUrl(html,lazyConn,topic)
    }

    val pattern2 = Pattern.compile("https://xueqiu.com/(\\d+)/")
    val result2 = pattern2.matcher(url)

    if(result2.find()){

      //解析出用户文章页面的最大页面值
      parserArticle(url,html,lazyConn,topic)

    }
  }

  //解析最大页面数
  def parserMaxPage(userId: String,html: String, lazyConn: LazyConnections, topic: String)={

    try {

      val baseArticleListUrl = "https://xueqiu.com/v4/statuses/user_timeline.json?user_id=%s&page=%s"

      val doc = Jsoup.parse(html)
      val data = doc.body().toString
      val result = StringUtil.getMatch(data,"\"maxPage\":(\\d)")

      if(null != result){

        var ArticleListUrl:String =""

        for(num <- 1 to result.toInt){

          ArticleListUrl = baseArticleListUrl.format(userId,num)
          lazyConn.sendTask(topic, StringUtil.getUrlJsonString(60005, ArticleListUrl, 0))

        }
      }

    }catch {

      case exception:Exception =>
        exception.printStackTrace()

    }
  }

  //解析文章列表页面的文章url
  def parserArticleUrl(html:String, lazyConn: LazyConnections, topic: String)={

    val xueqiuHost = "https://xueqiu.com"

    try{

      val json = new JSONObject(html)
      val jsonArr = json.getJSONArray("statuses")

      for(index <- 0 until jsonArr.length()){

        val articleInfo = jsonArr.getJSONObject(index)
        val target = articleInfo.getString("target")
        val articleUrl = xueqiuHost + target

        lazyConn.sendTask(topic, StringUtil.getUrlJsonString(60005, articleUrl, 0))

      }

    }catch {

      case exception:Exception=>
        exception.printStackTrace()

    }
  }

  def parserArticle(url: String,html: String, lazyConn: LazyConnections, topic: String) = {

  }
}
