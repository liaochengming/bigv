package com.kunyan.bigv.parser.zhongjin

import java.io.Serializable
import java.sql.{CallableStatement, PreparedStatement}
import java.util.regex.Pattern

import com.kunyan.bigv.db.LazyConnections
import com.kunyan.bigv.logger.BigVLogger
import com.kunyan.bigv.util.DBUtil._
import com.kunyan.bigv.util.StringUtil
import com.kunyandata.nlpsuit.util.KunyanConf
import org.apache.hadoop.hbase.client.{Get, Result}
import org.jsoup.Jsoup

/**
  * Created by wangzhi on 2016/12/15.
  */
object ZhongJinBlogUpdateParse {

  val articleListUrl = "http://blog\\.cnfol\\.com/index\\.php/article/blogarticlelist/\\w+\\?page=\\d+$"
  val articleUrl = "http://blog\\.cnfol\\.com/\\w+/article/"

  //中金博客解析
  def parse(pageUrl: String, html: String, lazyConn: LazyConnections, topic: String,kyConf: KunyanConf,stopWords: Array[String],classModels: Map[String, Map[String, Map[String, Serializable]]],sentiModels: Map[String, Any],keyWordDict: Map[String, Map[String, Array[String]]],extractSummaryConfiguration: (String, Int)) = {

    try {

      val articleListUrlRegex: Pattern = Pattern.compile(articleListUrl)
      val articleUrlRegex: Pattern = Pattern.compile(articleUrl)


      if (articleListUrlRegex.matcher(pageUrl).find())   parseArticleListPage(pageUrl, html, topic, lazyConn)

      if (articleUrlRegex.matcher(pageUrl).find())   parseArticlePage(pageUrl, html, topic, lazyConn,kyConf: KunyanConf,stopWords,classModels,sentiModels,keyWordDict,extractSummaryConfiguration)


    } catch {
      case e: Exception =>
        e.printStackTrace()
    }

  }

  def parseArticleListPage(pageUrl: String, html: String, topic: String, lazyConn: LazyConnections): Unit = {

    try{

      println("文章列表页解析:"+pageUrl)
      val subUrl = pageUrl.substring(0,pageUrl.length-1)
      val pageNum = pageUrl.split("page=")(1).toInt
      val doc = Jsoup.parse(html, "UTF-8")
      val li = doc.select("div.ArticleBox")

      for(i <- 0 until li.size() ){

        val url = li.get(i).select("a.ArtTit").attr("href")
        val noTop = li.get(i).select("i.top").isEmpty

        val table = lazyConn.getTable("news_detail")
        val get = new Get(url.getBytes)
        val result: Result = table.get(get)

        if(!noTop){

          //文章置顶情况
          if(result.isEmpty){

            println("返回文章url:"+url)
            //文章置顶且数据库中无记录则储存
            lazyConn.sendTask(topic, StringUtil.getUrlJsonString(60007, url, 0))

          }

          if(i == li.size()-1){

            val elem = doc.select("i.CoRed").text()

            if(elem!=null && !"".equals(elem)){

              val num = elem.split("/")(1).toInt

                if(pageNum < num){

                  val page = pageNum+1
                  val url = subUrl+page
                  println("返回文章列表的下一页:"+url)
                  lazyConn.sendTask(topic, StringUtil.getUrlJsonString(60007, url, 0))

                }

              }

            }

        } else {

          //文章非置顶
          if(result.isEmpty){

            println("返回文章url:"+url)
            //文章置顶且数据库中无记录则储存
            lazyConn.sendTask(topic, StringUtil.getUrlJsonString(60007, url, 0))

            if(i == li.size()-1){

              val elem = doc.select("i.CoRed").text()

              if(elem!=null && !"".equals(elem)){

                val num = elem.split("/")(1).toInt

                if(pageNum < num){

                  val page = pageNum+1
                  val url = subUrl+page
                  println("返回文章列表的下一页:"+url)
                  lazyConn.sendTask(topic, StringUtil.getUrlJsonString(60007, url, 0))

                }

              }

            }

          } else {

            return

          }

        }

      }

    } catch {
      case e: Exception =>
        e.printStackTrace()
    }

  }

  //解析文章信息
  def parseArticlePage(pageUrl: String, html: String, topic: String, lazyConn: LazyConnections,kyConf: KunyanConf,stopWords: Array[String],classModels: Map[String, Map[String, Map[String, Serializable]]],sentiModels: Map[String, Any],keyWordDict: Map[String, Map[String, Array[String]]],summaryExtraction: (String, Int) ): Unit = {
    try{

      println("文章页解析:"+pageUrl)

      val doc = Jsoup.parse(html, "UTF-8")
      val userId = pageUrl.split("http://blog\\.cnfol\\.com/")(1).split("/")(0)
      val tableName = "news_detail"
      val platform = 40002
      val platformStr = "中金博客"
      val userName = doc.select("#footer a[href]").get(0).text().trim

      val titleText = doc.select("h1.Head a[href]").get(0).text()
      var title = s"${userName}的观点:"

      if(titleText!=null && !"".equals(titleText)) title = titleText.trim

      var time = System.currentTimeMillis()
      var publish_time = ""
      try{

        publish_time= doc.select("span.MBTime").text().split("\\]")(0).split("\\[")(1)
        time = getTimeStamp(publish_time,"yyyy-MM-dd HH:mm:ss")

      }catch {
        case e: Exception =>
          e.printStackTrace()
      }


      val content = doc.select("div.ArticleCont").text()+doc.select("div.ContentBox").text()
      if(content==null || "".equals(content)) {

        BigVLogger.warn("文章内容不存在，url为:"+pageUrl)
        return

      }


      val transshipmentNumText = doc.select("#transshipmentnum").text().trim
      var transshipmentNum = 0
      if(transshipmentNumText != null && !"".equals(transshipmentNumText))   transshipmentNum = transshipmentNumText.toInt


      val articleCommentNumText = doc.select("#ArticleCommentNum").text().trim
      var articleCommentNum = 0
      if(articleCommentNumText != null && !"".equals(articleCommentNumText))   articleCommentNum = articleCommentNumText.toInt

      val showVotesText = doc.select("#showvotes").text().trim
      var showVotes = 0
      if(showVotesText != null && !"".equals(showVotesText))   showVotes = showVotesText.toInt

      println(s"存储文章信息到hbase,tableName:$tableName,pageUrl:$pageUrl,publish_time:$publish_time")
      insertHbase(tableName , pageUrl ,content ,publish_time, "60007" ,title , lazyConn: LazyConnections)

      println("存储过程1"+pageUrl)
      val cstmtDigest = lazyConn.mysqlVipConn.prepareCall("{call proc_InsertDigestCNFOL(?,?,?,?)}")
      //用户id,标题，推荐量，转载量，评论量，url，时间
      val insertArticle = lazyConn.mysqlVipConn.prepareCall("{call proc_InsertCNFOLNewArticle(?,?,?,?,?,?,?,?)}")
      println("存储过程2"+pageUrl)
      insertCall(insertArticle,userId,title,showVotes,transshipmentNum,articleCommentNum,pageUrl,time,"")
      println("存储mysql表news_info:"+pageUrl)
      val newsMysqlStatement = lazyConn.mysqlNewsConn
        .prepareStatement("insert into news_info(n_id ,type ,platform ,title ,url,news_time,industry," +
          "section,stock,digest,summary,sentiment,updated_time,source) values (?,?,?,?,?,?,?,?,?,?,?,?,?,?)")
      inputDataToSql(lazyConn: LazyConnections,
        cstmtDigest: CallableStatement,
        newsMysqlStatement: PreparedStatement,
        pageUrl: String,
        title: String,
        time: Long,
        content: String,
        platform:Int,
        platformStr:String,
        stopWords: Array[String],
        classModels: scala.Predef.Map[scala.Predef.String, scala.Predef.Map[scala.Predef.String, scala.Predef.Map[scala.Predef.String, java.io.Serializable]]],
        sentiModels: scala.Predef.Map[scala.Predef.String, scala.Any],
        keyWordDict: scala.Predef.Map[scala.Predef.String, scala.Predef.Map[scala.Predef.String, scala.Array[scala.Predef.String]]],
        kyConf: KunyanConf,
        summaryExtraction: (String, Int)
      )

    } catch {
      case e: Exception =>
        e.printStackTrace()
    }

  }

}
