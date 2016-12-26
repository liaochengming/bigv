package com.kunyan.bigv.parser.zhongjin

import java.util.Calendar
import java.util.regex.Pattern

import com.kunyan.bigv.db.LazyConnections
import com.kunyan.bigv.pojo.zhongjin.UserInfo
import com.kunyan.bigv.util.{DBUtil, StringUtil}
import org.jsoup.Jsoup
import org.jsoup.select.Elements

/**
  * Created by wangzhi on 2016/12/9
  * 中金博客从一个入口url解析筛选出top100用户
  */
object ZhonJinBlogParser {

  val myfocusFirstPage = "myfocus/friend\\?type=&&p=1$"
  val myfocusPage = "myfocus/friend\\?type=&&p=\\d+$"
  val hostPage = "http://blog\\.cnfol\\.com/\\w+$"
  val articleListUrl = "http://blog\\.cnfol\\.com/\\w+/archive/2016/\\d+/\\d+$"
  val articleListFirstUrl = "http://blog\\.cnfol\\.com/\\w+/archive/2016/\\d+/1$"
  val articleUrl = "http://blog\\.cnfol\\.com/\\w+/article/"
  val articleIdUrl = "http://blog\\.cnfol\\.com/articleclick/art/\\d+$"
  //中金博客解析
  def parse(pageUrl: String, html: String, lazyConn: LazyConnections, topic: String) = {

    try {

      val myfocusFirstPageRegex: Pattern = Pattern.compile(myfocusFirstPage)
      val myfocusPageRegex: Pattern = Pattern.compile(myfocusPage)
      val hostPageRegex: Pattern = Pattern.compile(hostPage)
      val articleListFirstUrlRegex: Pattern = Pattern.compile(articleListFirstUrl)
      val articleListUrlRegex: Pattern = Pattern.compile(articleListUrl)
      val articleUrlRegex: Pattern = Pattern.compile(articleUrl)
      val articleIdUrlRegex: Pattern = Pattern.compile(articleIdUrl)

      if (myfocusFirstPageRegex.matcher(pageUrl).find())   returnRemainAndHostPageUrl(pageUrl, html, topic, lazyConn)

      if (myfocusPageRegex.matcher(pageUrl).find())  parseFocusPage(pageUrl, html, topic, lazyConn)

      if (hostPageRegex.matcher(pageUrl).find())   parseHostPage(pageUrl, html, topic, lazyConn)

      if (articleListFirstUrlRegex.matcher(pageUrl).find())   returnArticleRemainPageUrl(pageUrl, html, topic, lazyConn)

      if (articleListUrlRegex.matcher(pageUrl).find())   parseArticleListPage(pageUrl, html, topic, lazyConn)

      if (articleUrlRegex.matcher(pageUrl).find())   parseArticlePage(pageUrl, html, topic, lazyConn)

      if (articleIdUrlRegex.matcher(pageUrl).find())   parseArticleIdPage(pageUrl, html, topic, lazyConn)


    } catch {
      case e: Exception =>
        e.printStackTrace()
    }

  }

  //当传入的url是关注列表的第一页返回二至尾页的url
  def returnRemainAndHostPageUrl(pageUrl: String, html: String, topic: String, lazyConn: LazyConnections): Unit = {
    try{

      println("关注页面的第一页："+pageUrl)
      val subUrl = pageUrl.substring(0,pageUrl.length-1)
      val doc = Jsoup.parse(html, "UTF-8")
      val elem = doc.select("i.CoRed").text()
      if(elem!=null && !"".equals(elem)){
        val num = elem.split("/")(1).toInt
        for(i <- 2 to num){

          val returnUrl = subUrl + i
          println("返回关注列表的第一页返回二至尾页的url:"+returnUrl)
          lazyConn.sendTask(topic, StringUtil.getUrlJsonString(60007, returnUrl, 0))

        }
      }

    } catch {
      case e: Exception =>
        e.printStackTrace()
    }

  }

  //解析关注页面
  def parseFocusPage(pageUrl: String, html: String, topic: String, lazyConn: LazyConnections): Unit = {

    try {

        println("1.用户关注页面解析:"+pageUrl)
        val userId = pageUrl.split("/")(3)
        var userList =  List[UserInfo]()
        val doc = Jsoup.parse(html, "UTF-8")
        val elements: Elements = doc.select("div.DetailBox")

        for( i <- 0 until  elements.size() ){

          val focusUserName = elements.get(i).select("p.FollowInfo span").get(0).select("a").attr("href").split("/")(3)
          val fanUserName = elements.get(i).select("p.FollowInfo span").get(1).select("a").attr("href").split("/")(3)

          if(focusUserName.equals(fanUserName) && !"returnbolg".equals(focusUserName)){

            val focusNum = elements.get(i).select("p.FollowInfo span").get(0).select("em").text().toInt
            val fanNum = elements.get(i).select("p.FollowInfo span").get(1).select("em").text().toInt
            val user = UserInfo(focusUserName,focusNum,fanNum)
            userList = userList :+ user

          }

        }

      //储存所有满足调价用户id以，分隔
      val users = new StringBuffer("")

      for(user <- userList){

        //1.返回大v备选用户（条件关注数<500,粉丝数>2000）的关注页面url
        //if (user.focusNum < 1000 && user.fansNum > 500){
        if (user.focusNum < 500 && user.fansNum > 2000){

          val url = s"http://blog.cnfol.com/${user.userId}/myfocus/friend?type=&&p=1"
          println("返回大v备选用户（条件关注数<500,粉丝数>2000）的关注页面url"+url)
          lazyConn.sendTask(topic, StringUtil.getUrlJsonString(60007, url, 0))

        }

        //2.储存用户关系网（条件关注数<1000,粉丝数>3000）并返回用户主页url和文章列表页url
        if(user.focusNum < 1000 && user.fansNum > 3000) {
          val cal = Calendar.getInstance()
          val month = cal.get(Calendar.MONTH) + 1
          val hostUrl = "http://blog.cnfol.com/"+user.userId
          println("（条件关注数<1000,粉丝数>3000）并返回用户主页url:"+hostUrl)
          lazyConn.sendTask(topic, StringUtil.getUrlJsonString(60007, hostUrl, 0))

          for (i <- month-1 to month){
            if(i<=0){

              val articleUrl = s"http://blog.cnfol.com/${user.userId}/archive/2016/${i+12}/1"
              println("（条件关注数<1000,粉丝数>3000）并返回文章列表页url"+articleUrl)
              lazyConn.sendTask(topic, StringUtil.getUrlJsonString(60007, articleUrl, 0))

            } else {

              val articleUrl = s"http://blog.cnfol.com/${user.userId}/archive/2016/$i/1"
              println("（条件关注数<1000,粉丝数>3000）并返回文章列表页url"+articleUrl)
              lazyConn.sendTask(topic, StringUtil.getUrlJsonString(60007, articleUrl, 0))

            }

          }

          users.append(user.userId).append(",")

        }

      }

        val mysqlConnection = lazyConn.mysqlConn
        mysqlConnection.setAutoCommit(true)
        val queryPS = mysqlConnection.prepareStatement("select * from zhongjin_user_nets_info where user_id = ?")
        queryPS.setString(1, userId)
        val rs = queryPS.executeQuery()
        //14天一更新用户关系网需清空
        if(rs.next()){

          users.append(rs.getString("focus_list"))
          println(s"储存用户关系网，userid:$userId,focus_list:$users")
          val updatePS = mysqlConnection.prepareStatement("update zhongjin_user_nets_info set focus_list = ? where user_id = ?")
          DBUtil.insert(updatePS, users.toString , userId)

        } else {

          val len: Int = users.length
          if (len > 0) users.deleteCharAt(len - 1)
          println(s"储存用户关系网，userid:$userId,focus_list:$users")
          val insertPS = mysqlConnection.prepareStatement("insert into zhongjin_user_nets_info(user_id,focus_list) values (?,?)")
          DBUtil.insert(insertPS, userId, users.toString)

        }



    } catch {
      case e: Exception =>
        e.printStackTrace()
    }

  }

  //解析用户主页
  def parseHostPage(pageUrl: String, html: String, topic: String, lazyConn: LazyConnections): Unit = {

    try{

      println("2.解析用户主页:"+pageUrl)
      val userId = pageUrl.split("http://blog\\.cnfol\\.com/")(1)
      val doc = Jsoup.parse(html, "UTF-8")
      val userName = doc.select("div.BloggerName").select("strong").text()
      val focusNum = doc.select("ul.BloggerAtten li").get(0).select("p").get(0).text().trim.toInt
      val fansNum = doc.select("ul.BloggerAtten li").get(1).select("p").get(0).text().trim.toInt
      val articleNum = doc.select("ul.BloggerAtten li").get(2).select("p").get(0).text().trim.toInt
      val isAuthentication = if(doc.select("i.ApproveIco")!=null) "true" else "false"
      println(s"储存bigv备选用户信息，userId:$userId,userName:$userName,focusNum:$focusNum,fansNum:$fansNum,articleNum:$articleNum,isAuthentication:$isAuthentication")
      val mysqlConnection = lazyConn.mysqlConn
      mysqlConnection.setAutoCommit(true)

      val queryPS = mysqlConnection.prepareStatement("select * from zhongjin_user_info where user_id = ?")
      queryPS.setString(1, userId)
      val rs = queryPS.executeQuery()

      if(rs.next()){

        val updatePS = mysqlConnection.prepareStatement("update zhongjin_user_info set user_name = ?,focus_num = ?,fans_num = ?,article_num = ?,is_authentication = ? where user_id = ?")
        DBUtil.insert(updatePS, userName,  focusNum, fansNum, articleNum, isAuthentication, userId)

      } else {

        val userPS = mysqlConnection
          .prepareStatement("insert into zhongjin_user_info(user_id , user_name , focus_num , fans_num , article_num, is_authentication ) values (?,?,?,?,?,?)")
        DBUtil.insert(userPS, userId, userName,  focusNum, fansNum, articleNum, isAuthentication)

      }



    } catch {
      case e: Exception =>
        e.printStackTrace()
    }
  }


  //当传入的url是文章列表的第一页返回二至尾页的url
  def returnArticleRemainPageUrl(pageUrl: String, html: String, topic: String, lazyConn: LazyConnections): Unit = {
    try{

      println("文章列表第一页:"+pageUrl)
      val subUrl = pageUrl.substring(0,pageUrl.length-1)
      val doc = Jsoup.parse(html, "UTF-8")
      val elem = doc.select("i.CoRed").text()

      if(elem!=null && !"".equals(elem)){

        val num = elem.split("/")(1).toInt

        for(i <- 2 to num){

          val returnUrl = subUrl+ i
          println("返回文章列表页:"+ returnUrl)
          lazyConn.sendTask(topic, StringUtil.getUrlJsonString(60007, returnUrl, 0))

        }

      }

    } catch {
      case e: Exception =>
        e.printStackTrace()
    }

  }

  //文章列表的解析：获取文章信息
  def parseArticleListPage(pageUrl: String, html: String, topic: String, lazyConn: LazyConnections): Unit = {

    try {


      println("4.文章列表页解析:"+pageUrl)
      val userId = pageUrl.split("http://blog\\.cnfol\\.com/")(1).split("/")(0)
      val doc = Jsoup.parse(html, "UTF-8")
      val li = doc.select("ul.ArticleLst li")
      val mysqlConnection = lazyConn.mysqlConn
      mysqlConnection.setAutoCommit(true)

      for(i <- 0 until li.size() ){

        val date = li.get(i).select("span.Date").text().split("\\)")(1).trim
        val url = li.get(i).select("a[href]").attr("href")
        val title = li.get(i).select("a[href]").text()
        val articleId = url.split("-")(1).split("\\.")(0).toInt
        println(s"存储文章信息,userId:$userId,,articleId:$articleId,title:$title,url:$url,date:$date")
        val articlePS = mysqlConnection.prepareStatement("insert into zhongjin_user_article_info (article_id , user_id , title , date , url ) values (?,?,?,?,?)")
        DBUtil.insert(articlePS, articleId , userId , title , date , url)

        println("返回文章url:"+url)
        lazyConn.sendTask(topic, StringUtil.getUrlJsonString(60007, url, 0))

        val returnUrl = s"http://blog.cnfol.com/articleclick/art/$articleId"
        println("返回文章阅读量url:"+returnUrl)
        lazyConn.sendTask(topic, StringUtil.getUrlJsonString(60007, returnUrl, 0))

      }

    } catch {
      case e: Exception =>
        e.printStackTrace()
    }

  }

  //文章的解析：获取文章信息
  def parseArticlePage(pageUrl: String, html: String, topic: String, lazyConn: LazyConnections): Unit = {

    try {

      println("5.文章页解析:"+pageUrl)
      val articleId = pageUrl.split("-")(1).split("\\.")(0).trim.toInt
      val mysqlConnection = lazyConn.mysqlConn
      mysqlConnection.setAutoCommit(true)
      val doc = Jsoup.parse(html, "UTF-8")

      val transshipmentNumText = doc.select("#transshipmentnum").text().trim
      var transshipmentNum = 0
      if(transshipmentNumText != null && !"".equals(transshipmentNumText))   transshipmentNum = transshipmentNumText.toInt


      val articleCommentNumText = doc.select("#ArticleCommentNum").text().trim
      var articleCommentNum = 0
      if(articleCommentNumText != null && !"".equals(articleCommentNumText))   articleCommentNum = articleCommentNumText.toInt

      val showVotesText = doc.select("#showvotes").text().trim
      var showVotes = 0
      if(showVotesText != null && !"".equals(showVotesText))   showVotes = showVotesText.toInt

      println(s"存储文章信息,articleId:$articleId,transshipmentNum:$transshipmentNum,articleCommentNum:$articleCommentNum,showVotes:$showVotes")
      val articlePS = mysqlConnection
        .prepareStatement("update zhongjin_user_article_info set transshipment_num =?,article_comment_num = ?,show_votes = ? where article_id = ?")
      DBUtil.insert(articlePS , transshipmentNum , articleCommentNum , showVotes , articleId )

    } catch {
      case e: Exception =>
        e.printStackTrace()
    }

  }

  //阅读量解析：获取文章信息
  def parseArticleIdPage(pageUrl: String, html: String, topic: String, lazyConn: LazyConnections): Unit = {

    try {

      println("6.解析阅读量页面:"+pageUrl)
      val articleId = pageUrl.split("http://blog.cnfol.com/articleclick/art/")(1).trim().toInt
      val mysqlConnection = lazyConn.mysqlConn
      mysqlConnection.setAutoCommit(true)
      val doc = Jsoup.parse(html, "UTF-8").html()
      var readAmount = 0
      try {

      if(doc != null && !"".equals(doc))   readAmount = doc.split(";")(0).split("html\\(\"")(1).split("\"")(0).trim.toInt

      } catch {
        case e: Exception =>
          e.printStackTrace()
      }
      println(s"存储文章信息,readAmount:$readAmount")
      val articlePS = mysqlConnection
        .prepareStatement("update zhongjin_user_article_info set read_amount =? where article_id = ?")
      DBUtil.insert(articlePS, readAmount , articleId )

    } catch {
      case e: Exception =>
        e.printStackTrace()
    }

  }

}



