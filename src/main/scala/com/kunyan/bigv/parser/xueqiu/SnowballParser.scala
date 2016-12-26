package com.kunyan.bigv.parser.xueqiu

import com.kunyan.bigv.config.Platform
import com.kunyan.bigv.db.LazyConnections
import com.kunyan.bigv.logger.BigVLogger
import com.kunyan.bigv.util.{StringUtil, DBUtil}
import org.json.JSONObject
import scala.util.parsing.json.JSON

/**
 * Created by niujiaojiao on 2016/11/16.
 * 雪球大V的top100选择阶段的大V信息解析
 */
object SnowballParser {

  val infoUrl = "https://xueqiu.com/friendships/groups/members.json?page="
  val specialUrl = "https://xueqiu.com/friendships/groups/members.json?page=1&uid="
  val jsonUrl = "https://xueqiu.com/cubes/list.json?user_id="
  val articleListUrlPart = "https://xueqiu.com/v4/statuses/user_timeline.json"
  val blackUserList = "['7437424816', '8152922548', '7912472055', '1262087548', '1689987310', '8606183498', '1107854878', '3771154167', '1676206424', '5171159182', '4319549361', '9320604669', '9046578402', '2552920054', '3746414875', '5214718015', '9189729049', '1333325987', '1777865136', '6847723845', '1253839828', '4234422658', '5215025851', '5449854060', '4226803442', '3966435964', '8818667120', '1876906471', '2994748381', '7886851669', '8918534600', '5401654358', '2186070899', '1784122336', '3904790550', '5599676754', '4660283758', '7237463580', '1930109830', '6592559979', '7786512276', '7802072887', '7634172892', '1987095818', '6547599794', '9243245648', '2083557747', '7122164210', '2724224241', '7718291643', '3136277248', '6614630997', '1139467579', '9796081404', '3648447359', '3765323641', '4642157440', '9245209147', '2371424990', '6093797154', '2190238829', '9939258427', '1344372584', '3214651458', '7130238133', '5899108858', '4119858385', '2326858367', '9143585413', '7748174714', '4907812515', '8823164979', '1685170838', '5716890709', '1591127948', '4046745784', '6717445458', '9049012592', '1680628438', '1176317596', '2808906508', '8365638673', '8103323056', '3980090908', '3055228260', '7563843313', '1605334241']"

  def parse(pageUrl: String, html: String, lazyConn: LazyConnections, topic: String) = {

    BigVLogger.warn("雪球 select url => " + pageUrl)

    try {

      if (pageUrl.startsWith(infoUrl)) {
        infoGet(pageUrl, html, lazyConn, topic)
      } else if (pageUrl.startsWith(jsonUrl)) {
        profitInfo(pageUrl, html, lazyConn, topic)
      } else if (pageUrl.startsWith(articleListUrlPart)) {
        parserArticleUrl(pageUrl, html, lazyConn, topic)
      }

    } catch {

      case e: Exception =>
        e.printStackTrace()

    }
  }


  def infoGet(pageUrl: String, html: String, lazyConn: LazyConnections, topic: String): Unit = {

    try{

      val mysqlConnection = lazyConn.mysqlConn
      mysqlConnection.setAutoCommit(true)
      val useInfoSql = mysqlConnection.prepareStatement("insert into  snowball_userInfo(userId,userName,remark,verified_description,stocks_count,status_count, followers_count,friends_count) VALUES (?,?,?,?,?,?,?,?)")

      val userRelationSql = mysqlConnection.prepareStatement("insert into snowball_user_relationship(user,related_user) VALUES (?,?)")

      val checkUserSql = mysqlConnection.prepareStatement("select * from snowball_userInfo where userId = ?")

      val checkSql = mysqlConnection.prepareStatement("select related_user from snowball_user_relationship where user = ?")

      val updateSql = mysqlConnection.prepareStatement("UPDATE snowball_user_relationship SET  related_user = ? where user = ?")

      val firstId = pageUrl.split("uid=")(1).split("&gid")(0)

      checkSql.setString(1, firstId)
      val checkResult = checkSql.executeQuery()
      val jsonInfo = JSON.parseFull(html)

      if (jsonInfo.isEmpty) {

        BigVLogger.error("雪球 select JSON parse value is empty,please have a check! url => " + pageUrl)

      } else jsonInfo match {

        case Some(mapInfo) =>
          val total = mapInfo.asInstanceOf[Map[String, List[Map[String, Any]]]]
          val content = total.getOrElse("users", "").asInstanceOf[List[Map[String, Any]]]

          if (pageUrl.startsWith(specialUrl)) {

            val maxPage = total.getOrElse("maxPage", "").toString

            for (k <- 2 until (maxPage.toDouble.toInt + 1)) {

              val sendInfo = "https://xueqiu.com/friendships/groups/members.json?page=" + k + "&uid=" + firstId + "&gid=0"
              lazyConn.sendTask(topic, StringUtil.getUrlJsonString(Platform.SNOW_BALL.id, sendInfo, 0)) //发送url

            }
          }

          var set = Set[String]()
          for (index <- content.indices) {

            val info = content(index)
            val userId = info.getOrElse("id", "")
            set = set ++ Set(userId.toString)

          }

          var relationId = ""

          for (i <- content.indices) {

            val info = content(i)
            val userId = info.getOrElse("id", "").asInstanceOf[Double].toLong.toString
            var name = info.getOrElse("screen_name", "")

            if (name == null) {
              name = ""
            }

            var verified = info.getOrElse("verified", "").toString
            var description = "null"

            if (verified == "true") {
              description = info.getOrElse("verified_description", "").toString
            } else {
              verified = "false"
            }

            val stocksCnt = info.getOrElse("stocks_count", "").toString
            val statusCnt = info.getOrElse("status_count", "").toString

            //粉丝数
            val followersCnt = info.getOrElse("followers_count", "").toString

            //关注数
            val friendsCnt = info.getOrElse("friends_count", "").toString

            if (followersCnt.toString.toDouble > 10000) {

              relationId = relationId + userId + ","
              checkUserSql.setString(1,userId)
              val rs = checkUserSql.executeQuery()

              if(!rs.next()){
                DBUtil.insert(useInfoSql, userId, name, verified, description, stocksCnt, statusCnt, followersCnt, friendsCnt) //插入数据库
              }

              if(!blackUserList.contains(userId)){

                val sendUrl = jsonUrl + userId
                val articleListUrl = "https://xueqiu.com/v4/statuses/user_timeline.json?user_id=" + userId + "&page=1"

                lazyConn.sendTask(topic, StringUtil.getUrlJsonString(Platform.SNOW_BALL.id, articleListUrl, 0))
                lazyConn.sendTask(topic, StringUtil.getUrlJsonString(Platform.SNOW_BALL.id, sendUrl, 0)) //发送url
              }
            }

            //第一页的id url 拼接
            if (userId != null && followersCnt.toString.toDouble > 10000 && friendsCnt.toDouble < 100) {

              val sendUrl = specialUrl + userId + "&gid=0"
              lazyConn.sendTask(topic, StringUtil.getUrlJsonString(Platform.SNOW_BALL.id, sendUrl, 0)) //发送url

            }
          }

          relationId = relationId.stripSuffix(",")

          if (relationId != "") {

            if (checkResult.next()) {

              val userBefore = checkResult.getString(1)
              println("firstID: " + firstId + "userBefore: " + userBefore + "relationId: " + relationId)

              relationId = relationId + "," + userBefore
              println("to insert data " + relationId)
              updateSql.setString(1, relationId)
              updateSql.setString(2, firstId)
              updateSql.executeUpdate()

            } else {

              println("this is new : insert data :" + firstId + "|" + relationId)
              DBUtil.insert(userRelationSql, firstId, relationId)

            }

          }

        case None => BigVLogger.error("雪球 select Parsing failed! url => " + pageUrl)

        case other => BigVLogger.error("雪球 select Unknown data struct! url => :" + pageUrl)
      }

    }catch {

      case exception:Exception =>
        exception.printStackTrace()

    }

  }

  //收益的信息
  def profitInfo(pageUrl: String, html: String, lazyConn: LazyConnections, topic: String): Unit = {

    try {

      val mysqlConnection = lazyConn.mysqlConn
      mysqlConnection.setAutoCommit(true)
      val jsonProfit = JSON.parseFull(html)

      val userProfitSql = mysqlConnection.prepareStatement("insert into snowball_profit(userId,profit) VALUES (?,?)")
      val userId = pageUrl.split("user_id=")(1)
      var profitTotal = ""

      if (jsonProfit.isEmpty) {
        BigVLogger.warn("\"JSON parse value is empty,please have a check!\"")
      } else jsonProfit match {

        case Some(profitInfo) =>
          val total = profitInfo.asInstanceOf[Map[String, List[Map[String, Any]]]]
          val list = total.getOrElse("list", "").asInstanceOf[List[Map[String, Any]]]

          for (j <- list.indices) {

            val str = list(j)
            val profit = str.getOrElse("net_value", "").toString
            profitTotal = profitTotal + profit + ","

          }

        case None =>
          BigVLogger.warn("Parsing failed!")

        case other =>
          BigVLogger.warn("Unknown data structure :" + other)
      }

      profitTotal = profitTotal.stripSuffix(",")

      if (profitTotal == "") {
        profitTotal = "null"
      }

      DBUtil.insert(userProfitSql, userId, profitTotal)

    } catch {
      case exception: Exception =>
        exception.printStackTrace()
    }

  }


  /**
   * 解析文章列表的文章url
   * @param pageUrl 文章列表的url
   * @param html 文章列表的页面数据（json格式）
   * @param lazyConn 连接
   * @param topic kafka消息发送的topic
   */
  def parserArticleUrl(pageUrl: String, html: String, lazyConn: LazyConnections, topic: String) = {

    val xueQiuHost = "https://xueqiu.com"

    val mysqlConnection = lazyConn.mysqlConn
    mysqlConnection.setAutoCommit(true)
    val articleInfoSql = mysqlConnection.prepareStatement("insert into  snowball_articleInfo(userId,url,created_at,retweet_count,reply_count, fav_count,view_count) VALUES (?,?,?,?,?,?,?)")


    try {

      val json = new JSONObject(html)
      val jsonArr = json.getJSONArray("statuses")
      val maxPage = json.getInt("maxPage")

      import scala.util.control.Breaks._
      breakable {

        for (index <- 0 until jsonArr.length()) {

          val articleInfo = jsonArr.getJSONObject(index)

          //文章的用户id
          val userId = articleInfo.getString("user_id")

          //文章的url
          val target = articleInfo.getString("target")
          val articleUrl = xueQiuHost + target

          //文章发表时间
          val createdAt = articleInfo.getLong("created_at")

          //转载量
          val retCount = articleInfo.getString("retweet_count")

          //回复量
          val replyCount = articleInfo.getString("reply_count")

          //收藏量
          val favCount = articleInfo.getString("fav_count")

          //阅读量
          val viewCount = articleInfo.getString("view_count")

          //置顶标签（1为置顶文章，0为非指定文章）
          val mark = articleInfo.getString("mark")

          if (mark == "1") {

            //判断是否是4个月内的
            if (!isFourMonthAgo(createdAt)) {

              //写到数据库
              DBUtil.insert(articleInfoSql, userId, articleUrl, createdAt.toString, retCount, replyCount, favCount, viewCount)
            }

          } else {

            if (!isFourMonthAgo(createdAt)) {

              //写到数据库
              DBUtil.insert(articleInfoSql, userId, articleUrl, createdAt.toString, retCount, replyCount, favCount, viewCount)
            } else {
              break()
            }

          }
        }

        //将下页文章列表的url发送给服务器
        val pageNum = StringUtil.getMatch(pageUrl, "page=(\\d+)").toInt
        val nextPageNum = pageNum + 1

        if(nextPageNum > maxPage){
          break()
        }

        val nextPageUrl = pageUrl.replace("page=" + pageNum, "page=" + nextPageNum)
        lazyConn.sendTask(topic, StringUtil.getUrlJsonString(Platform.SNOW_BALL.id, nextPageUrl, 0))

      }
    } catch {

      case exception: Exception =>
        exception.printStackTrace()

    }
  }

  /**
   * 判断时间是否4个月之前
   * @param createdAt 要判断的时间
   * @return true
   */
  def isFourMonthAgo(createdAt: Long): Boolean = {

    val currentTime = System.currentTimeMillis()
    currentTime - createdAt >  5270400000L

  }


}


