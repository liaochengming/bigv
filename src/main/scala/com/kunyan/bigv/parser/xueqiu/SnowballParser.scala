package com.kunyan.bigv.parser

import com.kunyan.bigv.db.LazyConnections
import com.kunyan.bigv.logger.BigVLogger
import com.kunyan.bigv.util.{StringUtil, DBUtil}
import scala.util.parsing.json.JSON

/**
  * Created by niujiaojiao on 2016/11/16.
  */
object SnowballParser {

  val infourl = "https://xueqiu.com/friendships/groups/members.json?page="
  val specialurl = "https://xueqiu.com/friendships/groups/members.json?page=1&uid="
  val jsonUrl = "https://xueqiu.com/cubes/list.json?user_id="

  def parse(pageUrl: String, html: String, lazyConn: LazyConnections, topic: String) = {

    try {
      if (pageUrl.startsWith(infourl)) {

        println("enter first")
        println(pageUrl)
        infoGet(pageUrl,html, lazyConn,topic)

      } else if (pageUrl.startsWith(jsonUrl)) {
//        println("enter second")
        profitInfo(pageUrl,html, lazyConn,topic)
      }
    } catch {
      case e: Exception =>
        e.printStackTrace()

    }
  }


  def infoGet(pageUrl: String, html: String, lazyConn: LazyConnections, topic: String):Unit = {

    val mysqlConnection = lazyConn.mysqlConn
    mysqlConnection.setAutoCommit(true)
    val useInfosql = mysqlConnection.prepareStatement("insert into  snowball_userInfo(userId,userName,remark,verified_description,stocks_count,status_count, followers_count,friends_count) VALUES (?,?,?,?,?,?,?,?)")

    val userRelationsql  = mysqlConnection.prepareStatement("insert into snowball_user_relationship(user,related_user) VALUES (?,?)")

    val checkSql = mysqlConnection.prepareStatement("select related_user from snowball_user_relationship where user = ?")

    val updateSql = mysqlConnection.prepareStatement("UPDATE snowball_user_relationship SET  related_user = ? where user = ?")

    val firstId = pageUrl.split("uid=")(1).split("&gid")(0)

    checkSql.setString(1,firstId)
    val checkResult = checkSql.executeQuery()

    val jsonInfo = JSON.parseFull(html)
//    println(jsonInfo)
    if (jsonInfo.isEmpty) {

      println("\"JSON parse value is empty,please have a check!\"")

    } else jsonInfo match {

      case Some(mapInfo) =>
        val total = mapInfo.asInstanceOf[Map[String, List[Map[String, Any]]]]
//        val count = total.getOrElse("count","").toString
//        val page = total.getOrElse("page","").toString
        val content = total.getOrElse("users", "").asInstanceOf[List[Map[String, Any]]]
        if(pageUrl.startsWith(specialurl)){
          val maxPage = total.getOrElse("maxPage", "").toString


          println("maxPage is :"+maxPage)


          for(k<-2 until (maxPage.toDouble.toInt+1)){
            val sendInfo = "https://xueqiu.com/friendships/groups/members.json?page=" + k + "&uid=" + firstId + "&gid=0"
            //          println(sendInfo)
            lazyConn.sendTask(topic, StringUtil.getUrlJsonString(60005, sendInfo, 0))//发送url
          }

        }

        var set = Set[String]()
        for(index<-content.indices){
          val info = content(index)
          val userId = info.getOrElse("id","")
          set = set++Set(userId.toString)
        }

        var relationId = ""

        for(i<- content.indices) {
          val info = content(i)
          val userId = info.getOrElse("id", "").asInstanceOf[Double].toLong.toString
//          println(userId)
          var name = info.getOrElse("screen_name", "")
          if(name == null){
            name = ""
          }

          var verified = info.getOrElse("verified", "").toString
          var description = "null"
          if(verified == "true"){
            description = info.getOrElse("verified_description","").toString
          }else{
            verified = "False"
          }

          val stocksCnt = info.getOrElse("stocks_count", "").toString
          val statusCnt = info.getOrElse("status_count","").toString
          val followersCnt = info.getOrElse("followers_count", "").toString
          val friendsCnt = info.getOrElse("friends_count", "").toString
//          val profit = info.getOrElse("id", "").toString

          if(friendsCnt.toString.toDouble < 500 && followersCnt.toString.toDouble>5000){
            relationId = relationId + userId +","
            DBUtil.insert(useInfosql,userId, name, verified,description,stocksCnt,statusCnt,followersCnt,friendsCnt) //插入数据库
            val sendUrl = jsonUrl + userId
//            println(sendUrl)
            lazyConn.sendTask(topic, StringUtil.getUrlJsonString(60005, sendUrl, 0))//发送url
          }
          //第一页的id url 拼接
          if(userId != null && friendsCnt.toString.toDouble < 500 && followersCnt.toString.toDouble>3000){
            val sendUrl = specialurl + userId + "&gid=0"
            lazyConn.sendTask(topic, StringUtil.getUrlJsonString(60005, sendUrl, 0)) //发送url
          }
          //        println(s"$userId +$name + $remark+$verified+$stocksCnt+$followersCnt+$friendsCnt")
        }

        relationId = relationId.stripSuffix(",")

        if(relationId != ""){

          if(checkResult.next()){

            val userBefore = checkResult.getString(1)
            println("firstID: "+ firstId + "userBefore: "+userBefore + "relationId: "+relationId)
            relationId = relationId + ","+userBefore
            println("to insert data "+relationId)
            updateSql.setString(1,relationId)
            updateSql.setString(2,firstId)
            updateSql.executeUpdate()

          } else {
            println("this is new : insert data :"+ firstId + "|"+relationId)
            DBUtil.insert(userRelationsql, firstId, relationId)
          }

        }

      case None => println("Parsing failed!")
      case other => println("Unknown data structure :" + other)
    }

  }


  def profitInfo(pageUrl: String, html: String, lazyConn: LazyConnections, topic: String):Unit ={

    val mysqlConnection = lazyConn.mysqlConn
    mysqlConnection.setAutoCommit(true)
    val jsonProfit = JSON.parseFull(html)

//      val setSql = mysqlConnection.prepareStatement("UPDATE snowball_user_relationship SET  profit = ? where user = ?")
//    val idSql = mysqlConnection.prepareStatement("select * from snowball_user_relationship where user = ?")
val userProfitsql  = mysqlConnection.prepareStatement("insert into snowball_profit(userId,profit) VALUES (?,?)")
//    BigVLogger.warn("this is pageurl:"+pageUrl)
//    println("this is html:"+html)
    val userId = pageUrl.split("user_id=")(1)
//    println("this is userId:"+userId)
    var profitTotal = ""
    if(jsonProfit.isEmpty){
      BigVLogger.warn("\"JSON parse value is empty,please have a check!\"")
    }else jsonProfit match {
      case Some(profitInfo) =>
        val total = profitInfo.asInstanceOf[Map[String, List[Map[String, Any]]]]
        val list = total.getOrElse("list", "").asInstanceOf[List[Map[String, Any]]]
        for(j<- list.indices){
          val str = list(j)
//          val id =  str.getOrElse("owner_id","").toString
          val profit = str.getOrElse("net_value", "").toString
//          println(profit)
          profitTotal= profitTotal+profit+","
        }
      case None =>  BigVLogger.warn("Parsing failed!")
      case other =>  BigVLogger.warn("Unknown data structure :" + other)
    }
    profitTotal = profitTotal.stripSuffix(",")

    if(profitTotal == ""){
      profitTotal = "null"
    }
//    BigVLogger.warn(profitTotal)
    DBUtil.insert(userProfitsql,userId, profitTotal)
//    idSql.setString(1,userId)
//    val blFlag = idSql.executeQuery().next()
//    if(!blFlag){
//      println("this userID not exist in the table")
//    } else {
//      println("exist Id in the table ")
//    }
//    println(" this is profitTotal："+ profitTotal)
//    setSql.setString(1,profitTotal)
//    setSql.setString(2,userId)
//    setSql.executeUpdate()

  }


}


