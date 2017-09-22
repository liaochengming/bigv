package com.kunyan.bigv.util

import java.sql.{CallableStatement, PreparedStatement}
import java.text.SimpleDateFormat
import java.util.Locale

import com.hankcs.hanlp.HanLP
import com.ibm.icu.text.CharsetDetector
import com.kunyan.bigv.db.LazyConnections
import com.kunyan.bigv.logger.BigVLogger
import com.kunyan.nlp.KunLP
import com.kunyan.nlp.task.NewsProcesser
import com.nlp.util.EasyParser
import org.apache.hadoop.hbase.client.{Get, Put}
import org.apache.hadoop.hbase.util.Bytes
import org.apache.spark.SparkEnv


/**
 * Created by niujiaojiao on 2016/11/16.
 *
 */
object DBUtil {

  def getDateString: String = getDateString(System.currentTimeMillis())

  def getDateString(timestamp: Long): String = new SimpleDateFormat("yyyyMMddHHmmss").format(timestamp)

  def getTimeStamp(dateStr: String, formatStr: String): Long = {

    val sdf = new SimpleDateFormat(formatStr)
    sdf.parse(dateStr).getTime

  }

  /**
   * 将字符串类型时间转换成long类型的时间戳
   * @param time 字符串类型的时间
   * @param format 时间的类型
   * @return long 类型的时间戳
   */
  def getLongTimeStamp(time: String, format: String): Long = {
    val loc = new Locale("en")
    val sdf = new SimpleDateFormat(format, loc)
    val date = sdf.parse(time)
    date.getTime
  }

  /**
   * 根据表名和rowkey从hbase中获取数据
   *
   * @param tableName 表名
   * @param rowkey    索引
   * @param lazyConn  连接容器
   * @return (url, html)
   */
  def query(tableName: String, rowkey: String, lazyConn: LazyConnections): (String, String) = {

    val table = lazyConn.getTable(tableName)
    val get = new Get(rowkey.getBytes)

    try {

      val url = table.get(get).getValue(Bytes.toBytes("basic"), Bytes.toBytes("url"))
      val content = table.get(get).getValue(Bytes.toBytes("basic"), Bytes.toBytes("content"))

      if (url == null) {
        println("hbase url == null")
      }
      if (content == null) {
        println("hbase content == null")
      }


      if (url == null && content == null) {
        BigVLogger.error(s"Get empty data by this table: $tableName and rowkey: $rowkey")
        return null
      }

      val encoding = new CharsetDetector().setText(content).detect().getName

      (new String(url, "UTF-8"), new String(content, encoding))
    } catch {

      case e: Exception =>
        BigVLogger.exception(e)
        null

    }

  }


  /**
   * 插入数据到hbase
   * @param tableName 表名
   * @param rowKey url
   * @param content 正文
   * @param publish_time 文章时间
   * @param platform 平台号
   * @param title 标题
   * @param lazyConn 连接
   */
  def insertHbase(tableName: String, rowKey: String, content: String, publish_time: String,
                  platform: String, title: String, lazyConn: LazyConnections) = {

    val table = lazyConn.getTable(tableName)
    val put = new Put(rowKey.getBytes)

    put.addColumn("basic".getBytes, "content".getBytes, content.getBytes)
    put.addColumn("basic".getBytes, "time".getBytes, publish_time.getBytes)
    put.addColumn("basic".getBytes, "platform".getBytes, platform.getBytes)
    put.addColumn("basic".getBytes, "title".getBytes, title.getBytes)

    table.put(put)


  }

  //插入数据库操作
  def insert(prep: PreparedStatement, params: Any*): Boolean = {

    try {

      for (i <- params.indices) {

        val param = params(i)

        param match {

          case param: String =>
            prep.setString(i + 1, param)
          case param: Int =>
            prep.setInt(i + 1, param)
          case param: Boolean =>
            prep.setBoolean(i + 1, param)
          case param: Long =>
            prep.setLong(i + 1, param)
          case param: Double =>
            prep.setDouble(i + 1, param)
          case _ =>
            BigVLogger.error("Unknown Type")

        }
      }
      prep.executeUpdate

      true
    } catch {

      case e: Exception =>
        BigVLogger.exception(e)
        false
    }

  }


  def insertCall(call: CallableStatement, params: Any*): Boolean = {

    try {

      for (i <- params.indices) {

        val param = params(i)

        param match {
          case param: String =>
            call.setString(i + 1, param)
          case param: Int =>
            call.setInt(i + 1, param)
          case param: Boolean =>
            call.setBoolean(i + 1, param)
          case param: Long =>
            call.setLong(i + 1, param)
          case param: Double =>
            call.setDouble(i + 1, param)
          case param: Short =>
            call.setShort(i + 1, param)
          case _ =>
            BigVLogger.error("Unknown Type")
        }
      }

      call.executeUpdate

      true

    } catch {

      case e: Exception =>
        BigVLogger.error("向MySQL插入数据失败")
        BigVLogger.exception(e)

        false
    }

  }


  /**
   * 将文章信息写分析之后 写到mysql
   * @param lazyConn 连接
   * @param cstmtDigest 存储过程的sql语句
   * @param newsMysqlStatement mysql的存储语句
   * @param url 文章网址
   * @param title 文章标题
   * @param time 文章时间
   * @param content 文章内容
   * @param platform 平台id
   * @param platformStr 平台字符串
   */
  def inputDataToSql(lazyConn: LazyConnections,
                     cstmtDigest: CallableStatement,
                     newsMysqlStatement: PreparedStatement,
                     url: String,
                     title: String,
                     time: Long,
                     content: String,
                     platform: Int,
                     platformStr: String,
                     newsProcesser: NewsProcesser,
                     easyParser:EasyParser
                      ): Unit = {

    var isOk = true

    var digest = ""
    var tempDigest = ""

    if (content != "") {

      try {
        tempDigest = easyParser.getSummary(title, content)
      }catch {
        case e:Exception =>
          println("提取摘要异常")
      }

    }

    if (tempDigest == "") {
      isOk = false
    } else {
      digest = DBUtil.getFirstSignData(tempDigest, "\t")
    }

    val summary = DBUtil.interceptData(tempDigest, 300)
    val newDigest = DBUtil.interceptData(digest, 500)
    // 情感
    var senti = 1
    if (content != "") {
      val sentiment = KunLP.getSentiment(title, content)

      if (sentiment == "neg")
        senti = 0

    } else {
      senti = -1
    }
    // 行业
    val industry = newsProcesser.getIndustry(content)
    // 概念
    val section = newsProcesser.getSection(content)
    // 股票
    val stock = newsProcesser.getStock(content)

    val newsType = 2

    var digestFlag: Boolean = true


    val t1 = System.currentTimeMillis()
    digestFlag = DBUtil.insertCall(cstmtDigest, url, newDigest, summary, stock)
    val t2 = System.currentTimeMillis()

    //插入news_info 数据
    if (digestFlag) {

      val n_id = System.currentTimeMillis() * 100 + SparkEnv.get.executorId.toInt
      val newsFlag = insert(newsMysqlStatement, n_id, newsType, platform, title, url, time, industry, section, stock, newDigest, summary, senti, System.currentTimeMillis(), platformStr)
      val t3 = System.currentTimeMillis()

      if (!newsFlag) {
        BigVLogger.warn(s"$platformStr begins to insert digest to mysql and data to news_info error")
      }else{

        val message = KunLP.segment(title, isUseStopWords = false)
          .map(_.word).mkString(",")

        lazyConn.sendTask("sentiment_title",url+"\t"+message)
      }

    } else {
      BigVLogger.warn(s"$platformStr begins to insert digest to mysql and data to article_info error:$digest,$summary,$stock")
    }


  }


  /**
   * 获取摘要
   *
   * @author sijiansheng
   * @param content 正文内容
   * @return 返回的300字摘要
   */
  def interceptData(content: String, number: Int): String = {

    var summary: String = ""

    if (content.length > number && number >= 0) {
      summary = content.substring(0, number)
    } else if (content.length < number) {
      summary = content
    } else {
      println("索引错误，结果不存在")
    }

    summary
  }


  /**
   * 获取完成字符(获取最后一个标识符前的字符串)
   *
   * @param data 原数据
   * @param sign 标志数据
   * @return
   */
  def getLastSignData(data: String, sign: String): String = {

    if (data.contains(sign))
      data.substring(0, data.lastIndexOf(sign))
    else data

  }


  /**
   * 获取完成字符(获取第一个标识符前的字符串)
   *
   * @param data 原数据
   * @param sign 标志数据
   * @return
   */
  def getFirstSignData(data: String, sign: String): String = {

    if (data.contains(sign))
      data.substring(0, data.indexOf(sign))
    else data

  }


  def getDigest(url: String, content: String, extractSummaryConfiguration: (String, Int)): String = {

    try {

      HanLP.getSummary(content, 50)
    } catch {

      case e: Exception =>
        BigVLogger.error(s"获取digest错误，url是$url")
        BigVLogger.exception(e)
        ""

    }

  }

  def getNewStock(lastStock: String, stockDict: scala.collection.Map[scala.Predef.String, scala.Array[scala.Predef.String]]): String = {

    lastStock.split(",").map(cate => {
      cate + "=" + stockDict(cate).filterNot(_ == cate)(0)
    }).mkString("&")

  }
}
