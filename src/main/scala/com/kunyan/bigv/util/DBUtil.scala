package com.kunyan.bigv.util

import java.sql.PreparedStatement
import java.text.SimpleDateFormat
import java.util.Locale

import com.ibm.icu.text.CharsetDetector
import com.kunyan.bigv.db.LazyConnections
import com.kunyan.bigv.logger.BigVLogger
import org.apache.hadoop.hbase.client.{Put, Get}
import org.apache.hadoop.hbase.util.Bytes

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

  def getLongTimeStamp(time:String):Long={
    val loc = new Locale("en")
    val sdf = new SimpleDateFormat("yyyy年MM月dd日 HH:mm:ss",loc)
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

    println("enter hbase")
    try {

      val url = table.get(get).getValue(Bytes.toBytes("basic"), Bytes.toBytes("url"))
      val content = table.get(get).getValue(Bytes.toBytes("basic"), Bytes.toBytes("content"))

      if(url == null){
        println("url == null")
      }
      if(content == null ){
        println("content == null")
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
   * @param rowKey 行名
   * @param platform 平台号
   * @param content 内容
   * @param title 标题
   * @param time 时间
   * @param lazyConn 连接
   */
  def insertHbase(tableName: String, rowKey: String, platform:String,content:String,title:String,time:String, lazyConn: LazyConnections) = {

    val table = lazyConn.getTable(tableName)
    val put = new Put(rowKey.getBytes)

    put.addColumn("basic".getBytes, "platform".getBytes, platform.getBytes)
    put.addColumn("basic".getBytes, "content".getBytes, content.getBytes)
    put.addColumn("basic".getBytes, "title".getBytes, title.getBytes)
    put.addColumn("basic".getBytes, "time".getBytes, time.getBytes)

    table.put(put)

  }

  //插入数据库操作
  def insert(prep: PreparedStatement, params: Any*): Unit = {

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
            println("Unknown Type")
        }

      }

      prep.executeUpdate

    } catch {
      case e: Exception =>
        e.printStackTrace()
    }

  }
}
