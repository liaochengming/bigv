package com.kunyan.bigv

import com.kunyan.bigv.db.LazyConnections
import com.kunyan.bigv.parser.SnowballParser
import com.kunyan.bigv.parser.moer.{MoerBigVHistoryParser, MoerBigVUpdateParser, MoerFinance}
import com.kunyan.bigv.util.DBUtil
import kafka.serializer.StringDecoder
import org.apache.log4j.{Level, LogManager}
import org.apache.spark.SparkConf
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}

import scala.util.parsing.json.JSON
import scala.xml.XML

/**
 * Created by niujiaojiao on 2016/11/16.
 *
 */
object Scheduler {

  def main(args: Array[String]): Unit = {

//    Logger.getLogger("org").setLevel(Level.ERROR)

    val sparkConf = new SparkConf()
      .setAppName("NewTask")
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .set("spark.kryoserializer.buffer.max", "2000")
      //.setMaster("local")

    val ssc = new StreamingContext(sparkConf, Seconds(1))


    val path = args(0)

    val configFile = XML.loadFile(path)
    val lazyConn = LazyConnections(configFile)
    val connectionsBr = ssc.sparkContext.broadcast(lazyConn)


    val groupId = (configFile \ "kafka" \ "groupId").text
    val brokerList = (configFile \ "kafka" \ "brokerList").text

    LogManager.getRootLogger.setLevel(Level.WARN)

    //摩尔的topic
    val moerReceiveTopic = (configFile \ "kafka" \ "moerreceive").text
    val moerSendTopic = (configFile \ "kafka" \ "moersend").text
    val moerTopicsSet = Set[String](moerReceiveTopic)

    //雪球的topic
    val xueQiuReceiveTopic = (configFile \ "kafka" \ "xueqiureceive").text
    val xueQiuSendTopic = (configFile \ "kafka" \ "xueqiusend").text
    val xueQiuTopicsSet = Set[String](xueQiuReceiveTopic)


    val kafkaParams = Map[String, String]("metadata.broker.list" -> brokerList,
      "group.id" -> groupId)

    //摩尔的信息
    val moerMessages = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](
      ssc, kafkaParams, moerTopicsSet)

    //摩尔的信息处理
    moerMessages.map(_._2).filter(_.length > 0).foreachRDD(rdd => {
      //
      rdd.foreach(message => {
        //
        println("get kafka moerTopic message: " + message)
        analyzer(message, connectionsBr.value, moerSendTopic)

      })
    })

    //雪球的信息
    val xueQiuMessages = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](
      ssc, kafkaParams, xueQiuTopicsSet)

//    雪球的信息处理
    xueQiuMessages.map(_._2).filter(_.length > 0).foreachRDD(rdd => {
      //
      rdd.foreach(message => {
        //
        println("get kafka xueqiuTopic message: " + message)
        analyzer(message, connectionsBr.value, xueQiuSendTopic)

      })
    })


    ssc.start()
    ssc.awaitTermination()
  }


  def analyzer(message: String, lazyConn: LazyConnections, topic: String): Unit = {

    val json: Option[Any] = JSON.parseFull(message)

    if (json.isDefined) {

      //get content from json
      val map: Map[String, String] = json.get.asInstanceOf[Map[String, String]]

      try {

        val attrId = map.get("attr_id").get.toInt
        val tableName = map.get("key_name").get
        val rowkey = map.get("pos_name").get
        val result = DBUtil.query(tableName, rowkey, lazyConn)

        //即将增加的状态
        val status = map.get("tag").get

        if (null == result) {
          //        if (result == null || result._1.isEmpty || result._2.isEmpty) {

          println("Get empty data from hbase table! Message :  " + message)
          return
        }

        attrId match {

          //摩尔金融和雪球平台
          case id if id == 60003 =>
            println(result._1 + "\n")

            status match {

              //解析top100阶段的数据
              case "SELECT" =>
                println("摩尔，状态==>SELECT")
                MoerFinance.parse(result._1, result._2, lazyConn, topic)

              //解析100大V的历史文章
              case "HISTORY" =>
                println("状态==>HISTORY")
                MoerBigVHistoryParser.parse(result._1, result._2, lazyConn, topic)

              //解析更新的文章
              case "UPDATE" =>
                println("状态==>UPDATE")
                MoerBigVUpdateParser.parse(result._1, result._2, lazyConn, topic)

            }

          //摩尔金融和雪球平台
          case id if id == 60005 =>
            println(result._1 + "\n")

            status match {

              //解析top100阶段的数据
              case "SELECT" =>
                println("雪球，状态==>SELECT")
                SnowballParser.parse(result._1, result._2, lazyConn, topic)

//              解析100大V的历史文章
              case "HISTORY" =>
                println("状态==>HISTORY")
                MoerBigVHistoryParser.parse(result._1, result._2, lazyConn, topic)
//
//              //解析更新的文章
//              case "UPDATE" =>
//                println("状态==>UPDATE")
//                MoerBigVUpdateParser.parse(result._1, result._2, lazyConn, topic)
              case _ =>


            }

          case _ =>
            println(attrId.toString)

        }

      } catch {
        case e: Exception =>
          println(e.printStackTrace())
          println("json格式不正确" + json)
      }

    } else {

      println("this source isn't standard json" + message)

    }

  }
}
