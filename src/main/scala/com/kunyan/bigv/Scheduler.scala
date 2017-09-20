package com.kunyan.bigv

import com.kunyan.bigv.db.LazyConnections
import com.kunyan.bigv.logger.BigVLogger
import com.kunyan.bigv.parser.moer.{MoerBigVHistoryParser, MoerBigVUpdateParser, MoerFinance}
import com.kunyan.bigv.parser.xueqiu.{SnowballHistoryParser, SnowballParser, SnowballUpdateParser}
import com.kunyan.bigv.util.DBUtil
import com.kunyan.nlp.task.NewsProcesser
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

    try {

      val sparkConf = new SparkConf()
        .setAppName("BIGV")
        .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
        .set("spark.kryoserializer.buffer.max", "2000")
//      .setMaster("local")

      val ssc = new StreamingContext(sparkConf, Seconds(args(1).toInt))


      val path = args(0)

      val configFile = XML.loadFile(path)
      val lazyConn = LazyConnections(configFile)
      val connectionsBr = ssc.sparkContext.broadcast(lazyConn)


      //val jdbcUrl = "jdbc:mysql://61.147.114.88:3306/news?user=news&password=news&useUnicode=true&characterEncoding=utf8"

      // 初始化行业、概念、股票字典
      val newsProcesser = NewsProcesser(ssc.sparkContext, (configFile \ "mysqlSen" \ "url").text)
      val newsProcesserBr = ssc.sparkContext.broadcast(newsProcesser)

      val groupId = (configFile \ "kafka" \ "groupId").text
      val brokerList = (configFile \ "kafka" \ "brokerList").text

      LogManager.getRootLogger.setLevel(Level.WARN)

      //摩尔的topic
      val moerReceiveTopic_s = (configFile \ "kafka" \ "moerreceive_s").text
      val moerReceiveTopic_h = (configFile \ "kafka" \ "moerreceive_h").text
      val moerReceiveTopic_u = (configFile \ "kafka" \ "moerreceive_u").text
      val moerSendTopic_s = (configFile \ "kafka" \ "moersend_s").text
      val moerSendTopic_h = (configFile \ "kafka" \ "moersend_h").text
      val moerSendTopic_u = (configFile \ "kafka" \ "moersend_u").text

      //雪球的topic
      val xueQiuReceiveTopic_s = (configFile \ "kafka" \ "xueqiureceive_s").text
      val xueQiuReceiveTopic_h = (configFile \ "kafka" \ "xueqiureceive_h").text
      val xueQiuReceiveTopic_u = (configFile \ "kafka" \ "xueqiureceive_u").text
      val xueQiuSendTopic_s = (configFile \ "kafka" \ "xueqiusend_s").text
      val xueQiuSendTopic_h = (configFile \ "kafka" \ "xueqiusend_h").text
      val xueQiuSendTopic_u = (configFile \ "kafka" \ "xueqiusend_u").text


      val topicsSet = Set[String](moerReceiveTopic_s,moerReceiveTopic_h,moerReceiveTopic_u,xueQiuReceiveTopic_s,xueQiuReceiveTopic_h,xueQiuReceiveTopic_u)
      val sendTopic = (moerSendTopic_s, moerSendTopic_h, moerSendTopic_u,xueQiuSendTopic_s, xueQiuSendTopic_h, xueQiuSendTopic_u)

      val kafkaParams = Map[String, String]("metadata.broker.list" -> brokerList,
      "group.id" -> groupId)

      //信息
      val allMessages = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](
            ssc, kafkaParams, topicsSet)

      //信息处理
      allMessages.map(_._2).filter(_.length > 0).foreachRDD(rdd => {
        //
        rdd.foreach(message => {
          //
          println("get kafka Topic message: " + message)
          analyzer(message,
            connectionsBr.value,
            sendTopic,
            newsProcesserBr.value)

        })
      })

      //中金的信息
      //      val zhongJinMessages = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](
      //      ssc, kafkaParams, zhongJinTopicsSet)

      ssc.start()
      ssc.awaitTermination()

    } catch {
      case excepiton: Exception =>
        excepiton.printStackTrace()
    }
  }


  def analyzer(message: String,
               lazyConn: LazyConnections,
               topic: (String,String,String,String, String, String),
               newsProcesser:NewsProcesser
                ): Unit = {

    val json: Option[Any] = JSON.parseFull(message)

    if (json.isDefined) {

      //get content from json
      val map: Map[String, String] = json.get.asInstanceOf[Map[String, String]]

      try {

        val attrId = map.get("attr_id").get.toInt
        val tableName = map.get("key_name").get
        val rowKey = map.get("pos_name").get
        val result = DBUtil.query(tableName, rowKey, lazyConn)

        //即将增加的状态
        val status = map.get("tag").get

        if (null == result) {

          BigVLogger.warn("Get empty data from hbase table! Message :  " + message)
          return
        }

        attrId match {

          //摩尔金融平台
          case id if id == 60003 =>

            status match {

              //解析top100阶段的数据
              case "SELECT" =>
                MoerFinance.parse(result._1, result._2, lazyConn, topic._1)

              //解析100大V的历史文章
              case "HISTORY" =>
                MoerBigVHistoryParser.parse(result._1,
                  result._2,
                  lazyConn,
                  topic._2,
                  newsProcesser)

              //解析更新的文章
              case "UPDATE" =>
                MoerBigVUpdateParser.parse(result._1,
                  result._2,
                  lazyConn,
                  topic._3,
                  newsProcesser)

            }

          //雪球平台
          case id if id == 60005 =>

            status match {

              //解析top100阶段的数据
              case "SELECT" =>
                SnowballParser.parse(result._1, result._2, lazyConn, topic._4)


              //解析100大V的历史文章
              case "HISTORY" =>
                SnowballHistoryParser.parse(result._1,
                  result._2,
                  lazyConn,
                  topic._5,
                  newsProcesser
                )

              //解析更新的文章
              case "UPDATE" =>
                SnowballUpdateParser.parse(result._1,
                  result._2,
                  lazyConn,
                  topic._6,
                  newsProcesser)

            }
        }

      } catch {

        case e: Exception =>
          println(e.printStackTrace())
          BigVLogger.error("json格式不正确=>" + json)

      }
    } else {

      BigVLogger.error("this message from kafka  isn't standard json =>" + message)

    }

  }
}
