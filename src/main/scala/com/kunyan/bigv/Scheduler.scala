package com.kunyan.bigv

import com.kunyan.bigv.db.LazyConnections
import com.kunyan.bigv.logger.BigVLogger
import com.kunyan.bigv.parser.moer.{MoerBigVHistoryParser, MoerBigVUpdateParser, MoerFinance}
import com.kunyan.bigv.parser.xueqiu.{SnowballHistoryParser, SnowballParser, SnowballUpdateParser}
import com.kunyan.bigv.parser.zhongjin.{ZhonJinBlogParser, ZhongJinBlogHistoryParse, ZhongJinBlogUpdateParse}
import com.kunyan.bigv.util.DBUtil
import com.kunyandata.nlpsuit.classification.Bayes
import com.kunyandata.nlpsuit.sentiment.PredictWithNb
import com.kunyandata.nlpsuit.util.KunyanConf
import kafka.serializer.StringDecoder
import org.apache.log4j.{Level, LogManager}
import org.apache.spark.SparkConf
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}

import scala.io.Source
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
      //.setMaster("local")

      val ssc = new StreamingContext(sparkConf, Seconds(args(1).toInt))


      val path = args(0)

      val configFile = XML.loadFile(path)
      val lazyConn = LazyConnections(configFile)
      val connectionsBr = ssc.sparkContext.broadcast(lazyConn)

      val stopWordsPath = (configFile \ "segment" \ "stopWords").text
      val modelsPath = (configFile \ "segment" \ "classModelAddress").text
      val sentiPath = (configFile \ "segment" \ "sentimentModelAddress").text
      val keyWordDictPath = (configFile \ "segment" \ "keyWords").text

      val kyConf = new KunyanConf()

      kyConf.set((configFile \ "segment" \ "ip").text, (configFile \ "segment" \ "port").text.toInt)

      //val stopWords = null//Bayes.getStopWords(stopWordsPath)
      val stopWords = Source.fromFile(stopWordsPath).getLines().toArray
      val classModels = Bayes.initModels(modelsPath)
      val sentiModels = PredictWithNb.init(sentiPath)
      val keyWordDict = Bayes.initGrepDicts(keyWordDictPath)

      //val stopWordsBr = ssc.sparkContext.broadcast(stopWords)
      //val classModelsBr = ssc.sparkContext.broadcast(classModels)
      val sentiModelsBr = ssc.sparkContext.broadcast(sentiModels)
      //val keyWordDictBr = ssc.sparkContext.broadcast(keyWordDict)

      val summaryExtraction = ((configFile \ "summaryConfiguration" \ "ip").text, (configFile \ "summaryConfiguration" \ "port").text.toInt)



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
            stopWords,
            classModels,
            sentiModelsBr.value,
            keyWordDict,
            kyConf,
            summaryExtraction)

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
               stopWords: Array[String],
               classModels: scala.Predef.Map[scala.Predef.String, scala.Predef.Map[scala.Predef.String, scala.Predef.Map[scala.Predef.String, java.io.Serializable]]],
               sentimentModels: scala.Predef.Map[scala.Predef.String, scala.Any],
               keyWordDict: scala.Predef.Map[scala.Predef.String, scala.Predef.Map[scala.Predef.String, scala.Array[scala.Predef.String]]],
               kyConf: KunyanConf,
               summaryExtraction: (String, Int)
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
                println("摩尔，状态==>SELECT" + "\n")
                BigVLogger.warn("摩尔，状态==>SELECT" + "\n")
                MoerFinance.parse(result._1, result._2, lazyConn, topic._1)
              //                lazyConn.sendTask(topic, StringUtil.getUrlJsonString(Platform.MOER.id, "https://www.moer.com/", 0))

              //解析100大V的历史文章
              case "HISTORY" =>
                println("摩尔，状态==>HISTORY" + "\n")
                BigVLogger.warn("摩尔，状态==>HISTORY" + "\n")
                MoerBigVHistoryParser.parse(result._1,
                  result._2,
                  lazyConn,
                  topic._2,
                  stopWords,
                  classModels,
                  sentimentModels,
                  keyWordDict,
                  kyConf,
                  summaryExtraction)

              //解析更新的文章
              case "UPDATE" =>
                println("摩尔，状态==>UPDATE" + "\n")
                BigVLogger.warn("摩尔，状态==>UPDATE" + "\n")
                MoerBigVUpdateParser.parse(result._1,
                  result._2,
                  lazyConn,
                  topic._3,
                  stopWords,
                  classModels,
                  sentimentModels,
                  keyWordDict,
                  kyConf,
                  summaryExtraction)

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
                  stopWords,
                  classModels,
                  sentimentModels,
                  keyWordDict,
                  kyConf,
                  summaryExtraction
                )

              //解析更新的文章
              case "UPDATE" =>
                println("雪球，状态==>UPDATE" + "\n")
                SnowballUpdateParser.parse(result._1,
                  result._2,
                  lazyConn,
                  topic._6,
                  stopWords,
                  classModels,
                  sentimentModels,
                  keyWordDict,
                  kyConf,
                  summaryExtraction)

            }
          //中金博客
          case id if id == 60007 =>
            println("中金，状态==>UPDATE" + "\n")
            BigVLogger.warn("中金，状态==>UPDATE" + "\n")
            status match {

              //解析top100阶段的数据
              case "SELECT" =>
                println("中金，状态==>SELECT")

                ZhonJinBlogParser.parse(result._1, result._2, lazyConn, topic._1)

              //解析100大V的历史文章
              case "HISTORY" =>
                println("中金，状态==>HISTORY")
                ZhongJinBlogHistoryParse.parse(result._1, result._2, lazyConn, topic._2,
                  kyConf,
                  stopWords,
                  classModels,
                  sentimentModels,
                  keyWordDict,
                  summaryExtraction
                )

              //解析更新的文章
              case "UPDATE" =>
                println("中金，状态==>UPDATE")
                ZhongJinBlogUpdateParse.parse(result._1, result._2, lazyConn, topic._3,
                  kyConf,
                  stopWords,
                  classModels,
                  sentimentModels,
                  keyWordDict,
                  summaryExtraction)

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
