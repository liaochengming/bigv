package com.kunyan.bigv.db

import java.sql.{Connection, DriverManager, PreparedStatement}
import java.util.Properties

import com.kunyan.bigv.logger.BigVLogger
import kafka.producer.{KeyedMessage, Producer, ProducerConfig}
import org.apache.hadoop.hbase.{HBaseConfiguration, TableName}

import scala.xml.Elem


class LazyConnections(createHbaseConnection: () => org.apache.hadoop.hbase.client.Connection,
                      createProducer: () => Producer[String, String],
                      createMySQLConnection: () => Connection,
                      createMySQLNewsConnection: () => Connection,
                      createMySQLVipConnection: () => Connection,
                      createMySQLPS: () => PreparedStatement) extends Serializable {


  lazy val hbaseConn = createHbaseConnection()

  lazy val mysqlConn = createMySQLConnection()

  lazy val mysqlNewsConn = createMySQLNewsConnection()

  lazy val mysqlVipConn = createMySQLVipConnection()

  lazy val preparedStatement = createMySQLPS()

  lazy val producer = createProducer()


  def sendTask(topic: String, value: String): Unit = {

    println(value)

    val message = new KeyedMessage[String, String](topic, value)

    try {
      producer.send(message)
    } catch {
      case e: Exception =>
        BigVLogger.exception(e)
    }
  }

  def sendTask(topic: String, values: Seq[String]): Unit = {

    val messages = values.map(x => new KeyedMessage[String, String](topic, x))

    try {
      producer.send(messages: _*)
    } catch {
      case e: Exception =>
        BigVLogger.exception(e)
    }
  }

  def getTable(tableName: String) = hbaseConn.getTable(TableName.valueOf(tableName))

}

object LazyConnections {

  def apply(configFile: Elem): LazyConnections = {


    val createHbaseConnection = () => {

      val hbaseConf = HBaseConfiguration.create
      hbaseConf.set("hbase.rootdir", (configFile \ "hbase" \ "rootDir").text)
      hbaseConf.set("hbase.zookeeper.quorum", (configFile \ "hbase" \ "ip").text)
      BigVLogger.warn("create connection")

      val connection = org.apache.hadoop.hbase.client.ConnectionFactory.createConnection(hbaseConf)

      sys.addShutdownHook {
        connection.close()
      }

      BigVLogger.warn("Hbase connection created.")

      connection
    }

    val createProducer = () => {

      val props = new Properties()
      props.put("metadata.broker.list", (configFile \ "kafka" \ "brokerList").text)
      props.put("serializer.class", "kafka.serializer.StringEncoder")
      props.put("producer.type", "async")

      val config = new ProducerConfig(props)
      val producer = new Producer[String, String](config)

      sys.addShutdownHook {
        producer.close()
      }

      producer
    }

    //连接给算法组计算大V的数据库表，存储文章和用户信息
    val createMySQLConnection = () => {

      Class.forName("com.mysql.jdbc.Driver")
      val connection = DriverManager.getConnection((configFile \ "mysqlArticle" \ "url").text, (configFile \ "mysqlArticle" \ "username").text, (configFile \ "mysqlArticle" \ "password").text)

      sys.addShutdownHook {
        connection.close()
      }

      connection
    }

    //连接新闻表
    val createMySQLNewsConnection = () => {

      Class.forName("com.mysql.jdbc.Driver")
      val connection = DriverManager.getConnection((configFile \ "mysqlNews" \ "url").text, (configFile \ "mysqlNews" \ "username").text, (configFile \ "mysqlNews" \ "password").text)

      sys.addShutdownHook {
        connection.close()
      }

      connection
    }

    //连接vip表
    val createMySQLVipConnection = () => {

      Class.forName("com.mysql.jdbc.Driver")
      val connection = DriverManager.getConnection((configFile \ "mysqlVip" \ "url").text, (configFile \ "mysqlVip" \ "username").text, (configFile \ "mysqlVip" \ "password").text)

      sys.addShutdownHook {
        connection.close()
      }

      connection
    }
    val createCNFOLPs = () => {

      Class.forName("com.mysql.jdbc.Driver")
      val connection = DriverManager.getConnection((configFile \ "mysql" \ "url").text, (configFile \ "mysql" \ "username").text, (configFile \ "mysql" \ "password").text)

      sys.addShutdownHook {
        connection.close()
      }

      BigVLogger.warn("MySQL connection created.")

      connection.prepareStatement("INSERT INTO article_cnfol (user_id, title, recommend, time, reproduce, comment, url) VALUES (?,?,?,?,?,?,?)")
    }

    val createMOERPs = () => {

      Class.forName("com.mysql.jdbc.Driver")
      val connection = DriverManager.getConnection((configFile \ "mysql" \ "url").text, (configFile \ "mysql" \ "username").text, (configFile \ "mysql" \ "password").text)

      sys.addShutdownHook {
        connection.close()
      }

      BigVLogger.warn("MySQL connection created.")

      connection.prepareStatement("INSERT INTO article_moer (user_id, title, read_count, buy_count, price, url, ts) VALUES (?,?,?,?,?,?,?)")
    }

    val createTGBPs = () => {

      Class.forName("com.mysql.jdbc.Driver")
      val connection = DriverManager.getConnection((configFile \ "mysql" \ "url").text, (configFile \ "mysql" \ "username").text, (configFile \ "mysql" \ "password").text)

      sys.addShutdownHook {
        connection.close()
      }

      BigVLogger.warn("MySQL connection created.")

      connection.prepareStatement("INSERT INTO article_taoguba (user_id, title, recommend, `read`, comment, url, ts) VALUES (?,?,?,?,?,?,?)")
    }

    val createSnowBallPs = () => {

      Class.forName("com.mysql.jdbc.Driver")
      val connection = DriverManager.getConnection((configFile \ "mysql" \ "url").text, (configFile \ "mysql" \ "username").text, (configFile \ "mysql" \ "password").text)

      sys.addShutdownHook {
        connection.close()
      }

      BigVLogger.warn("MySQL connection created.")

      connection.prepareStatement("INSERT INTO temp_article_snowball (user_id, title, retweet, reply, url, ts) VALUES (?,?,?,?,?,?)")
    }

    val createWeiboPs = () => {

      Class.forName("com.mysql.jdbc.Driver")
      val connection = DriverManager.getConnection((configFile \ "mysql" \ "url").text, (configFile \ "mysql" \ "username").text, (configFile \ "mysql" \ "password").text)

      sys.addShutdownHook {
        connection.close()
      }

      BigVLogger.warn("MySQL connection created.")

      connection.prepareStatement("INSERT INTO article_weibo (user_id, title, retweet, reply, like_count, url, ts) VALUES (?,?,?,?,?,?,?)")
    }

    new LazyConnections(createHbaseConnection, createProducer, createMySQLConnection, createMySQLNewsConnection,createMySQLVipConnection, createMOERPs)

  }

}
