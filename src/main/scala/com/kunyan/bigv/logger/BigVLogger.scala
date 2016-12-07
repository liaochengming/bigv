package com.kunyan.bigv.logger

import org.apache.log4j.{PropertyConfigurator, BasicConfigurator, Logger}

/**
  * Created by niujiaojiao on 2016/11/16.
  */
object BigVLogger {
  val logger = Logger.getLogger("BIGV analyzer")

  BasicConfigurator.configure()
//    PropertyConfigurator.configure("/home/vip/conf/log4j.properties")

  def exception(e: Exception) = {
    logger.error(e.getLocalizedMessage)
    logger.error(e.getStackTraceString)
  }

  def error(msg: String): Unit = {
    println(msg)
//    logger.error(msg)
  }

  def warn(msg: String): Unit = {
    logger.warn(msg)
  }

  def info(msg: String): Unit = {
    logger.info(msg)
  }

  def debug(msg: String): Unit = {
    logger.debug(msg)
  }

}
