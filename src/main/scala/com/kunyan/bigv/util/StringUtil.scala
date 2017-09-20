package com.kunyan.bigv.util

import java.util.Date
import java.util.regex.Pattern

import com.kunyan.bigv.logger.BigVLogger

/**
  * Created by niujiaojiao on 2016/11/16.
 * 处理固定格式的字符串
  */
object StringUtil {

  def getUrlJsonString(attrId: Int, url: String, result: Int): String = {

    val json = "{\"id\":\"\", \"attrid\":%d, \"cookie\":\"\", \"referer\":\"\", \"url\":\"%s\", \"timestamp\":\"%s\", \"result\":\"%d\"}"

    val message = json.format(attrId, url, DBUtil.getDateString, result)

    BigVLogger.warn(message)


    message
  }

  def toJson(attrId: String, islogin: Int, url: String): String = {

    val json = "{\"id\":%s, \"islogin\":%d,\"attrid\":%s, \"depth\":%d, \"cur_depth\":%d, \"method\":%s, \"url\":\"%s\"}"
    json.format(new Date().getTime.toString, islogin, attrId, 2, 2, "2", url)

  }

  /**
   * 匹配需要的字符串
   * @param str 原字符串
   * @param reg 正则规则
   * @return 需求的字符串
   */
  def getMatch(str: String, reg: String): String = {

    val pattern = Pattern.compile(reg)
    val matchUserId = pattern.matcher(str)

    if (matchUserId.find()) matchUserId.group(1) else ""

  }
}
