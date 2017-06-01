package com.kunyan.bigv.parser.moer

import org.jsoup.Jsoup

/**
 * Created by lcm on 2017/5/27.
 */
object Test extends App{

  val doc = Jsoup.connect("http://www.moer.cn/articleDetails.htm?articleId=158109").get()
  doc.select("")

}
