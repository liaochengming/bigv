import java.io.File
import java.util.regex.Pattern

import org.json.JSONObject
import org.jsoup.Jsoup

import scala.io.Source

/**
 * Created by lcm on 2016/12/6.
 * 雪球历史的测试类
 */
object SnowballHistoryParserTest extends App{
//  val articleUrl = "https://xueqiu.com/2724224241"
//  val articleListUrl = "https://xueqiu.com/v4/statuses/user_timeline.json?"
//  val pattern = Pattern.compile("https://xueqiu.com/(\\d+)/")
//  val result = pattern.matcher(articleUrl)
//  if(result.find()){
//    println(result.group(1))
//  }
//  val doc = Jsoup.parse(new File("E:\\intellij-workspace\\bigv\\src\\test\\scala\\xueqiuMainPage.txt"),"UTF-8")
//  val data = doc.body().toString
//  println(data)
//  val result = StringUtil.getMatch(data,"\"maxPage\":(\\d)")
//  println(result)
//  var line = ""
//  for(l<-Source.fromFile("E:\\intellij-workspace\\bigv\\src\\main\\resources\\xueqiuarticlelist.txt")("UTF-8").getLines()){
//    println(l)
//    line = l
//  }
//  val jsonStr = line
//  val json = new JSONObject(jsonStr)
//  val jsonArr = json.getJSONArray("statuses")
//  for(index <- 0 until jsonArr.length()){
//    val articleInfo = jsonArr.getJSONObject(index)
//    val target = articleInfo.getString("target")
//    println(target)
//  }

  val doc = Jsoup.parse(new File("E:\\intellij-workspace\\bigv\\src\\main\\resources\\xueqiuArticle"),"UTF-8")
  val time = doc.select("div[class=\"subtitle\"] a").attr("data-created_at")
  val title = doc.select("")
}
