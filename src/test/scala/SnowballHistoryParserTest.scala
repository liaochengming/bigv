import java.io.File
import java.util.regex.Pattern

import com.kunyan.bigv.config.Platform
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
//  for(l<-Source.fromFile("E:\\github\\bigv\\src\\main\\resources\\xueqiuarticlelist.txt")("UTF-8").getLines()){
//    println(l)
//    line = l
//  }
//  val jsonStr = line
//  val json = new JSONObject(jsonStr)
//  val jsonArr = json.getJSONArray("statuses")
//  val xueqiuHost = "https://xueqiu.com"
//  for(index <- 0 until jsonArr.length()){
//
//    val articleInfo = jsonArr.getJSONObject(index)
//
//    //文章的用户id
//    val userId = articleInfo.getString("user_id")
//    println("userId = >" + userId)
//
//    //文章的url
//    val target = articleInfo.getString("target")
//    val articleUrl = xueqiuHost + target
//    println("articleUrl = >" + articleUrl)
//
//    //文章发表时间
//    val created_at = articleInfo.getLong("created_at")
//    println("created_at = >" + created_at)
//
//    //转载量
//    val retweet_count = articleInfo.getString("retweet_count")
//    println("retweet_count = >" + retweet_count)
//
//    //回复量
//    val reply_count = articleInfo.getString("reply_count")
//    println("reply_count = >" + reply_count)
//
//    //收藏量
//    val fav_count = articleInfo.getString("fav_count")
//    println("fav_count = >" + fav_count)
//
//    //阅读量
//    val view_count = articleInfo.getString("view_count")
//    println("view_count = >" + view_count)
//    println("========================================")
//  }

  val doc = Jsoup.parse(new File("E:\\github\\bigv\\src\\main\\resources\\xueqiuArticle3.txt"),"UTF-8")
  val isRaw = doc.select("div.status-retweet").isEmpty
  println(isRaw)
  val time = doc.select("div[class=\"subtitle\"] a").attr("data-created_at")
  val screeName = doc.select("a[class=\"avatar\"]").attr("data-screenname")
  //println(screeName)
  val uid = doc.select("div.status-item").attr("data-uid")
  println(uid)
  var title = ""
  var content = ""

  if(isRaw){

    //原创
    title = doc.select("div.status-content h1").text()
    content = doc.select("div.status-content div.detail").text()

  }else{

    //转载
    title = screeName + "：的观点"
    content = doc.select("div.status-content").get(0).text().replace("查看对话","")
    //title = doc.select("script.single-description").get(0).data().split(":")(1)
    //content = doc.select("div.status-content:has(h1)").text()

  }
  println(title)
  println(content)
//  val ret = doc.select("a.btn-repost em.em_number").text()
//  println(ret)
//  val repl = doc.select("a[class=\"btn-status-reply last\"] em.em_number").text()
//  println(repl)
//  println(Platform.OLD_SNOW_BALL.toString)
//  println("time =>" + time)
//  if(isRaw){
//    val title = doc.select("div.status-content h1").text()
//    println("title =>" + title)
//
//    val content = doc.select("div.status-content div.detail").text()
//    println("content =>" + content)
//  }else{
//
//    val title = doc.select("script.single-description").get(0).data().split(":")(1)
//    println("title =>" + title)
//
//    val content = doc.select("div.status-content:has(h1)").text()
//    println("content =>" + content)
//  }

}
