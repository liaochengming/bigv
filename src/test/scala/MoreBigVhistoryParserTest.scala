import com.kunyan.bigv.db.LazyConnections
import com.kunyan.bigv.util.DBUtil

import scala.xml.XML

/**
 * Created by lcm on 2016/11/29.
 * 大V历史文章解析
 */
object MoreBigVhistoryParserTest extends App {

  //val file = new File("")
  //println(file.getAbsolutePath)
  //println(file.getCanonicalPath)
  //  val doc = Jsoup.parse(new File("F:\\资料\\牛娇娇\\目前的项目\\bigv\\src\\test\\scala\\阿贵主页.txt"),"UTF-8")
  //  val articleNum = doc.select("li[id=\"articleLeft\"] span").text()
  //println(articleNum)
  //  val doc = Jsoup.parse(new File("F:\\资料\\牛娇娇\\top100_history\\作者文章列表.txt"),"UTF-8")
  //  val elements = doc.select("a[title]")
  //  println(doc)
  //  val doc = Jsoup.parse(new File("F:\\资料\\牛娇娇\\top100_history\\卖想高价.txt"), "UTF-8")
  //  val timestamp = DBUtil.getTimeStamp(doc.select("p.time-stamp").text(),"yyyy年MM月dd日 HH:mm:ss")
  //  println(timestamp)
  //  val title = doc.select("h2.article-title").text()
  //  println(title)
  //  val elements = doc.select("div[class=\"article-daily article-daily-first\"] p:not([style],[class])")
  //  for(index <- 0 until elements.size()){
  //    println(elements.get(index).text())
  //  }
  val lazyConn = LazyConnections(XML.loadFile("F:\\资料\\牛娇娇\\目前的项目\\config_sql.xml"))
  DBUtil.insertHbase("bigv_article", "row1", "60003", "内容", "题目","123", lazyConn)

}
