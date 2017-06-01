import java.io.File

import com.kunyan.bigv.config.Platform
import com.kunyan.bigv.db.LazyConnections
import com.kunyan.bigv.util.{StringUtil, DBUtil}
import org.jsoup.Jsoup

import scala.collection.mutable.ListBuffer
import scala.xml.XML

/**
 * Created by lcm on 2016/11/29.
 * 大V历史文章解析
 */
object MoreBigVhistoryParserTest extends App {

  //val file = new File("")
  //println(file.getAbsolutePath)
  //println(file.getCanonicalPath)
  val doc = Jsoup.parse(new File("E:\\github\\bigv\\src\\main\\resources\\article.txt"), "UTF-8")
  val elements = doc.select("div[class=\"article-daily article-daily-first\"] p:not([class])")
  println(elements.size())
//  val read = StringUtil.getMatch(doc.select("p.summary-footer i:contains(浏览)").text(),"(\\d+)")
//  println(read)
//  val buy = doc.select("p.summary-footer i.red").text().toInt
//  println(buy)
//  val price= doc.select("span[class=\"red moerb-icon\"] strong").text().toDouble
//  println(price)
//  val uid = doc.select("a.follow").attr("uid")
//  println(uid)
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
  //  val lazyConn = LazyConnections(XML.loadFile("E:\\github\\bigv\\src\\main\\resources\\config_sql.xml"))
  //  val mysqlConnection = lazyConn.mysqlConn
  //  val checkUserSql = mysqlConnection.prepareStatement("select * from snowball_userInfo where userId = ?")
  //  val selectSql = mysqlConnection.prepareStatement("select userId from snowball_userInfo")
  //  //checkUserSql.setString(1,"1001865669")
  //  val rs = selectSql.executeQuery()
  //  val list = new ListBuffer[String]
  //  while(rs.next()){
  //    val id = rs.getString("userId")
  //    if(!list.contains()){
  //      list.+=(id)
  //    }else{
  //      println(id)
  //    }
  //  }
  //  if(!rs.next()){
  //
  //    println("用户不存在")
  //  }else{
  //
  //    println("用户已存在")
  //  }
  //DBUtil.insertHbase("bigv_article", "row1", "content", "123", "60003", "1", "1", "题目", lazyConn)

  //  println(Platform.SNOW_BALL.id)
}
