import java.io.File
import java.util.regex.Pattern

import com.kunyan.bigv.util.{DBUtil, StringUtil}
import org.jsoup.Jsoup
import org.jsoup.nodes.Element
import org.jsoup.select.Elements

/**
 * Created by lcm on 2016/12/12.
 *
 */
object ZhongJinHistoryParserTest extends App{

  val doc = Jsoup.parse(new File("E:\\github\\bigv\\src\\main\\resources\\zhongjinarticlelist"),"UTF-8")
//  val title = doc.select("h1.Head a").text()
//  println(title)
//  val time = DBUtil.getLongTimeStamp(StringUtil.getMatch(doc.select("span.MBTime").text(),"\\[(.*)\\]"),"yyyy-MM-dd HH:mm:ss").toString
//  println(time)
//  val content = doc.select("p.MsoNormal b").text()
//  println(content)
  val list = doc.select("a.ArtTit")

  for(index <- 0 until list.size()){
    val article = list.get(index)
    println(article.select("i.Top").size())
  }

//  val page = doc.select("i.CoRed").text()
//  val articleList = doc.select("a.ArtTit")
//
//  for(index <- 0 until articleList.size()){
//
//    val article = articleList.get(index)
//    val url = article.attr("href")
//    println(url)
//
//  }
  //println(page.split("/")(1))

//  val pattern = Pattern.compile("http://blog.cnfol.com/.*article/(\\d+)")
//  val result = pattern.matcher("http://blog.cnfol.com/index.php/article/blogarticlelist/tyj?page=1")
//
//  if(result.find()){
//
//    println(true)
//  }

}
