import java.util.regex.Pattern

import com.ibm.icu.text.CharsetDetector
import com.kunyan.bigv.db.LazyConnections
import com.kunyan.bigv.util.StringUtil
import org.apache.hadoop.hbase.client.Get
import org.apache.hadoop.hbase.util.Bytes
import org.jsoup.Jsoup

import scala.xml.XML

/**
 * Created by lcm on 2016/11/30.
 *
 */
object MoreBigVUpdateParserTest extends App {

  //  import scala.util.control.Breaks._
  //  breakable{
  //
  //    for(i<-0 to 4 ){
  //      println(i)
  //      if(i==5){
  //        break()
  //      }
  //    }
  //    println("xx")
  //  }

  val url = "http://www.moer.cn/findArticlePageList.htm?theId=100170279&returnAuPage=&page=1&collectType=0"
  val host = "http://www.moer.cn/"
  val lazyConn = LazyConnections(XML.loadFile("F:\\资料\\牛娇娇\\目前的项目\\config_sql.xml"))
  val table = lazyConn.getTable("60003_detail_1206")
  val g = new Get(url.getBytes)
  //val result = table.get(g)
  val content = table.get(g).getValue(Bytes.toBytes("basic"), Bytes.toBytes("content"))
  val encoding = new CharsetDetector().setText(content).detect().getName
  val html = new String(content, encoding)
  println(html)
  val doc = Jsoup.parse(html)
  var pageNum: Int = 0

  val elements = doc.select("a[title]")

  import scala.util.control.Breaks._
  breakable {

    for (index <- 0 until elements.size()) {

      val element = elements.get(index)
      val href = element.attr("href")
      val url = host + href

      val table = lazyConn.getTable("bigv_article")
      val g = new Get(url.getBytes)
      val result = table.get(g)

      if (result.isEmpty) {

        //将解析的文章页面的url发送给服务器
        //lazyConn.sendTask(topic, StringUtil.getUrlJsonString(60003, url, 0))
        println("文章不存在")
      } else {

        println("文章存在--》断开")
        break()

      }

    }

    //将下页文章列表的url发送给服务器
    val nextPageNum = pageNum + 1
    val nextPageUrl = url.replace("page=" + pageNum,"page=" + nextPageNum)
    println("nextPageUrl ==> " + nextPageUrl)
    //lazyConn.sendTask(topic, StringUtil.getUrlJsonString(60003, nextPageUrl, 0))

  }

//  val userId = StringUtil.getMatch("http://www.moer.cn/findArticlePageList.htm?theId=%s&returnAuPage=&page=9&collectType=0","page=(\\d+)")
  //  println(userId)
}
