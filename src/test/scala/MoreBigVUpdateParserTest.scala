import java.util.regex.Pattern

import com.kunyan.bigv.db.LazyConnections
import com.kunyan.bigv.util.StringUtil
import org.apache.hadoop.hbase.client.Get

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

  val lazyConn = LazyConnections(XML.loadFile("F:\\资料\\牛娇娇\\目前的项目\\config_sql.xml"))
  val table = lazyConn.getTable("bigv_article")
  val g = new Get("http://www.moer.cn/articleDetails.htm?articleId=125192".getBytes)
  val result = table.get(g)
  println(result.isEmpty)

//  val userId = StringUtil.getMatch("http://www.moer.cn/findArticlePageList.htm?theId=%s&returnAuPage=&page=9&collectType=0","page=(\\d+)")
  //  println(userId)
}
