import com.kunyan.bigv.db.LazyConnections
import com.kunyan.bigv.util.DBUtil
import org.jsoup.Jsoup

import scala.xml.XML

/**
  * Created by niujiaojiao on 2016/11/18.
  */
object testHbase extends App{

  val lazyConn = LazyConnections(XML.loadFile(args(0)))
  var url = "http://www.moer.cn/articleDetails.htm?articleId=69597"
  val result = DBUtil.query("60003_detail_test", url, lazyConn)

  var html = result._2
//  println(html)
  try{
//    val mysqlConnection = lazyConn.mysqlConn
//    mysqlConnection.setAutoCommit(true)
//    val articlesql = mysqlConnection.prepareStatement("insert into  article_info(articleId,authorId,buy,view,time,likeInfo, priceInfo,profit) VALUES (?,?,?,?,?,?,?,?)")
//    val authorsql  = mysqlConnection.prepareStatement("insert into  author_info(authorId,authorName,authorType,watchCount,fansCount,articleCount) VALUES (?,?,?,?,?,?)")
    val doc = Jsoup.parse(html, "UTF-8")
    println(doc)
    var articleId = url.split("articleId=")(1)
    var authorId = ""
    var flag = true
    if(doc.toString.contains("author-info")){
      authorId = doc.select("div.author-info h3  a[href]").attr("href").split("theId=")(1)
    }else{
      flag = false
    }

    if(flag){
      println("enter ")
      var buyCnt = doc.select("div.left-content i.red").text()
      if(null == buyCnt){
        buyCnt = "0"
      }
      var tags = doc.getElementsByClass("summary-footer").first.getElementsByTag("span").first.getElementsByTag("i")
      var readCnt = tags.get(0).text.substring(3).replaceAll("次", "")
      val timeStamp = (DBUtil.getTimeStamp(tags.get(1).text.substring(3), "yyyy年MM月dd日 HH:mm:ss") / 1000).toInt.toString
      var likeCnt = doc.getElementsByClass("article-handler").first().getElementsByTag("b").text
      var price = "0"
      val priceTag = doc.getElementsByAttributeValue("class", "float-r goOrder").first
      if (priceTag != null)
        price = priceTag.getElementsByTag("strong").first.text
      var profit = "0"
      if(doc.toString.contains("total_yield")){
        var total = doc.toString.split("total_yield\":")
        if((total.length>=2)  && doc.toString.contains("article_type") ){
          profit = total(1).split("article_type")(0).split(",")(0).replace("\"","")
          println(profit)
        }
      }
      //文章信息
//      DBUtil.insert(articlesql,articleId,authorId, buyCnt,readCnt,timeStamp,likeCnt,price,profit)

      var author = doc.select("div.author-info h3  a[href]").text()
      var authorType = 0
      if (doc.getElementsByClass("userv-red").size > 0 ||doc.getElementsByClass("userv-blue").size > 0)
        authorType = 1
      var numberTags  = doc.select("div.author ul.stats-list li")
      var watchCount = ""
      var fansCount = ""
      var articleCount = ""

      if(numberTags!= null){
        watchCount = numberTags.get(0).select("span").text()
        fansCount = numberTags.get(1).select("span").text()
        articleCount = numberTags.get(2).select("i").text()
      }
      //作者信息
//      DBUtil.insert(authorsql,authorId,author,authorType,watchCount,fansCount,articleCount )
    }

  } catch {
    case e: Exception =>
      e.printStackTrace()
  }
}
