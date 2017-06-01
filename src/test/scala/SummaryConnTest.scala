import java.util.concurrent.Executors

import com.kunyandata.nlpsuit.summary.SummaryExtractor
import org.apache.spark.{SparkContext, SparkConf}

/**
 * Created by lcm on 2016/12/28.
 *
 */
object SummaryConnTest extends App{

  val sc = new SparkContext(new SparkConf().setMaster("local").setAppName("test"))
  val content = "新浪娱乐讯 12月27日消息，据台湾媒体报道，武打巨星成龙[微博]近日在综艺节目《新闻当事人》中，表达自己很不满，指出某演员在片场“耍大牌”，拍两场戏只是喘喘气，就嫌累急忙收工。成龙在节目中没有指名，便引起众多网友猜测，其中黄子韬[微博]、张艺兴[微博]等当红演员全都在名单之中。\n\n成龙近期受访时表示，某演员一到片场，就把自己当成是大牌，在八千人的簇拥下，完装到现场，武打戏的部分别人都帮忙拍完了，只是来拍几个喘喘气的镜头，就直喊“我好累啊”连忙收工，让成龙气得只想送上5个字，“看你几时完！”\n\n这段访问播出以后，许多人都相当好奇成龙口中的大牌演员究竟是谁，在网络上展开热烈的讨论，当红演员“黄子韬”、“张艺兴”等全都在名单之中。\n\n微博闹得沸沸扬扬之时，粉丝们也立即跳出来替偶像澄清，“成龙夸讲过黄子韬，而且他们一起上快本关系很好的”、“采访里面成龙大哥有说是以前红，现在不红的”、“绵羊（张艺兴）很有礼貌的”"

  val pool = Executors.newFixedThreadPool(12)
  for(index <- 0 to 1000){

    val r = new Runnable {
      override def run(): Unit ={

        val t1 = System.currentTimeMillis()
        try {
          val sum = SummaryExtractor.extractSummary(content, "61.147.114.88", 16002)
          println(index + " ====> "+sum)
        }catch {
          case e:Exception=>
            val t2 = System.currentTimeMillis()
            val t3 = t2 - t1
            println("time out  == >"  + t3)
        }

      }
    }

    pool.execute(r)
  }

  pool.shutdown()
}
