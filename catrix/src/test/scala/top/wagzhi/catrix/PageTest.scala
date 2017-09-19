package top.wagzhi.catrix

import java.util.Date

import com.datastax.driver.core.SimpleStatement
import org.scalatest.{Matchers, Outcome}
import org.slf4j.LoggerFactory

/**
  * Created by paul on 2017/7/27.
  * CREATE KEYSPACE catrix WITH replication = {'class': 'SimpleStrategy', 'replication_factor': '1'}  AND durable_writes = true;
  * use catrix;
  * create table web_page (host text,fetch_day int,fetch_time timestamp, url text,content text,tags set<text>, primary key ((host,fetch_day),fetch_time,url)) WITH CLUSTERING ORDER BY (fetch_time desc);
  * create index web_page_url_idx on web_page (url);
  * create index web_page_url_tag on web_page (tags);
  *
  *
  * if test failed, please check and make sure all table and index is created successfully!
  */
case class WebPage(host:String,fetchDay:Int,fetchTime:Date,url:String,content:String,tags:Set[String]){
  def save(implicit conn:Connection) = WebPage.table.save(this)
}
object WebPage{
  val table = new WebPageTable()
}
class WebPageTable extends OlderTable[WebPage]{
  import top.wagzhi.catrix.query.QueryBuilder._
  override val modelType: Class[WebPage] = classOf[WebPage]
  def getPage(size:Int=20,state:String="")(implicit conn:Connection) = all.
    page(size=size,state=state).select.asPage[WebPage]

  def getByTag(tag:String)(implicit conn:Connection) = filter("tags" _contains "a").select.asPage[WebPage]

  def getByUrl(url:String)(implicit conn:Connection) = filter("url" === url).select.headOption.map(_.as[WebPage])

  def getByDays(host:String,days:Int*)(implicit conn:Connection) = filter("host"===host).filter("fetchDay" in days.toSeq).select.asPage[WebPage]
}
//class WebPageTest extends org.scalatest.fixture.FlatSpec with Matchers {
//  val logger = LoggerFactory.getLogger(classOf[WebPageTest])
//  case class FixtureParam(conn:Connection,pages:Seq[WebPage])
//  override protected def withFixture(test: OneArgTest): Outcome = {
//    implicit val conn = Catrix.connect("172.16.102.238","catrix")
//    try{
//      val sstmt=new  SimpleStatement("truncate table web_page")
//      conn.session.execute(sstmt)
//      val now = new Date().getTime
//      val webPages = Seq(
//        WebPage("www.19lou.com",20170725,new Date(now),"http://www.19lou.com/1.html","<html></html>",Set("a","b","c","d")),
//        WebPage("www.19lou.com",20170725,new Date(now+1),"http://www.19lou.com/2.html","<html></html>",Set("a")),
//        WebPage("www.19lou.com",20170725,new Date(now+2),"http://www.19lou.com/3.html","<html></html>",Set("a")),
//        WebPage("www.19lou.com",20170725,new Date(now+3),"http://www.19lou.com/4.html","<html></html>",Set("a","b","c","d")),
//        WebPage("www.19lou.com",20170725,new Date(now+4),"http://www.19lou.com/5.html","<html></html>",Set("a","b","c","d")),
//        WebPage("www.19lou.com",20170725,new Date(now+5),"http://www.19lou.com/6.html","<html></html>",Set("a","b","c","d")),
//        WebPage("www.19lou.com",20170723,new Date(now+6),"http://www.19lou.com/7.html","<html></html>",Set("a","b","c","d")),
//        WebPage("www.19lou.com",20170723,new Date(now+7),"http://www.19lou.com/8.html","<html></html>",Set("a","b","c","d")),
//        WebPage("www.19lou.com",20170724,new Date(now+8),"http://www.19lou.com/9.html","<html></html>",Set("a")),
//        WebPage("www.19lou.com",20170724,new Date(now+9),"http://www.19lou.com/10.html","<html></html>",Set[String]())
//      )
//      webPages.foreach(_.save)
//
//      withFixture(test.toNoArgTest(FixtureParam(conn,webPages)))
//    }finally {
//      conn.close
//    }
//  }
//  "WebPage table " should "query page" in{
//    f=>
//      implicit val conn = f.conn
//
//      val p1 = WebPage.table.getPage(5)
//      p1.rows should have length 5
//      var pageNumber = 10
//      p1.rows.foreach{row=>
//        val url = s"http://www.19lou.com/$pageNumber.html"
//        pageNumber = pageNumber-1
//        row.url shouldBe url
//      }
//      p1.pagingState should not be empty
//
//      val p2 = WebPage.table.getPage(5,p1.pagingState)
//      p2.rows.map(_.url).foreach(logger.info)
//      p2.rows should have length 5
//      pageNumber =5
//      p2.rows.foreach{
//        row=>
//          val url = s"http://www.19lou.com/$pageNumber.html"
//          pageNumber = pageNumber -1
//          row.url shouldBe url
//      }
//
//      logger.info("p2 paging state:"+p2.pagingState)
//      p2.pagingState should not be empty //cassandra分页的一个问题：如果最后一页刚好满的，返回的paggingstate不为空
//      val p3 = WebPage.table.getPage(5,p2.pagingState)
//      p3.rows shouldBe empty
//      p3.pagingState shouldBe empty //现在才是空的
//
//  }
//  "WebPage table " should "query page use contains" in{
//    f=>
//      implicit val conn = f.conn
//      val page = WebPage.table.getByTag("a")
//      page.rows should have length 9
//      page.rows.map(_.url) should not (contain ("http://www.19lou.com/10.html"))
//  }
//
//  "WebPage table " should "query page use ===" in{
//    f=>
//      implicit val conn = f.conn
//      val page = WebPage.table.getByUrl("http://www.19lou.com/10.html")
//      page should not be empty
//      page.get.url shouldBe "http://www.19lou.com/10.html"
//  }
//
//  "WebPage table " should "query page use in" in {
//    f=>
//     implicit val conn = f.conn
//      val page = WebPage.table.getByDays("www.19lou.com",20170723,20170724)
//      page.rows should have length 4
//      page.rows.map(_.fetchDay) should not (contain (20170725))
//  }
//

//}
