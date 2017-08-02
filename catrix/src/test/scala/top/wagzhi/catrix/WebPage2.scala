package top.wagzhi.catrix
import java.util.Date

import com.datastax.driver.core.{DataType, Row, SimpleStatement, TypeCodec}
import org.scalatest.{Matchers, Outcome}
import org.slf4j.LoggerFactory
import top.wagzhi.catrix.query._

/**
  * Created by paul on 2017/7/31.
  */
case class WebPage2(host:String,fetchDay:Int,fetchTime:Date,url:String,content:String,tags:Set[String]) {

}

class PageTable extends CassandraTable[WebPage2]{
  val host = column[String]("host")
  val fetchTime = column[Date]("fetch_time")
  val fetchDay = column[Int]("fetch_day",DataType.cint())
  val url = column[String]("url",DataType.text())
  val content = column[String]("content",DataType.text())
  val tags = column[Set[String]]("tags")



  val columns = host ~ fetchTime ~ fetchDay ~ url ~ content ~ tags


  override val tableName: String = "web_page"

  def insert(wp:WebPage2)(implicit conn:Connection)={
    super.insert(this.columns(wp)).execute
  }

  def getByTag(tag:String)(implicit conn:Connection)={
    super.select(*).filter(tags contains "娱乐").execute.map{
      r=>
        WebPage2(host(r),fetchDay(r),fetchTime(r),url(r),content(r),tags(r))
    }
  }

  def getByUrl(u:String)(implicit conn:Connection) ={
    select(*).filter(url == u).execute.map{
      r=>
        WebPage2(host(r),fetchDay(r),fetchTime(r),url(r),content(r),tags(r))
    }
  }

  def getByHostAndDay(h:String,d:Int*)(implicit conn:Connection)={
    select(*).filter(host == h).filter(fetchDay in d.toSeq).execute.map{
      r=>
        WebPage2(host(r),fetchDay(r),fetchTime(r),url(r),content(r),tags(r))
    }
  }



  def page(hostName:String,day:Int,pagingState:String="")(implicit conn:Connection) = {
    super.select(*).filter(host == hostName).
      filter(fetchDay == day).page(pagingState).execute.map{
      r=>
         WebPage2(host(r),fetchDay(r),fetchTime(r),url(r),content(r),tags(r))
    }
  }

}


class WebPageTest2 extends org.scalatest.fixture.FlatSpec with Matchers {
  val logger = LoggerFactory.getLogger(classOf[WebPageTest2])

  case class FixtureParam(conn: Connection, table: PageTable,samples:Seq[WebPage2])




  override protected def withFixture(test: OneArgTest): Outcome = {
    implicit val conn = Catrix.connect("172.16.102.239", "catrix")
    try {
      val sstmt = new SimpleStatement("truncate table web_page")
      conn.session.execute(sstmt)
      val now = new Date()
      val time = now.getTime
      val table = new PageTable()
      val samples=Seq(
        WebPage2("www.19lou.com", 20170727,now,"http://www.19lou.com/abc.html", "中国的",Set[String]()),
        WebPage2("www.19lou.com", 20170727,new Date(time+1),"http://www.19lou.com/abcd.html", "中文",Set[String]("新闻","娱乐","八卦"))
      )
      samples.foreach(table.insert)
      withFixture(test.toNoArgTest(FixtureParam(conn, table,samples)))
    } finally {
      conn.close
    }
  }

  "WebPageTest2" should "select" in{
    f=>
      implicit val conn = f.conn
      val table = f.table

      val samples = f.samples
      //filter by contains
      val pages = table.getByTag("娱乐").rows //table.page("www.19lou.com",20170727).rows
      pages should have length 1
      pages.head shouldBe samples(1)

      //filter by ==
      val pages2 = table.getByUrl(samples(0).url).rows
      pages2(0) shouldBe samples(0)

      //filter by in
      val host = samples(0).host
      val day = samples(0).fetchDay
      val urls = samples.map(_.url)
      val pages3 = table.getByHostAndDay(host,day,day+1)
      pages3.rows should have length 2

  }



}
