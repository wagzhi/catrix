package top.wagzhi.catrix
import java.util.Date

import com.datastax.driver.core.{DataType, Row, SimpleStatement, TypeCodec}
import org.scalatest.{Matchers, Outcome}
import org.slf4j.LoggerFactory
import top.wagzhi.catrix.query._

/**
  * Created by paul on 2017/7/31.
  */
case class WebPage2(host:String,fetchDay:Int,fetchTime:Date,url:String,content:String) {

}

class PageTable extends CassandraTable[WebPage2]{
  val host = column[String]("host")
  val fetchTime = column[Date]("fetch_time")
  val fetchDay = column[Int]("fetch_day",DataType.cint())
  val url = column[String]("url",DataType.text())
  val content = column[String]("content",DataType.text())



  val columns = host ~ fetchTime ~ fetchDay ~ url ~ content


  override val tableName: String = "web_page"

  def insert(wp:WebPage2)(implicit conn:Connection)={
    super.insert(parse(wp)).execute
  }


  def page(hostName:String,day:Int,pagingState:String="")(implicit conn:Connection) = {
    super.select(*).filter(host === hostName).
      filter(fetchDay === day).page(pagingState).execute.pageRows._1.map{
      r=>
        val wp =  WebPage2(host(r),fetchDay(r),fetchTime(r),url(r),content(r))
        parse(wp)
        wp
    }
  }

}


class WebPageTest2 extends org.scalatest.fixture.FlatSpec with Matchers {
  val logger = LoggerFactory.getLogger(classOf[WebPageTest2])

  case class FixtureParam(conn: Connection, table: PageTable)




  override protected def withFixture(test: OneArgTest): Outcome = {
    implicit val conn = Catrix.connect("172.16.102.239", "catrix")
    try {
      val sstmt = new SimpleStatement("truncate table web_page")
      conn.session.execute(sstmt)
      val now = new Date()
      val table = new PageTable()
      val wp=WebPage2("www.19lou.com", 20170727,now,"http://www.19lou.com/abc.html", "中国的")
      table.insert(wp)
      withFixture(test.toNoArgTest(FixtureParam(conn, table)))
    } finally {
      conn.close
    }
  }

  "WebPageTest2" should "select" in{
    f=>
      implicit val conn = f.conn
      val table = f.table
      val wp = table.page("www.19lou.com",20170727).head

      println(table.parse(wp))
  }
}
