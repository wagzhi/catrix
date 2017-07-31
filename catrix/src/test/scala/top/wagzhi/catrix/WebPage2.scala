package top.wagzhi.catrix
import java.util.Date

import com.datastax.driver.core.{DataType, SimpleStatement}
import org.scalatest.{Matchers, Outcome}
import org.slf4j.LoggerFactory
import top.wagzhi.catrix.query.{CassandraColumn, ColumnValue}

/**
  * Created by paul on 2017/7/31.
  */
case class WebPage2(host:String,fetchDay:Int,url:String,content:String) {

}

class PageTable extends CassandraTable[WebPage2]{
  val host:CassandraColumn = column("host",DataType.text())
  val fetchTime = column("fetch_time",DataType.timestamp())
  val fetchDay = column("fetch_day",DataType.cint())
  val url = column("url",DataType.text())
  val content = column("content",DataType.text())



  def hostRep:Unit=>CassandraColumn = {
    _:Unit=>
      column("host",DataType.text())
  }

  def columns: Seq[CassandraColumn] = Seq[CassandraColumn](
    host,fetchDay,fetchTime,url,content
  )
  override val tableName: String = "web_page"

//  def map:Seq[ColumnValue]=>WebPage2 = {
//    values =>
//      values.
//  }
  val reps :Seq[Unit=>CassandraColumn] = Seq(hostRep)



  def insert(hostName:String,day:Int,time:Date,turl:String,tcontent:String)(implicit conn:Connection):Unit = {
    val values = Seq(host(hostName),fetchDay(day),fetchTime(time),url(turl),content(tcontent))
    super.insert(values).execute
  }
  def page(hostName:String,day:Int,pagingState:String="")(implicit conn:Connection) = {
    super.select(*).filter(host === hostName).
      filter(fetchDay === day).page(pagingState).execute
  }

  //override def map[T](values: Map[CassandraColumn, Any]): T = ???
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
      table.insert("www.19lou.com", 20170727, now,"http://www.19lou.com/abc.html", "abc")
      withFixture(test.toNoArgTest(FixtureParam(conn, table)))
    } finally {
      conn.close
    }
  }

  "WebPageTest2" should "select" in{
    f=>
      implicit val conn = f.conn
      val table = f.table
      val row = table.page("www.19lou.com",20170727).one()
      logger.info(row.toString)
      table.columnNames.foreach(logger.info)

  }
}
