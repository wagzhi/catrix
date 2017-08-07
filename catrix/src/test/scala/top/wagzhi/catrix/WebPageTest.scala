package top.wagzhi.catrix
import java.util.Date

import com.datastax.driver.core.{DataType, Row, SimpleStatement, TypeCodec}
import org.scalatest.{Matchers, Outcome}
import org.slf4j.LoggerFactory
import top.wagzhi.catrix.query._

/**
  * Created by paul on 2017/7/31.
  * CREATE KEYSPACE catrix WITH replication = {'class': 'SimpleStrategy', 'replication_factor': '1'}  AND durable_writes = true;
  * use catrix;
  * create table t_web_page (host text,fetch_day int,fetch_time timestamp, url text,content text,tags set<text>,links map<text,int>, primary key ((host,fetch_day),fetch_time,url)) WITH CLUSTERING ORDER BY (fetch_time desc);
  * create index t_web_page_url_idx on t_web_page (url);
  * create index t_web_page_url_tag on t_web_page (tags);
  * alter table t_web_page add reply_id list<bigint>;
  */
case class TestWebPage(host:String,fetchTime:Date,fetchDay:Int,url:String,content:Option[String],tags:Set[String],links:Map[String,Int],replyId:Seq[Long]) {

}

class TestWebPageTable extends CassandraTable[TestWebPage]("t_web_page"){
  val host = column[String]("host")
  val fetchTime = column[Date]("fetch_time")
  val fetchDay = column[Int]("fetch_day")
  val url = column[String]("url")
  val content = column[Option[String]]("content")
  val tags = column[Set[String]]("tags")
  val links = column[Map[String,Int]]("links")
  val replyId= column[Seq[Long]]("reply_id")

  lazy val columns = host ~ fetchTime ~ fetchDay ~ url ~ content ~ tags ~ links ~ replyId


  def insert(wp:TestWebPage)(implicit conn:Connection)={
    super.insert(this.columns(wp)).execute
  }

  def getByTag(tag:String)(implicit conn:Connection)={
    super.select(*).filter(tags contains "娱乐").execute.map
  }

  def getByUrl(u:String)(implicit conn:Connection) ={
    select(*).filter(url == u).execute.map{
      r=>
        TestWebPage(host(r),fetchTime(r),fetchDay(r),url(r),content(r),tags(r),links(r),replyId(r))
    }
  }

  def getByHostAndDay(h:String,d:Int*)(implicit conn:Connection)={
    select(*).filter(host == h).filter(fetchDay in d.toSeq).execute.map{
      r=>
        TestWebPage(host(r),fetchTime(r),fetchDay(r),url(r),content(r),tags(r),links(r),replyId(r))
    }
  }



  def page(hostName:String,day:Int,pagingState:String="")(implicit conn:Connection) = {
    super.select(*).filter(host == hostName).
      filter(fetchDay == day).page(pagingState).execute.map{
      r=>
        TestWebPage(host(r),fetchTime(r),fetchDay(r),url(r),content(r),tags(r),links(r),replyId(r))
    }
  }

}


class TestWebPageTest extends org.scalatest.fixture.FlatSpec with Matchers {
  val logger = LoggerFactory.getLogger(getClass)

  case class FixtureParam(conn: Connection, table: TestWebPageTable,samples:Seq[TestWebPage])

  override protected def withFixture(test: OneArgTest): Outcome = {
    implicit val conn = Catrix.connect("172.16.102.239", "catrix")
    try {
      val sstmt = new SimpleStatement("truncate table t_web_page")
      conn.session.execute(sstmt)
      val now = new Date()
      val time = now.getTime
      val table = new TestWebPageTable()
      val samples=Seq(
        TestWebPage("www.19lou.com", now,20170727,"http://www.19lou.com/abc.html", None,Set[String](),Map[String,Int](),Seq[Long]()),
        TestWebPage("www.19lou.com",
          new Date(time+1),
          20170727,
          "http://www.19lou.com/abcd.html",
          Some("中文"),
          Set[String]("新闻","娱乐","八卦"),
          Map[String,Int]("http://www.domain2.com/1.html"->1,"http://www.domain3.com/3.html"->3),
          Seq[Long](1000l,2000l)
        )
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
