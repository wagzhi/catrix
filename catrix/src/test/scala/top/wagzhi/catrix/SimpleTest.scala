package top.wagzhi.catrix
import java.awt.image.SampleModel
import java.util.Date

import com.datastax.driver.core.SimpleStatement
import org.scalatest.{Matchers, Outcome}
import top.wagzhi.catrix.query.{RowParser, TupledColumns1}

/**
  * Created by paul on 2017/9/13.
  */
case class Simple(id:Int,name:String)
case class Simple2(id:Int,name:String,birthDay:Date)
class SimpleTable extends CTable[Simple]("simple"){
  val id = column[Int]("id")
  val name = column[String]("name")
  override val parser  =
    TupledColumns1(id) ~ name  <> (Simple.tupled,Simple.unapply)

  def insert(s:Simple)(implicit conn:Connection) ={
    super.insert(s).execute
  }
  def all()(implicit conn:Connection)=
    select(*).execute.mapResult(parser.parse)
}
class SimpleTest extends org.scalatest.fixture.FlatSpec with Matchers {
  case class FixtureParam(conn: Connection, table: SimpleTable,samples:Seq[Simple])

  override protected def withFixture(test: OneArgTest): Outcome = {
    implicit val conn = Catrix.connect("127.0.0.1", "catrix")
    try {
      val sstmt = new SimpleStatement("truncate table t_web_page")
      conn.session.execute(sstmt)
      val table = new SimpleTable
      val simples = Seq(Simple(1,"paul wang"),Simple(2,"pony"))
      withFixture(test.toNoArgTest(FixtureParam(conn, table,simples)))
    }finally {
      conn.close
    }
  }
  "Simple test" should "insert" in{
    f=>
      implicit val conn = f.conn
      f.samples.map{
        s=>
          f.table.insert(s)
      }

      val ss = f.table.all()
      ss.rows.foreach(println)
  }
}
