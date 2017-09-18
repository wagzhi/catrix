package top.wagzhi.catrix
import java.awt.image.SampleModel
import java.util.Date

import catrix.model.{RowParser2, Table}
import com.datastax.driver.core.SimpleStatement
import org.scalatest.{Matchers, Outcome}

/**
  * Created by paul on 2017/9/13.
  * create table simple (id int, name text, primary key(id));
  */
case class Simple(id:Int,name:String)
case class Simple2(id:Int,name:String,birthDay:Date)
class SimpleTable(implicit conn:Connection) extends Table[Simple]("simple"){
  val id = column[Int]("id")
  val name = column[String]("name")
  override lazy val primaryKey = partitionKeys(id).clusteringKeys(name).orderBy(name Desc)
  val parser1 :RowParser2[Simple,Int,String] =
    (id ~ name  <> (Simple.tupled,Simple.unapply))
  val parser  = parser1

  def add(s:Simple) ={
    super.insert(s).execute
  }
  def getById(id:Int) ={
    select(*).filter(this.id == 1).execute.pageResult
  }
  def all() = {
    select(*).execute.pageResult
  }
}
class SimpleTest extends org.scalatest.fixture.FlatSpec with Matchers {
  var tableCreated = false
  case class FixtureParam(conn: Connection, table: SimpleTable,samples:Seq[Simple])

  override protected def withFixture(test: OneArgTest): Outcome = {
    implicit val conn = Catrix.connect("172.16.102.238", "catrix")
    try {
      val table = new SimpleTable
      if(tableCreated){
        conn.session.execute(new SimpleStatement(table.truncateCql))
      }else{
        conn.session.execute(new SimpleStatement(table.dropCql))
        val createcql = table.createCql
        println(createcql)
        conn.session.execute(new SimpleStatement(createcql))
        tableCreated = true
      }

      val simples = Seq(Simple(1,"paul wang"),Simple(2,"猪猪侠"))
      withFixture(test.toNoArgTest(FixtureParam(conn, table,simples)))
    }finally {
      //conn.close
    }
  }
  "Simple test2" should "insert and select" in{
    f=>
      f.samples.map{
        s=>
          f.table.add(s)
      }
      val results = f.table.all().results
      results should have length 2
      results(0) shouldBe f.samples(0)
      results(1) shouldBe f.samples(1)
  }
  it should  "select by id" in{
    f=>
      f.samples.map{
        s=>
          f.table.add(s)
      }
      val results = f.table.getById(1).results
      results should have length 1
      results(0) shouldBe f.samples(0)
  }
}
