package top.wagzhi.catrix

import java.util.Date

import catrix.model.Table
import com.datastax.driver.core.SimpleStatement
import org.scalatest.{Matchers, Outcome}
import top.wagzhi.catrix.model._

/**
  * Created by paul on 2017/9/18.
  */
abstract class ModelTest[M] extends org.scalatest.fixture.FlatSpec with Matchers {
  var tableCreated = false
  case class FixtureParam(conn: Connection, table: Table[M],samples:Seq[M])
  val samples:Seq[M]
  def table(implicit conn:Connection):Table[M]

  override protected def withFixture(test: OneArgTest): Outcome = {
    implicit val conn = Catrix.connect("127.0.0.1", "catrix")
    try {
      val table = this.table
      if(tableCreated){
        conn.session.execute(new SimpleStatement(table.truncateCql))
      }else{
        conn.session.execute(new SimpleStatement(table.dropCql))
        val createcql = table.createCql
        println(createcql)
        conn.session.execute(new SimpleStatement(createcql))
        table.createIndexCqls.foreach{
          s=>
            println(s)
            conn.session.execute(new SimpleStatement(s))
        }
        tableCreated = true
      }

      withFixture(test.toNoArgTest(FixtureParam(conn, table,this.samples)))
    }finally {
      conn.close
    }
  }

}
class Model2Test extends ModelTest[Model2]{
  override val samples: Seq[Model2] = Seq(
    Model2(1,"paul"),
    Model2(2,"张三"),
    Model2(3,"李四")
  )
  override def table(implicit conn:Connection) = new Model2Table()
  "model2" should "getall" in{
    f=>
      val table = f.table.asInstanceOf[Model2Table]
      samples.map(table.add)
      val models = table.all().results
      models shouldBe samples
  }
  it should "get by name" in{
    f=>
      val table = f.table.asInstanceOf[Model2Table]
      samples.map(table.add)
      val model = table.getByName("张三")
      model shouldBe Some(samples(1))
  }
  it should "get by in ids" in{
    f=>
      val table = f.table.asInstanceOf[Model2Table]
      samples.map(table.add)
      val models = table.getByIds(1,2).results
      models shouldBe Seq(samples(0),samples(1))
  }
}

class Model3Test extends ModelTest[Model3]{
  override val samples: Seq[Model3] = Seq(
    Model3(1,"张三",Seq[String]("男人","NB")),
    Model3(2,"李四",Seq[String]("男人","SB")),
    Model3(3,"王五",Seq[String]())
  )
  override def table(implicit conn:Connection) = new Model3Table()
  "model2" should "getall" in{
    f=>
      val table = f.table.asInstanceOf[Model3Table]
      samples.map(table.add)
      val models = table.all().results
      models shouldBe samples
  }
  it should  "select by contains" in{
    f=>
      val table = f.table.asInstanceOf[Model3Table]
      samples.map(table.add)
      val models = table.getByTags("男人").results
      models shouldBe Seq(samples(0),samples(1))
  }
}

class Model4Test extends ModelTest[Model4] {
  import Gender._
  val now = System.currentTimeMillis()
  override val samples: Seq[Model4] = Seq(
    Model4(1, "张三", Man,new Date(now)),
    Model4(2, "李四", Man,new Date(now+100)),
    Model4(3, "王五", Woman,new Date(now+200))
  )

  override def table(implicit conn: Connection) = new Model4Table()

  "model4" should "get by enumeration" in {
    f =>
      val table = f.table.asInstanceOf[Model4Table]
      samples.map(table.add)
      val models = table.getByGender(Man).results
      models shouldBe samples.filter(_.gender ==  Man)
  }
}

class Model5Test extends ModelTest[Model5] {
  val now = System.currentTimeMillis()
  override val samples: Seq[Model5] = Seq(
    Model5(1, "张三",Set("男人","牛人"),true,new Date(now)),
    Model5(1, "李四",Set("男人","牛人"),true,new Date(now-100)),
    Model5(1, "王五",Set("男人","牛人"),true,new Date(now-200)),
    Model5(1, "六六",Set("女人"),true,new Date(now-300))
  )

  override def table(implicit conn: Connection) = new Model5Table()

  "model5" should "get all" in {
    f =>
      val table = f.table.asInstanceOf[Model5Table]
      samples.map(table.add)
      val models = table.all().results
      models shouldBe samples
  }
  it should "get by boolean" in{
    f=>
      val table = f.table.asInstanceOf[Model5Table]
      samples.map(table.add)
      val models = table.getUndeleted.results
      models shouldBe samples.filter(!_.deleted)
  }
}

