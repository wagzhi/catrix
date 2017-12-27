package catrix

import java.util.{Date, UUID}

import catrix.model.{Table, _}
import com.datastax.driver.core.{Cluster, SimpleStatement, SocketOptions}
import org.scalatest.{Matchers, Outcome}
object ConnectManager{
  private var tableCreated = false

  def  conn:Connection = this.synchronized{
    val cluster = Cluster.builder().addContactPoint("127.0.0.1")
      .withSocketOptions(new SocketOptions().setConnectTimeoutMillis(5000).setReadTimeoutMillis(20000))
      .build()
    implicit val conn = Catrix.connect(cluster, "catrix")
    conn
  }


  def createTable(n:Int) ={
    implicit val conn = ConnectManager.conn
    try {
      val tables = Seq[Table[_]](
        new Model2Table(),
        new Model3Table(),
        new Model4Table(),
        new Model5Table(),
        new Model6Table(),
        new Model7Table(),
        new Model8Table()
      )
      val table = tables(n-2)
          conn.session.execute(new SimpleStatement(table.dropCql))
          table.createCqls.foreach {
            s =>
              println(s)
              conn.session.execute(new SimpleStatement(s))
          }
    }finally {
      conn.close
    }

  }
  def createTables()={
    implicit val conn = ConnectManager.conn
    try {
      val tables = Seq[Table[_]](
        new Model2Table(),
        new Model3Table(),
        new Model4Table(),
        new Model5Table(),
        new Model6Table(),
        new Model7Table(),
        new Model8Table()
      )
      tables.foreach {
        table =>
          conn.session.execute(new SimpleStatement(table.dropCql))

          table.createCqls.foreach {
            s =>
              println(s)
              conn.session.execute(new SimpleStatement(s))
          }
      }
    }finally {
      conn.close
    }

  }
}
/**
  * Created by paul on 2017/9/18.
  */
abstract class ModelTest[M] extends org.scalatest.fixture.FlatSpec with Matchers {
  var tableCreated = false
  case class FixtureParam(conn: Connection, table: Table[M],samples:Seq[M])
  val samples:Seq[M]
  def table(implicit conn:Connection):Table[M]

  override protected def withFixture(test: OneArgTest): Outcome = {
    implicit val conn = ConnectManager.conn
    try {
      val table = this.table
      conn.session.execute(new SimpleStatement(table.truncateCql))

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
  it should "test tuple table" in{
    f=>
      implicit val conn = f.conn
      val table = f.table.asInstanceOf[Model2Table]
      samples.map(table.add)
      val tupleTable = new Model2TupleTable
      val models = tupleTable.fistPage().results
      models shouldBe samples.map(Model2.unapply(_).get)


  }
}

class Model3Test extends ModelTest[Model3]{
  override val samples: Seq[Model3] = Seq(
    Model3(UUID.randomUUID(),"张三",Seq[String]("男人","NB")),
    Model3(UUID.randomUUID(),"李四",Seq[String]("男人","SB")),
    Model3(UUID.randomUUID(),"王五",Seq[String]())
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
  it should "get some columns" in{
    f=>
      val table = f.table.asInstanceOf[Model3Table]
      samples.map(table.add)
      val names = table.names
      names.results shouldBe samples.map{
        m=>
          (m.id,m.name)
      }
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

class Model6Test extends ModelTest[Model6] {
  val now = System.currentTimeMillis()
  override val samples: Seq[Model6] = Seq(
    Model6(1,1, "张三",Map("男人"->1,"牛人"->2),true,new Date(now)),
    Model6(1,2, "李四",Map("男人"->2,"牛人"->1),true,new Date(now-100)),
    Model6(1,3, "王五",Map("男人"->3,"牛人"->0),true,new Date(now-200)),
    Model6(1,4, "六六",Map("女人"->5),true,new Date(now-300))
  )

  override def table(implicit conn: Connection) = new Model6Table()

  "model6" should "get all" in {
    f =>
      val table = f.table.asInstanceOf[Model6Table]
      samples.map(table.add)
      val models = table.all.results
      models shouldBe samples
  }
  it should "get by key" in{
    f=>
      val table = f.table.asInstanceOf[Model6Table]
      samples.map(table.add)
      val models = table.getByKey("男人").results
      models shouldBe samples.filter(_.words.contains("男人"))
  }
  it should "get by value" in{
    f=>
      val table = f.table.asInstanceOf[Model6Table]
      samples.map(table.add)
      val models = table.getByValue(1).results
      models shouldBe samples.filter(_.words.values.toSeq.contains(1))
  }
  it should "get by entry" in{
    f=>
      val table = f.table.asInstanceOf[Model6Table]
      samples.map(table.add)
      val models = table.getByWord("牛人",1).results
      models shouldBe samples.filter{
        m=>
         m.words.contains("牛人") && m.words("牛人") == 1
      }
  }
}
