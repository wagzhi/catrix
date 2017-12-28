package catrix

import catrix.model._

/**
  *
  * @author paul <wagzhi@gmail.com>
  * @since 2017/12/27 下午1:58
  */
class TestDatabase extends Keyspace(contactPoint = "127.0.0.1",keyspace = "catrix") {

  object model2 extends Model2Table()
  object model3 extends Model3Table()
  object model4 extends Model4Table()
  object model5 extends Model5Table()
  object model6 extends Model6Table()
  object model7 extends Model7Table()
  object model8 extends Model8Table()


}
object DatabaseTest extends App{
  val db = new TestDatabase()
  db.createKeyspace
  db.dropTables
  db.createTables
  db.close
}