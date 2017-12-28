package catrix

import catrix.model.Table
import com.datastax.driver.core.{Cluster,SimpleStatement}
import org.slf4j.LoggerFactory

import scala.reflect.runtime.{universe => ru}
/**
  *
  * @author paul <wagzhi@gmail.com>
  * @since 2017/12/27 下午1:43
  */
abstract class Keyspace(val contactPoint:String,
val keyspace:String) {

  val logger = LoggerFactory.getLogger(getClass)
  lazy val cluster = Cluster.builder().addContactPoint(contactPoint).build()

  lazy implicit val conn = {
    Connection(cluster,keyspace)
  }
  lazy val tables = {
    val m = ru.runtimeMirror(getClass.getClassLoader)
    val clazz = m.classSymbol(this.getClass)
    clazz.toType.members.filter{
      module=>
        module.isModule
    }.map{
      module=>
        val im = m.reflect(this)
        val mm = im.reflectModule(module.asModule)
        mm.instance.asInstanceOf[Table[_]]
    }
  }

  def createKeyspace = {
    val sess = cluster.connect()
    val cql = s"create keyspace if not exists $keyspace with replication={ 'class' : 'SimpleStrategy', 'replication_factor' : 1 };"
    sess.execute(new SimpleStatement(cql))
    sess.close()
  }

  def dropTables ={
    tables.foreach{
      table=>
        val cql = table.dropCql
        logger.info(s"execute: $cql")
        conn.session.execute(new SimpleStatement(cql))
    }
  }
  def createTables = {
    tables.foreach{
      table=>
        table.createCqls.foreach{
          cql=>
            logger.info(s"execute: $cql")
            conn.session.execute(new SimpleStatement(cql))
        }

    }
  }
  def close = {
    conn.close
  }
}
