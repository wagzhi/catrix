package catrix

import com.datastax.driver.core._
import org.slf4j.LoggerFactory
import top.wagzhi.catrix.exception.{ExecuteException, PrepareStatementException}

/**
  * Created by paul on 2017/7/20.
  */
object Catrix{
  def connect(contactPoint:String,keyspace:String) ={
    val cluster = Cluster.builder().addContactPoint(contactPoint).build()
    Connection(cluster ,keyspace)
  }

  def connect(cluster:Cluster,keyspace:String) ={
    Connection(cluster,keyspace)
  }
}

case class Connection(val cluster: Cluster, keyspace:String){
  private val logger = LoggerFactory.getLogger(getClass)

  lazy val session = cluster.connect(keyspace)

  def close ={
    session.close()
    cluster.close()
  }
  val stmts = scala.collection.mutable.WeakHashMap[String,PreparedStatement]()

  /**
    * for cache prepared statement
    * @param query
    * @param f
    * @tparam T
    * @return
    */
  def withPreparedStatement[T](query:String)(f:(PreparedStatement,Session)=>T):T={
    logger.info(query)

    try{
      val stmt:PreparedStatement = stmts.synchronized(
        stmts.get(query).getOrElse{
          val ps = session.prepare(query)
          stmts.put(query,ps)
          ps
        })
        f(stmt,session) //PreparedStatement is thread safe
    }catch {
      case e:ExecuteException=>
        throw e
      case t:Throwable=>
        throw PrepareStatementException(s"Prepare statement failed: $query",t)
    }
  }

  def withSession[T](f:Session=>T):T ={
    try{
      f(session)
    }catch {
      case e:Throwable=> {throw e}
    }finally {
    }
  }
}

