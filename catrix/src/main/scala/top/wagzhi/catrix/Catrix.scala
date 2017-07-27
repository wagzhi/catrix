package top.wagzhi.catrix


import com.datastax.driver.core.{Cluster, PreparedStatement, Session}

/**
  * Created by paul on 2017/7/20.
  */
object Catrix{
  def connect(contactPoint:String,keyspace:String) =
        Connection(contactPoint ,keyspace)
}

case class Connection(val contactPoint:String, keyspace:String){
  val cluster = Cluster.builder().addContactPoint(contactPoint).build()
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
    val stmt:PreparedStatement = stmts.synchronized(
      stmts.get(query).getOrElse{
        val ps = session.prepare(query)
        stmts.put(query,ps)
        ps
      })
    f(stmt,session) //PreparedStatement is thread safe
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

