package top.wagzhi.catrix

import com.datastax.driver.core.{Cluster, Session}

/**
  * Created by paul on 2017/7/20.
  */
object Catrix{
  def connect(contactPoint:String,keyspace:String) =
        Connector(contactPoint ,keyspace)
}

case class Connector(val contactPoint:String, keyspace:String){
  val cluster = Cluster.builder().addContactPoint(contactPoint).build()

  def close = cluster.close()

  def withSession[T](f:Session=>T)(implicit conn: Connector):T ={
    val session = conn.cluster.connect(conn.keyspace)
    try{
      f(session)
    }catch {
      case e:Throwable=> {throw e}
    }finally {
      session.close()
    }
  }
}

