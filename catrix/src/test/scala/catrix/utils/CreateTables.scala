package catrix.utils

import catrix.Catrix
import catrix.model._
import com.datastax.driver.core.{Cluster, SimpleStatement, SocketOptions}

/**
  * Created by paul on 2017/9/21.
  */
object CreateTables{
  def createTables()={
    val cluster = Cluster.builder().addContactPoint("172.16.102.238")
      .withSocketOptions(new SocketOptions().setConnectTimeoutMillis(5000).setReadTimeoutMillis(20000))
      .build()
    implicit val conn = Catrix.connect(cluster, "catrix")
    try {
      val tables = Seq[Table[_]](
        new Model2Table(),
        new Model3Table(),
        new Model4Table(),
        new Model5Table(),
        new Model6Table(),
        new Model7Table()
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
