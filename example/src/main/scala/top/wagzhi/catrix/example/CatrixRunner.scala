package top.wagzhi.catrix.example

import java.util.Date

import com.datastax.driver.core.utils.UUIDs
import top.wagzhi.catrix.{Catrix, Table}
import top.wagzhi.catrix.query.QueryBuilder._
/**
  * Created by paul on 2017/7/20.
  */
object CatrixRunner extends App{
  implicit val conn = Catrix.connect("127.0.0.1","catrix")
  val books = Seq(
    Book(1,"scala",98.12,new Date()),
    Book(2,"java", 97,new Date()),
    Book(3,"go",2,new Date())
    )

  books.map(BookTable.save)
  val q = BookTable.filter("id" === 1)
  val rs = q.execute
  println(rs.one().toString)
  conn.close
}
