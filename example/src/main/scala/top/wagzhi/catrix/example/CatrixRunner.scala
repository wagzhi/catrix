package top.wagzhi.catrix.example

import java.util.Date

import top.wagzhi.catrix.{Catrix, Model}

import scala.reflect.runtime.{universe => ru}
import top.wagzhi.catrix.query.QueryBuilder._
/**
  * Created by paul on 2017/7/20.
  */
object CatrixRunner extends App{
  implicit val conn = Catrix.connect("172.16.102.239","catrix")
  val book = Book(1,"scala",98.12,new Date())
  Book.save(book)
  val q = Book.filter("id" === 1)
  val rs = q.execute
  println(rs.one().toString)
  conn.close
}
