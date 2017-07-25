package top.wagzhi.catrix.example

import java.util.Date

import top.wagzhi.catrix.query.Column
import top.wagzhi.catrix.{ Connection, Table}
import top.wagzhi.catrix.query.QueryBuilder._

/**
  * Created by paul on 2017/7/20.
  */
case class Book(id:Int, name:String, price:Double, publishedAt:Date) {
  def save(implicit conn:Connection) = Book.books.save(this)
}

object Book{
  lazy val books = new BookTable()
}

class BookTable extends Table[Book]{
  override val modelType= classOf[Book]
  def one(implicit conn:Connection) = filter("id" === 1).select.one().as[Book]
  def page1(size:Int,state:String)(implicit conn:Connection)  = all.page(size,state).select.asPage[Book]
  def page2(start:Int)(implicit conn:Connection) = filter("id" >== 2).allowFiltering.select.asPage[Book]
  def removeById(id:Int)(implicit conn:Connection) = filter("id" === id).delete
  def getById(id:Int)(implicit conn:Connection) = filter("id" === id).select.one().as[Book]
  def update(book:Book)(implicit conn:Connection) = filter("id" === book.id).update(Map(("name","Scala programing"),("price",17.0)))
}

case class Page(domain:String,url:String,content:String,createdAt:Date)

class PageTable extends Table[Page] {
  override val modelType: Class[Page] = classOf[Page]
  def des(implicit conn:Connection) = filter("domain" === "a.com").order("createdAt","desc").select.asPage[Page]
}

case class Attachment(code:String,url:String,referer:Set[String],data:Array[Byte])
object Attachment{
  val table = new AttachmentTable()
}

class AttachmentTable extends Table[Attachment]{
  override val modelType: Class[Attachment] = classOf[Attachment]
  def getByCode(code:String)(implicit conn:Connection) = filter("code" === code).select.headOption.map(_.as[Attachment])
  def getByReferer (referer:String)(implicit conn:Connection) =
        filter("referer" _contains "a").select.asPage[Attachment]
}