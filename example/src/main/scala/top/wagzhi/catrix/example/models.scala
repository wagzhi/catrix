package top.wagzhi.catrix.example

import java.util.Date

import top.wagzhi.catrix.query.Column
import top.wagzhi.catrix.{ Connector, Table}
import top.wagzhi.catrix.query.QueryBuilder._

/**
  * Created by paul on 2017/7/20.
  */
case class Book(id:Int, name:String, price:Double, publishedAt:Date) {
  def save(implicit conn:Connector) = Book.books.save(this)
}

object Book{
  lazy val books = new BookTable()
}

class BookTable extends Table[Book]{
  override val modelType= classOf[Book]
  def one(implicit conn:Connector) = filter("id" === 1).select.one().as[Book]
  def page1(size:Int,state:String)(implicit conn:Connector)  = all.page(size,state).select.pageResult[Book]
  def page2(start:Int)(implicit conn:Connector) = filter("id" >== 2).allowFiltering.select.pageResult[Book]
  def removeById(id:Int)(implicit conn:Connector) = filter("id" === id).delete
  def getById(id:Int)(implicit conn:Connector) = filter("id" === id).select.one().as[Book]
  def update(book:Book)(implicit conn:Connector) = filter("id" === book.id).update(Map(("name","Scala programing"),("price",17.0)))
}

case class Page(domain:String,url:String,content:String,createdAt:Date)

class PageTable extends Table[Page] {
  override val modelType: Class[Page] = classOf[Page]
  def des(implicit conn:Connector) = filter("domain" === "a.com").order("createdAt","desc").select.pageResult[Page]
}