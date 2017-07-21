package top.wagzhi.catrix.example

import java.util.Date

import top.wagzhi.catrix.{Catrix, Table}
import scala.collection.JavaConverters._
/**
  * Created by paul on 2017/7/20.
  */
object CatrixRunner extends App{
  implicit val conn = Catrix.connect("172.16.102.239","catrix")
  val bookList = Seq(
    Book(1,"scala",98.12,new Date()),
    Book(2,"java", 97,new Date()),
    Book(3,"go",2,new Date())
  )

  bookList.map(Book.books.save)

  val book = Book.books.one

  println(book)
  println("page1:")
  val books1 = Book.books.page1(2,"")
  books1.rows.map(println)
  println(books1.pagingState)

  println("page2:")
  val books2 = Book.books.page1(2,books1.pagingState)
  books2.rows.map(println)
  println(books2.pagingState)

  println("page start from2:")
  val books = Book.books.page2(2)
  books.rows.map(println)
  println(books.pagingState)



  println("update: ")
  Book.books.update(books1.rows.head)
  books1.rows(1).copy(name="Java Programing").save

  println("delete:")
  Book.books.removeById(3).iterator().asScala.foreach(println)

  println("select desc:")
  val pages = Seq(
    Page("a.com","a.com/1","abc",new Date(10000)),
    Page("a.com","a.com/2","abc",new Date(10001)),
    Page("a.com","a.com/3","abc",new Date(10002)),
    Page("b.com","b.com/1","abc",new Date(10000)),
    Page("c.com","c.com/1","abc",new Date(10000))
  )

  val pageDao = new PageTable()
  pages.foreach(pageDao.save)
  pageDao.des.rows.foreach(println)


  conn.close
}
