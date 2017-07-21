package top.wagzhi.catrix.example

import java.util.Date

import top.wagzhi.catrix.query.Column
import top.wagzhi.catrix.{BaseModel, Table}
import top.wagzhi.catrix.query.QueryBuilder._

/**
  * Created by paul on 2017/7/20.
  */
case class Book(id:Int, name:String, price:Double, publishedAt:Date) {
}

object BookTable extends Table[Book]{
  override val modelType: Class[Book] = classOf[Book]


  def find = {
    val filter = Column("id") === 1
    BookTable.filter("id" === 1)
  }
}