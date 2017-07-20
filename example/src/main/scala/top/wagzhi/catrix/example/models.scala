package top.wagzhi.catrix.example

import java.util.Date

import top.wagzhi.catrix.query.Column
import top.wagzhi.catrix.{BaseModel, Model}
import top.wagzhi.catrix.query.QueryBuilder._

/**
  * Created by paul on 2017/7/20.
  */
case class Book(id:Int, name:String, price:Double ,publishedAt:Date) extends BaseModel{
  override def unapply(): Seq[Any] = this.productIterator.toSeq
}

object Book extends Model[Book]{
  override val modelType: Class[Book] = classOf[Book]


  def find = {
    val filter = Column("id") === 1
    Book.filter("id" === 1)
  }
}