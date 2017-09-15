package catrix.query

import catrix.model.{Column, ListColumn}


/**
  * Created by paul on 2017/9/15.
  */
case class EqualsQueryFilter[T](column: Column[T],value:T) extends QueryFilter[T]{
  def queryString ={
    val columnName = column.columnName
    s"$columnName = ?"
  }
  def values = Seq(value)
}
case class InQueryFilter[T](column: Column[T],vs:T*) extends QueryFilter[T]{
  override def queryString: String = {
    val columnName= column.columnName
    val f = vs.toSeq.map(_=>"?").mkString(",")
    s"$columnName in ($f)"
  }
  def values = vs.toSeq

}

trait QueryFilter[T]{
  def queryString:String
  def values:Seq[Any]
}
case class Pagination(pagingState:String="",pageSize:Int=20)

case class Order(column: Column[_],orderType:OrderType.OrderType){
  def toCql = s"${column.columnName}  ${orderType.toString.toLowerCase}"
}

object OrderType extends Enumeration {
  type OrderType = Value
  val Asc, Desc = Value
}

