package catrix.query

import catrix.model.{Column, MapColumn}


/**
  * Created by paul on 2017/9/15.
  */
case class EqualsQueryFilter[T](column: Column[T],value:T) extends QueryFilter[T]{
  def queryString ={
    val columnName = column.columnName
    s"$columnName = ?"
  }
  def values = {
    val v = if(value.isInstanceOf[Enumeration#Value]){
      value.asInstanceOf[Enumeration#Value].id
    }else{
      value
    }
    Seq(v)
  }
}
case class InQueryFilter[T](column: Column[T],vs:T*) extends QueryFilter[T]{
  override def queryString: String = {
    val columnName= column.columnName
    val f = vs.toSeq.map(_=>"?").mkString(",")
    s"$columnName in ($f)"
  }
  def values = vs.toSeq.map{
    value=>
      if(value.isInstanceOf[Enumeration#Value]){
        value.asInstanceOf[Enumeration#Value].id
      }else{
        value
      }
  }
}

case class ContainsKeyQueryFilter[K,V](mapColumn: MapColumn[K,V],key:K) extends QueryFilter[Map[K,V]]{
  override def queryString: String = {
    s"${mapColumn.columnName} contains KEY ?"
  }

  override def values: Seq[Any] = Seq(key)
}

case class EntryQueryFilter[K,V](mapColumn: MapColumn[K,V],key:K,value:V) extends QueryFilter[Map[K,V]]{
  override def queryString: String = {
    s"${mapColumn.columnName}[?] = ?"
  }

  override def values: Seq[Any] = Seq(key,value)
}

case class ContainsQueryFilter[T](column: Column[_],v:T) extends QueryFilter[T]{
  override def queryString: String = {
    s"${column.columnName} contains ?"
  }

  override def values: Seq[Any] = Seq(v)
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

