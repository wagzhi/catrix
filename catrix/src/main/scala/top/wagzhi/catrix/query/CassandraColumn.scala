package top.wagzhi.catrix.query

import java.util.Date

import com.datastax.driver.core.{DataType, Row}

import scala.language.{existentials, higherKinds, implicitConversions}
import scala.reflect.ClassTag
import scala.reflect.runtime.{universe => ru}

/**
  * Created by paul on 2017/7/31.
  */
case class CassandraColumn[T](
                           columnName:String,
                           columnType:DataType,
                           fieldName:String = ""
                          )(implicit val classTag:ClassTag[T]){
  def === (value:T):QueryFilter[T]= QueryFilter(this,"=",value)
  def > (value:T):QueryFilter[T]= QueryFilter(this,">",value)
  def >== (value:T):QueryFilter[T]= QueryFilter(this,">=",value)
  def < (value:T):QueryFilter[T]= QueryFilter(this,"<",value)
  def <== (value:T):QueryFilter[T]= QueryFilter(this,"<=",value)
  def _contains (value:T):QueryFilter[T] = QueryFilter(this,"contains",value)
  def in (values:Seq[T]):QueryFilter[T] = {
    QueryFilter(this,"in",values:_*)
  }
  def apply(row:Row):T = row.get(columnName,classTag.runtimeClass).asInstanceOf[T]

  def ~[T] (column: CassandraColumn[T])(implicit columnList:Map[String,CassandraColumn[_]]):Columns = {
    Columns(Seq(this.named,column.named))
  }
  def named(implicit columnList:Map[String,CassandraColumn[_]]):CassandraColumn[T]={
    columnList.keySet.filter{
      fname=>
        columnList.get(fname).get.columnName==this.columnName
    }.headOption.map{
      name=>
        this.copy[T](fieldName=name)
    }.getOrElse(throw new IllegalArgumentException("not fieldName found!"))
  }

}

case class Columns(columns:Seq[CassandraColumn[_]]){
  def ~[T] (column:CassandraColumn[T])(implicit columnList:Map[String,CassandraColumn[_]]) ={
    Columns( this.columns :+ column.named)
  }

}
case class QueryFilter[T](column: CassandraColumn[T],word:String,value:Any*){
  def queryString ={
    val columnName = column.columnName

    val valueString = if(word.equals("in")){
      value.asInstanceOf[Seq[Any]].map(_=>"?").mkString("(",",",")")
    }else{
      "?"
    }

    s"$columnName $word $valueString"
  }
}
case class Pagination(pagingState:String="",pageSize:Int=20)

case class Order(column: CassandraColumn[_],orderType:OrderType.OderType)
object OrderType extends Enumeration{
  type OderType = Value
  val asc ,desc = Value
}