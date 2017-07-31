package top.wagzhi.catrix.query

import java.util.Date

import com.datastax.driver.core.DataType

/**
  * Created by paul on 2017/7/31.
  */
case class CassandraColumn(
                           columnName:String,
                           columnType:DataType
                          ){
  def === (value:Any):QueryFilter= QueryFilter(this,"=",value)
  def > (value:Any):QueryFilter= QueryFilter(this,">",value)
  def >== (value:Any):QueryFilter= QueryFilter(this,">=",value)
  def < (value:Any):QueryFilter= QueryFilter(this,"<",value)
  def <== (value:Any):QueryFilter= QueryFilter(this,"<=",value)
  def _contains (value:Any):QueryFilter = QueryFilter(this,"contains",value)
  def in (values:Seq[Any]):QueryFilter = {
    QueryFilter(this,"in",values:_*)
  }
  def apply(value:Any)=ColumnValue(this,value)

  def v = {
    vs:Seq[ColumnValue]=>
      vs.filter(_.column.columnName == this.columnName).headOption.map(_.value)
  }
}
case class ColumnValue(column: CassandraColumn,value:Any)
case class QueryFilter(column: CassandraColumn,word:String,value:Any*){
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

case class Order(column: CassandraColumn,orderType:OrderType.OderType)
object OrderType extends Enumeration{
  type OderType = Value
  val asc ,desc = Value
}