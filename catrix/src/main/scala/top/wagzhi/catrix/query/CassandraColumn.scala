package top.wagzhi.catrix.query

import java.util.Date

import com.datastax.driver.core.{DataType, Row}
import org.slf4j.LoggerFactory

import scala.language.{existentials, higherKinds, implicitConversions}
import scala.reflect.ClassTag
import scala.collection.JavaConversions._
import scala.reflect.runtime.{universe => ru}

/**
  * Created by paul on 2017/7/31.
  */
case class CassandraColumn[T](
                           columnName:String,
                           columnType:DataType,
                           fieldName:String = ""
                          )(implicit val typeTag:ru.TypeTag[T]){
  private val logger = LoggerFactory.getLogger(getClass)

  def == (value:T):QueryFilter[T]= QueryFilter(this,"=",value)
  def > (value:T):QueryFilter[T]= QueryFilter(this,">",value)
  def >= (value:T):QueryFilter[T]= QueryFilter(this,">=",value)
  def < (value:T):QueryFilter[T]= QueryFilter(this,"<",value)
  def <= (value:T):QueryFilter[T]= QueryFilter(this,"<=",value)
  def contains (value:Any):QueryFilter[T] = QueryFilter(this,"contains",value)
  def in (values:Seq[T]):QueryFilter[T] = {
    QueryFilter(this,"in",values:_*)
  }
  def apply(row:Row):T = {
    val m = ru.runtimeMirror(this.getClass.getClassLoader)

    if(this.columnType.getName.equals(DataType.Name.SET)){
      val tpe = typeTag.tpe.typeArgs.head
      val clazz = m.runtimeClass(tpe)
      row.getSet(this.columnName,clazz).toSet[Any].asInstanceOf[T]
    }else{
      val clazz = m.runtimeClass(typeTag.tpe)
      row.get(columnName,clazz).asInstanceOf[T]
    }

  }

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
case class ColumnValue[T](column:CassandraColumn[T],value:Any){
  def getValue[T] = value.asInstanceOf[T]
}

case class Columns(columns:Seq[CassandraColumn[_]]){
  def ~[T] (column:CassandraColumn[T])(implicit columnList:Map[String,CassandraColumn[_]]) ={
    Columns( this.columns :+ column.named)
  }
  def apply[T](model:T)(implicit  typeTag:ru.TypeTag[T],modelClassTag:ClassTag[T]): Seq[ColumnValue[_]] ={
    val m = ru.runtimeMirror(this.getClass.getClassLoader)
    val cs = m.classSymbol(m.runtimeClass(typeTag.tpe))
    val im = m.reflect(model)
    this.columns.map{
      column=>
        cs.toType.decls.filter{
          scope=>
            scope.asTerm.name.toString.equals(column.fieldName)
        }.headOption.map{
          scope=>

            val value = im.reflectField(scope.asTerm).get
            ColumnValue(column,value)
        }.getOrElse(throw new IllegalArgumentException("No field '"+column.fieldName+" found in the object: "+m.toString()))
    }
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