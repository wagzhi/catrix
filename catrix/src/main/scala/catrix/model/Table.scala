package catrix.model

import java.util.Date

import catrix.query.{Order, Query, QueryAction}
import com.datastax.driver.core.{DataType, ResultSet, Row}
import top.wagzhi.catrix.{Connection, MappedResultSet}

import scala.collection.JavaConverters._
import scala.reflect.ClassTag
import scala.reflect.runtime.{universe => ru}
/**
  * Created by paul on 2017/9/13.
  */
abstract class Table[T](tableName:String)(implicit val conn:Connection, val mTypeTag:ru.TypeTag[T], val mClassTag:ClassTag[T]) {
  val parser:RowParser[T]
  case class PrimaryKey(partitionKeys:Seq[Column[_]],clusteringKeys:Seq[Column[_]] = Seq[Column[_]](),orderBy:Seq[Order]=Seq[Order]()){
    def clusteringKeys(columns:Column[_]*) = PrimaryKey(this.partitionKeys,columns.toSeq)
    def orderBy(orders:Order*) = this.copy(orderBy = orders.toSeq)
    def primaryKeyCql = {
      val ps = partitionKeys.map(_.columnName).mkString(", ")
      val cs = clusteringKeys.map(_.columnName).mkString(", ")

      val ss = if(cs.isEmpty){
        ps
      }else{
        s"($ps), $cs"
      }

      s"primary key ($ss)"
    }
    def orderByCql = {
      if(orderBy.isEmpty){
        ""
      }else{
        val orders = orderBy.map(_.toCql).mkString(", ")
        s" WITH CLUSTERING ORDER BY ($orders)"
      }


    }
  }

  lazy val primaryKey:PrimaryKey = {
    val first = parser.columns.productIterator.next().asInstanceOf[Column[_]]
    partitionKeys(first)
  }

  def partitionKeys(columns:Column[_]*): PrimaryKey = PrimaryKey(columns.toSeq)


  def createCql = {
    val colums = parser.columns.productIterator.map(_.asInstanceOf[Column[_]]).map{
      c=>
        val columnType = if(c.columnType.getName.equals(DataType.Name.LIST)){
          val inType = c.columnType.getTypeArguments.get(0).getName.toString
          s"list<$inType>"
        }else if(c.columnType.getName.equals(DataType.Name.SET)) {
          val inType = c.columnType.getTypeArguments.get(0).getName.toString
          s"set<$inType>"
        }else{
          c.columnType.getName.toString
        }

        c.columnName + " " + columnType
    }.mkString(",")
    s"create table $tableName ( $colums , ${primaryKey.primaryKeyCql})${primaryKey.orderByCql}"
  }

  def createIndexCqls = parser.*.map(_.indexCql(tableName)).filter(_.nonEmpty)

  def dropCql = {
    s"drop table if exists $tableName"
  }


  def truncateCql = {
    s"truncate table $tableName"
  }

  def * = parser.columns.productIterator.map(_.asInstanceOf[Column[_]]).toSeq

  def enumColumn[T <: Enumeration#Value](columnName:String)(implicit typeTag:ru.TypeTag[T],classTag:ClassTag[T]): EnumerationColumn[T] =
    EnumerationColumn[T](columnName,DataType.cint())

  def column[T](columnName:String)(implicit typeTag:ru.TypeTag[T],classTag:ClassTag[T]): DefaultColumn[T] =
        DefaultColumn[T](columnName)

  def listColumn[T](columnName:String)(implicit typeTag:ru.TypeTag[T],classTag:ClassTag[T]) : ListColumn[T] =
      ListColumn[T](columnName)

  def setColumn[T](columnName:String)(implicit typeTag:ru.TypeTag[T],classTag:ClassTag[T]) : SetColumn[T] =
    SetColumn[T](columnName)

  def insert(t:T) = {
    val columns = parser.*
    val values = parser(t).productIterator.map{
      v=>
        if(v.isInstanceOf[Seq[_]]){
          v.asInstanceOf[Seq[Object]].asJava
        }else if(v.isInstanceOf[Set[_]]){
          v.asInstanceOf[Set[Object]].asJava
        }else if(v.isInstanceOf[Enumeration#Value]){
          v.asInstanceOf[Enumeration#Value].id
        }
        else{
          v
        }
    }.toSeq
    Query(tableName,QueryAction.insert,columns,values)
  }

  def select(columns:Seq[Column[_]]) ={
    Query(tableName,QueryAction.select,columns,Seq())
  }


  implicit class ResultSetWrap(val rs:ResultSet){
    def mapResult:ModelResultSet[T]={
      val pageRows = this.pageRows
      val rows = pageRows._1.map(row=>parser.parse(row))
      ModelResultSet(rs,rows,pageRows._2)
    }

    def pageResult:PageResult[T] = {
      val pageRows = this.pageRows
      val rows = pageRows._1.map(parser.parse)
      PageResult(rows,pageRows._2)
    }


    def pageRows:(Seq[Row],String)={
      val pagingState = rs.getExecutionInfo.getPagingState
      val ps = if (pagingState!=null) {
        pagingState.toString
      }else{
        ""
      }

      val remain = rs.getAvailableWithoutFetching
      val it = rs.iterator()
      val rows = new Array[Row](remain)
      for(i <- (0 to remain-1)){
        rows(i) = it.next()
      }
      (rows,ps)
    }
  }

}
