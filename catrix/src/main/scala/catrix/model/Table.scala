package catrix.model

import catrix.query.{Order, Query, QueryAction}
import com.datastax.driver.core.{DataType, ResultSet, Row}
import top.wagzhi.catrix.{Connection, MappedResultSet}

import scala.reflect.ClassTag
import scala.reflect.runtime.{universe => ru}
/**
  * Created by paul on 2017/9/13.
  */
abstract class Table[T](tableName:String)(implicit val conn:Connection, val mTypeTag:ru.TypeTag[T], val mClassTag:ClassTag[T]) {
  val parser:RowParser[T]
  import catrix.query.OrderType._
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
        c.columnName + " " +c.columnType.getName.toString
    }.mkString(",")
    s"create table $tableName ( $colums , ${primaryKey.primaryKeyCql})${primaryKey.orderByCql}"
  }
  def dropCql = {
    s"drop table if exists $tableName"
  }

  def truncateCql = {
    s"truncate table $tableName"
  }

  def * = parser.columns.productIterator.map(_.asInstanceOf[Column[_]]).toSeq

  def column[T](columnName:String)(implicit typeTag:ru.TypeTag[T],classTag:ClassTag[T]): Column[T] =
        Column[T](columnName)

  def insert(t:T) = {
    val columnValues = parser(t).productIterator.map(_.asInstanceOf[ColumnValue[_]]).toSeq
    val columns = columnValues.map(_.column)
    val values = columnValues.map(_.value)
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
