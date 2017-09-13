package top.wagzhi.catrix

import com.datastax.driver.core.{DataType, ResultSet, Row}
import org.slf4j.{Logger, LoggerFactory}
import top.wagzhi.catrix.query._
import scala.reflect.ClassTag
import scala.collection.JavaConverters._
import scala.reflect.runtime.{universe => ru}
/**
  * Created by paul on 2017/7/31.
  */
abstract class CassandraTable[T](val tableName:String)(implicit val mTypeTag:ru.TypeTag[T], val mClassTag:ClassTag[T]) {
  protected val logger:Logger = LoggerFactory.getLogger(classOf[CassandraTable[_]])
  private val m = ru.runtimeMirror(getClass.getClassLoader)
  val columns: Columns
  def * = columns
  /**
    * Get all defined columns field in subclass, and get field name as key.
    * The field name should be same with the field name of model class if you need mapping values to model
    */
  implicit lazy val definedColumnList:Map[String,CassandraColumn[_]]={
    val m = ru.runtimeMirror(getClass.getClassLoader)
    val tableType = m.classSymbol(this.getClass)
    tableType.toType.decls.filter{
      memberScope=>
        val resultType = memberScope.asTerm.typeSignature.resultType
        resultType.typeSymbol.isClass && resultType.typeSymbol.asClass.equals(m.classSymbol(classOf[CassandraColumn[_]]))
    }.map{ms=>
      val name = ms.name.toString.trim //some time name has blank char ,don't know why. so need trim here.
      val im = m.reflect(this)
      val columnFiled = im.reflectField(ms.asTerm)
      val column = columnFiled.get.asInstanceOf[CassandraColumn[_]]
      (name,column)
    }.toMap[String,CassandraColumn[_]]
  }

  def select(columns: CassandraColumn[_]*): CassandraQuery =
    CassandraQuery(tableName, QueryAction.select, columns = columns)

  def select(columns:Columns) :CassandraQuery =
    CassandraQuery(tableName, QueryAction.select, columns = columns.columns)


  def insert(columnValues:Seq[ColumnValue[_]]):CassandraQuery = {
    val cvs = columnValues.filter{
      cv=>
        val columnTypeName = cv.column.columnType.getName
        if(columnTypeName.equals(DataType.Name.SET) ||
          columnTypeName.equals(DataType.Name.MAP) ||
          columnTypeName.equals(DataType.Name.LIST)
        ){
          cv.value.asInstanceOf[Traversable[_]].nonEmpty //filter empty value
        }else if(cv.column.typeTag.tpe.baseClasses.contains(CassandraColumn.optionClassSymbol)){
          cv.value.asInstanceOf[Option[_]].nonEmpty
        }
        else{
          true
        }
    }
    val columns = cvs.map(_.column)
    val values = cvs.map{
      cv=>
        val columnTypeName = cv.column.columnType.getName
        if(columnTypeName.equals(DataType.Name.SET)){
          cv.value.asInstanceOf[Set[Object]].asJava
        }else if(columnTypeName.equals(DataType.Name.LIST)){
          cv.value.asInstanceOf[Seq[Object]].asJava
        }else if(columnTypeName.equals(DataType.Name.MAP)){
          cv.value.asInstanceOf[Map[Object,Object]].asJava
        }else if(cv.column.isOptionType){
          cv.value.asInstanceOf[Option[Object]].get
        }else if(cv.column.isEnumerationType){
          cv.value.asInstanceOf[Enumeration#Value].id
        }
        else{
          cv.value
        }
    }
    CassandraQuery(tableName, QueryAction.insert, columns = columns, values = values)
  }

  def insert(columns: Seq[CassandraColumn[_]], values: Seq[Any]):CassandraQuery =
    CassandraQuery(tableName, QueryAction.insert, columns = columns, values = values)

  def delete = CassandraQuery(tableName, QueryAction.delete)

  def update(columns: Seq[CassandraColumn[T]], values: Seq[Any]) =
    CassandraQuery(tableName, QueryAction.update, columns = columns, values = values)

  def column[T](columnName:String)(implicit typeTag:ru.TypeTag[T],classTag:ClassTag[T]): CassandraColumn[T] = CassandraColumn[T](columnName)

  def column[T](columnName:String,dataType: DataType)(implicit typeTag:ru.TypeTag[T],classTag:ClassTag[T]) = CassandraColumn[T](columnName,dataType)

  /**
    * map a row to table default model object
    * @param columns
    * @param row
    * @return
    */
  def mapRowDefault(columns:Columns, row:Row):T={
    val conTerm = mTypeTag.tpe.decl(ru.termNames.CONSTRUCTOR).asMethod
    val cm = m.classSymbol(mClassTag.runtimeClass)
    val ctorm = m.reflectClass(cm).reflectConstructor(conTerm)
    val values = conTerm.paramLists.flatMap{
      pm=>pm
    }.map{
      pf=>
        val paramName = pf.name.toString
        columns.columns.filter{
          c=>
            val fieldName = c.fieldName
            fieldName.equals(paramName)
        }.headOption.map{
          c=>
            c(row)
        }.getOrElse(throw new IllegalArgumentException(s"No column found for field $paramName"))

    }
    ctorm(values:_*).asInstanceOf[T]
  }

  implicit class RowWrap(val row:Row){
    def as:T={
      mapRowDefault(columns,row)
    }
  }

  implicit class ResultSetWrap(val rs:ResultSet){
    def mapResult:MappedResultSet[T]={
      val pageRows = this.pageRows
      val rows = pageRows._1.map(row=>mapRowDefault(columns,row))
      MappedResultSet(rs,rows,pageRows._2)
    }

    /**
      * for map to other model object
      * @param f
      * @tparam M
      * @return
      */
    def mapResult[M](f:Row=>M): MappedResultSet[M] ={
      val pageRows = this.pageRows
      val rows = pageRows._1.map(f)
      MappedResultSet(rs,rows,pageRows._2)
    }

    def headOption:Option[Row] ={
      val it =rs.iterator()
      if(it.hasNext){
        Some(it.next())
      }else{
        None
      }
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
