package top.wagzhi.catrix



import com.datastax.driver.core.DataType
import org.slf4j.{Logger, LoggerFactory}
import top.wagzhi.catrix.query._

import scala.reflect.runtime.{universe => ru}
/**
  * Created by paul on 2017/7/31.
  */
trait CassandraTable[T] {
  protected val logger:Logger = LoggerFactory.getLogger(classOf[CassandraTable[_]])
  type ModelType = T
  def columns: Seq[CassandraColumn]

  //def map[T](values:Map[CassandraColumn,Any]):T
  lazy val columnNames: Seq[String] = {
    val m = ru.runtimeMirror(getClass.getClassLoader)
    val tableType = m.classSymbol(this.getClass)
    logger.info(tableType.toType.toString)
    tableType.baseClasses.map{
      clazz=>
        logger.info(clazz.asClass.toString)
        logger.info(clazz.asClass.typeParams.map(_.typeSignature.toString).mkString("."))
    }
    tableType.toType.decls.filter{
      memberScope=>
        val resultType = memberScope.asTerm.typeSignature.resultType
        resultType.equals(ru.typeOf[CassandraColumn])
    }.map(_.name.toString).toSeq
  }
  val tableName: String

  def * = columns

  def select(columns: Seq[CassandraColumn]): CassandraQuery =
    CassandraQuery(tableName, QueryAction.select, columns = columns)

  //protected def insert(values: Seq[Any]):CassandraQuery = insert(columns, values)

  protected def insert(values: Seq[ColumnValue]):CassandraQuery = insert(values.map(_.column), values.map(_.value))

  def insert(columns: Seq[CassandraColumn], values: Seq[Any]):CassandraQuery =
    CassandraQuery(tableName, QueryAction.insert, columns = columns, values = values)

  def delete = CassandraQuery(tableName, QueryAction.delete)

  def update(columns: Seq[CassandraColumn], values: Seq[Any]) =
    CassandraQuery(tableName, QueryAction.update, columns = columns, values = values)



  def column(columnName:String,dataType: DataType) = CassandraColumn(columnName,dataType)

  def c(fieldName: String) = {
    val columName = toTableOrColumnName(fieldName)
    columns.filter(_.columnName.equalsIgnoreCase(columName)).head
  }

  private def toTableOrColumnName(name: String): String = {
    val n1 = name.flatMap {
      c =>
        if (c.isUpper) {
          "_" + c.toLower
        } else {
          c.toString
        }
    }
    if (n1.startsWith("_")) {
      n1.substring(1)
    } else {
      n1
    }


  }
}
