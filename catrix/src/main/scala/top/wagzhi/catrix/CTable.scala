package top.wagzhi.catrix

import top.wagzhi.catrix.query.{CassandraColumn, Columns, RowParser, TupledColumns1}

import scala.reflect.ClassTag
import scala.reflect.runtime.{universe => ru}
/**
  * Created by paul on 2017/9/13.
  */
abstract class CTable[T](override val tableName:String)(implicit override val mTypeTag:ru.TypeTag[T], override val mClassTag:ClassTag[T])
  extends CassandraTable[T](tableName){
  val parser:RowParser[T]
  override lazy val columns = {
    Columns(parser.columns.columns.map(_.named))
  }

  def insert(m:T) ={
    super.insert(parser(m))
  }

  implicit def wap[T1](column:CassandraColumn[T1]) = TupledColumns1(column)

}
