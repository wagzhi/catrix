package catrix.model

import java.awt.image.DataBuffer
import java.nio.ByteBuffer
import java.util.Date

import catrix.model.Column.getDataType
import catrix.query._
import com.datastax.driver.core.{DataType, Row}
import org.slf4j.LoggerFactory
import top.wagzhi.catrix.query.CassandraColumn

import scala.collection.JavaConversions._
import scala.reflect.runtime.{universe => ru}
import scala.reflect.ClassTag


case class ListColumn[T](override val columnName:String,
                              override val columnType:DataType)
                             (implicit val typeTag:ru.TypeTag[T], val classTag: ClassTag[T])
  extends Column[Seq[T]](columnName = columnName,columnType = columnType){
  override def apply(row: Row): Seq[T] = {
    val m = ru.runtimeMirror(this.getClass.getClassLoader)
    val clazz = m.runtimeClass(typeTag.tpe)
    row.getList(this.columnName,clazz).toIndexedSeq.map(_.asInstanceOf[T])
  }
}

case class SetColumn[T](override val columnName:String,
                         override val columnType:DataType)
                        (implicit val typeTag:ru.TypeTag[T], val classTag: ClassTag[T])
  extends Column[Set[T]](columnName = columnName,columnType = columnType){
  override def apply(row: Row): Set[T] = {
    val m = ru.runtimeMirror(this.getClass.getClassLoader)
    val clazz = m.runtimeClass(typeTag.tpe)
    row.getSet(this.columnName,clazz).toSet[Any].map(_.asInstanceOf[T])
  }
}

case class MapColumn[K,V](override val columnName:String,
                        override val columnType:DataType)
                       (implicit val typeTag:ru.TypeTag[K], val classTag: ClassTag[K],val typeTagV:ru.TypeTag[V])
  extends Column[Map[K,V]](columnName = columnName,columnType = columnType){
  override def apply(row: Row): Map[K,V] = {
    val m = ru.runtimeMirror(this.getClass.getClassLoader)
    val clazz = m.runtimeClass(typeTag.tpe)
    val clazzV = m.runtimeClass(typeTagV.tpe)
    row.getMap(this.columnName,clazz,clazzV).toMap[Any,Any].asInstanceOf[Map[K,V]]
  }
}

case class OptionColumn[T](override val columnName:String,
                            override val columnType:DataType)
                          (implicit val typeTag:ru.TypeTag[T], val classTag: ClassTag[T])
  extends Column[Option[T]](columnName = columnName,columnType = columnType){
  override def apply(row: Row): Option[T] = {
    val m = ru.runtimeMirror(this.getClass.getClassLoader)
    val clazz = m.runtimeClass(typeTag.tpe)
    val rawValue = row.get(columnName,clazz)
    if(rawValue!=null){
      Some(rawValue.asInstanceOf[T])
    }else{
      None
    }
  }
}

case class DefaultColumn[T](override val columnName:String,
                            override val columnType:DataType)
                           (implicit val typeTag:ru.TypeTag[T], val classTag: ClassTag[T])
  extends Column[T](columnName = columnName,columnType = columnType){
  override def apply(row: Row): T = {
    val m = ru.runtimeMirror(this.getClass.getClassLoader)
    val clazz = m.runtimeClass(typeTag.tpe)
    val rawValue = row.get(columnName,clazz)
    rawValue.asInstanceOf[T]
  }

  def in(t:T*)={
    new InQueryFilter[T](this,t:_*)
  }

  def ==(t:T) = {
    EqualsQueryFilter(this,t)
  }
}
/**
  * Created by paul on 2017/9/14.
  */
abstract class Column[T](val columnName:String,val columnType:DataType){
  private val logger = LoggerFactory.getLogger(getClass)

//  lazy val isEnumerationType:Boolean={
//    typeTag.tpe.baseClasses.contains(CassandraColumn.enumerationValueClassSymbol)
//  }
//  lazy val isOptionType:Boolean={
//    typeTag.tpe.baseClasses.contains(CassandraColumn.optionClassSymbol)
//  }

  def apply(row:Row):T





  def Desc = Order(this,OrderType.Desc)

  def Asc = Order(this,OrderType.Asc)

  def ~[T1] (c:Column[T1]) = ColumnTuple1(this) ~ c


//  def apply(row:Row):T = {
//    val m = ru.runtimeMirror(this.getClass.getClassLoader)
//    val columnTypeName = columnType.getName
//    if(columnTypeName.equals(DataType.Name.SET)){
//      val clazz = typeTag.tpe.typeArgs.map(m.runtimeClass).map(CassandraColumn.mapPrimitiveTypes).head
//      row.getSet(this.columnName,clazz).toSet[Any].asInstanceOf[T]
//    }else if(columnTypeName.equals(DataType.Name.MAP)){
//      val classSeq = typeTag.tpe.typeArgs.map(m.runtimeClass).map(CassandraColumn.mapPrimitiveTypes)
//      val (classKey,classValue) = (classSeq(0),classSeq(1))
//      row.getMap(this.columnName,classKey,classValue).toMap[Any,Any].asInstanceOf[T]
//    }else if(columnTypeName.equals(DataType.Name.LIST)){
//      val clazz = typeTag.tpe.typeArgs.map(m.runtimeClass).map(CassandraColumn.mapPrimitiveTypes).head
//      row.getList(this.columnName,clazz).toIndexedSeq.asInstanceOf[T]
//    }
//    else{
//      //get column real data class for cassandra
//      val dataClass = if(columnType.getName.equals(DataType.Name.BLOB)){
//        classOf[ByteBuffer]
//      }else if(this.isEnumerationType){
//        m.runtimeClass(ru.typeOf[Int])
//      }else if(this.isOptionType){
//        typeTag.tpe.typeArgs.map(m.runtimeClass).map(CassandraColumn.mapPrimitiveTypes).head
//      }else{
//        m.runtimeClass(typeTag.tpe)
//      }
//
//      //get raw value
//      val rawValue = row.get(columnName,dataClass)
//
//      //map raw value to corresponding type of T
//      if(rawValue == null){
//        if(isOptionType){
//          None.asInstanceOf[T]
//        }else{
//          this.defaultValue.getOrElse(throw new IllegalArgumentException(s"column $columnName get null value, but no default value for this column."))
//        }
//      }else{
//        val data = if(isEnumerationType){ //for enumeration, the raw data is int, and need to be convert to enumeration value of [T]
//          lazy val universeMirror = ru.runtimeMirror(getClass.getClassLoader)
//          val defaultEnum = this.defaultValue.getOrElse{
//            throw new IllegalStateException(s"column ${columnName} is enumeration, must have a default value")
//          }
//          val im = m.reflect(defaultEnum)(classTag)
//          typeTag.tpe.decls.filter{
//            scope=>
//              scope.asTerm.fullName.contains("outerEnum")
//          }.headOption.map{
//            scope=>
//              //Get enumeration Object
//              val enumObject = im.reflectField(scope.asTerm).get
//              enumObject.asInstanceOf[Enumeration].apply(rawValue.asInstanceOf[Int])
//          }.get
//        }else if(columnType.getName.equals(DataType.Name.BLOB)){ //for blob ,the field class may be DataBuffer or Array[Byte]
//          val fieldClass = m.runtimeClass(typeTag.tpe)
//          if(fieldClass.equals(dataClass)){ //DataBuffer
//            rawValue
//          }else{
//            rawValue.asInstanceOf[ByteBuffer].array()
//          }
//        }else{
//          rawValue
//        }
//
//        if(isOptionType){
//          Some(data).asInstanceOf[T]
//        }else{
//          data.asInstanceOf[T]
//        }
//      }
//    }
//  }
  
}
object Column{
  private val baseTypes = Map(
    ru.typeOf[Int]->DataType.cint(),
    ru.typeOf[String]->DataType.text(),
    ru.typeOf[Date]->DataType.timestamp(),
    ru.typeOf[Long]->DataType.bigint(),
    ru.typeOf[Float]->DataType.cfloat(),
    ru.typeOf[Double]->DataType.cdouble(),
    ru.typeOf[Boolean]->DataType.cboolean(),
    ru.typeOf[Array[Byte]]->DataType.blob(),
    ru.typeOf[java.nio.ByteBuffer]->DataType.blob(),
    ru.typeOf[Short] -> DataType.smallint()
  )

  lazy val m = ru.runtimeMirror(getClass.getClassLoader)
  lazy val setClassSymbol = m.classSymbol(classOf[Set[_]])
  lazy val seqClassSymbol = m.classSymbol(classOf[Seq[_]])
  lazy val mapClassSymbol = m.classSymbol(classOf[Map[_,_]])
  lazy val optionClassSymbol = m.classSymbol(classOf[Option[_]])
  lazy val enumerationValueClassSymbol =  m.classSymbol(classOf[Enumeration#Value])


  def getBaseDataType(tpe:ru.Type):DataType =
    baseTypes.get(tpe).getOrElse(throw new IllegalArgumentException(s"Unsupported data type for cassandra: ${tpe.toString}"))


  def getDataType[T](implicit typeTag:ru.TypeTag[T]): DataType ={
    val tpe = typeTag.tpe
    if(tpe.baseClasses.contains(setClassSymbol)){
      val dataType = getBaseDataType(tpe.typeArgs.head)
      DataType.set(dataType)
    }else if(tpe.baseClasses.contains(seqClassSymbol)){
      val dataType = getBaseDataType(tpe.typeArgs.head)
      DataType.list(dataType)
    }else if(tpe.baseClasses.contains(mapClassSymbol)){
      val types = tpe.typeArgs.map(getBaseDataType)
      val (dataTypeKey,dataTypeValue) = (types(0),types(1))
      DataType.map(dataTypeKey,dataTypeValue)
    }else if(tpe.baseClasses.contains(optionClassSymbol)){
      getBaseDataType(tpe.typeArgs.head)
    }else if(tpe.baseClasses.contains(enumerationValueClassSymbol)){
      DataType.cint()
    }else{
      getBaseDataType(tpe)
    }
  }

  def apply[T](columnName:String,defaultValue:Option[T]=None)(implicit typeTag:ru.TypeTag[T],classTag:ClassTag[T]):Column[T]={
    DefaultColumn(columnName,getDataType[T])
  }

}
object ListColumn{
  def apply[T](columnName:String)(implicit typeTag:ru.TypeTag[T],classTag:ClassTag[T]):ListColumn[T]={
    ListColumn(columnName,Column.getDataType[T])
  }
}

object DefaultColumn{
  def apply[T](columnName:String)(implicit typeTag:ru.TypeTag[T],classTag:ClassTag[T]):DefaultColumn[T]={
    DefaultColumn(columnName,Column.getDataType[T])
  }
}