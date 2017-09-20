package catrix.model

import java.util.Date

import catrix.query._
import com.datastax.driver.core.{DataType, Row}
import org.slf4j.LoggerFactory

import scala.collection.JavaConversions._
import scala.reflect.runtime.{universe => ru}
import scala.reflect.ClassTag


case class ListColumn[T](override val columnName:String,
                              override val columnType:DataType,
                         override val isIndex:Boolean = false)
                             (implicit val typeTag:ru.TypeTag[T], val classTag: ClassTag[T])
  extends Column[Seq[T]](columnName = columnName,columnType = columnType,isIndex = isIndex){
  override def apply(row: Row): Seq[T] = {
    val m = ru.runtimeMirror(this.getClass.getClassLoader)
    val clazz = m.runtimeClass(typeTag.tpe)
    row.getList(this.columnName,clazz).toIndexedSeq.map(_.asInstanceOf[T])
  }

  def contains(t:T)={
    new ContainsQueryFilter[T](this,t)
  }

  override def index = this.copy(isIndex = true)
}

case class SetColumn[T](override val columnName:String,
                         override val columnType:DataType,
                         override val isIndex:Boolean = false)
                        (implicit val typeTag:ru.TypeTag[T], val classTag: ClassTag[T])
  extends Column[Set[T]](columnName = columnName,columnType = columnType,isIndex = isIndex){
  override def apply(row: Row): Set[T] = {
    val m = ru.runtimeMirror(this.getClass.getClassLoader)
    val clazz = m.runtimeClass(typeTag.tpe)
    row.getSet(this.columnName,clazz).toSet[Any].map(_.asInstanceOf[T])
  }

  def contains(t:T)={
    new ContainsQueryFilter[T](this,t)
  }

  override def index = this.copy(isIndex = true)
}



case class MapColumn[K,V](override val columnName:String,
                          override val columnType:DataType,
                          override val isIndex:Boolean = false,
                          val isKeyIndex:Boolean = false,
                          val isValueIndex:Boolean = false,
                          val isEntryIndex:Boolean = false
                         )
                         (implicit val typeTag:ru.TypeTag[K], val typeTagV:ru.TypeTag[V])
  extends Column[Map[K,V]](columnName = columnName,columnType = columnType,isIndex = isIndex){
  override def apply(row: Row): Map[K,V] = {
    val clazz = Column.getRuntimeClass(typeTag.tpe)
    val clazzV = Column.getRuntimeClass(typeTagV.tpe)
    row.getMap(this.columnName,clazz,clazzV).toMap[Any,Any].asInstanceOf[Map[K,V]]
  }
  override def index = this.copy(isIndex = true,isValueIndex = true)
  def keyIndex = this.copy(isIndex = true,isKeyIndex = true)(typeTag,typeTagV)
  def valueIndex = this.copy(isIndex = true,isValueIndex = true)(typeTag,typeTagV)
  def entryIndex = this.copy(isIndex = true,isEntryIndex = true)(typeTag,typeTagV)

  override def indexCql(tableName:String)={
    var r = Seq[String]()
    if(isKeyIndex){
      r = r :+ s"create index ${tableName}_${columnName}_key_idx on ${tableName} ( KEYS ($columnName))"
    }
    if(isValueIndex){
      r = r :+ s"create index ${tableName}_${columnName}_value_idx on ${tableName} ( VALUES ($columnName))"
    }
    if(isEntryIndex){
      r = r :+ s"create index ${tableName}_${columnName}_entry_idx on ${tableName} ( ENTRIES ($columnName))"
    }
    r
  }
  def containsKey(key:K)={
    new ContainsKeyQueryFilter(this,key)
  }

  def contains(value:V) ={
    new ContainsQueryFilter(this,value)
  }

  def hasEntry(kv:(K,V)) = {
    new EntryQueryFilter(this,kv._1,kv._2)
  }
}

case class OptionColumn[T](override val columnName:String,
                            override val columnType:DataType,
                           override val isIndex:Boolean = false
                          )
                          (implicit val typeTag:ru.TypeTag[T], val classTag: ClassTag[T])
  extends Column[Option[T]](columnName = columnName,columnType = columnType,isIndex = isIndex){
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

  override def index = this.copy(isIndex = true)

}


case class EnumerationColumn[T <:Enumeration#Value](override val columnName:String,
                            override val columnType:DataType,
                            override val isIndex:Boolean = false,override val defaultValue:Option[T]= None)
                           (implicit override val typeTag:ru.TypeTag[T], override val classTag: ClassTag[T])
  extends SingleValueColumn[T](columnName = columnName,columnType = columnType,isIndex = isIndex){
  override def index = this.copy(isIndex = true)
  override def default(t: T) = this.copy(defaultValue = Some(t))


  private def getEnumerationValue(id:Int):T ={
    val universeMirror = ru.runtimeMirror(getClass.getClassLoader)
    val valueName = typeTag.tpe.toString
    val className = valueName.substring(0,valueName.lastIndexOf('.'))
    val clazz = Class.forName(className)
    val cs = universeMirror.classSymbol(clazz)
    val cm =universeMirror.reflectModule(cs.companion.asModule)
    val enumInst = cm.instance.asInstanceOf[Enumeration]
    enumInst(id).asInstanceOf[T]
  }

  override def apply(row: Row): T = {
    val id = row.getInt(columnName)
    this.getEnumerationValue(id)
  }

}


case class DefaultColumn[T](override val columnName:String,
                            override val columnType:DataType,
                            override val isIndex:Boolean = false,override val defaultValue:Option[T]=None)
                           (implicit override val typeTag:ru.TypeTag[T], override val classTag: ClassTag[T])
  extends SingleValueColumn[T](columnName = columnName,columnType = columnType,isIndex = isIndex){
  override def index = this.copy(isIndex = true)

  override def default(t: T) = this.copy(defaultValue = Some(t))
}


abstract class SingleValueColumn[T](override val columnName:String,
                            override val columnType:DataType,
                            override val isIndex:Boolean,val defaultValue:Option[T]= None)
                           (implicit val typeTag:ru.TypeTag[T], val classTag: ClassTag[T])
  extends Column[T](columnName = columnName,columnType = columnType,isIndex = isIndex){
  override def apply(row: Row): T = {
    val m = ru.runtimeMirror(this.getClass.getClassLoader)
    val clazz = m.runtimeClass(typeTag.tpe)
    val rawValue = row.get(columnName,clazz)
    rawValue.asInstanceOf[T]
  }

  def in(t:Seq[T])={
    new InQueryFilter[T](this,t:_*)
  }

  def ==(t:T) = {
    EqualsQueryFilter(this,t)
  }

  def default(t:T):SingleValueColumn[T]

}
/**
  * Created by paul on 2017/9/14.
  */
abstract class Column[T](val columnName:String,val columnType:DataType,val isIndex:Boolean){
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

  def index:Column[T]

  def indexCql(tableName:String):Seq[String] = {
    if (isIndex) {
      Seq(s"create index ${tableName}_${columnName}_idx on ${tableName} ($columnName)")
    } else {
      Seq[String]()
    }
  }
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

  def getRuntimeClass(tpe:ru.Type) ={
    val m = ru.runtimeMirror(getClass.getClassLoader)
    val clazz =  m.runtimeClass(tpe)
    if(clazz.getName.equals("int")){
      classOf[java.lang.Integer]
    }else{
      clazz
    }
  }

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
    val listDataType = DataType.list(Column.getDataType[T])
    ListColumn(columnName,listDataType)
  }
}

object SetColumn{
  def apply[T](columnName:String)(implicit typeTag:ru.TypeTag[T],classTag:ClassTag[T]):SetColumn[T]={
    val setDataType = DataType.set(Column.getDataType[T])
    SetColumn(columnName,setDataType)
  }
}
object MapColumn{
  def apply[K,V](columnName:String)(implicit typeTag:ru.TypeTag[K],typeTag2:ru.TypeTag[V]):MapColumn[K,V]={
    val kdt = Column.getDataType[K](typeTag)
    val vdt = Column.getDataType[V](typeTag2)
    val mapDataType = DataType.map(kdt,vdt)
    MapColumn(columnName,mapDataType)
  }

}

object DefaultColumn{
  def apply[T](columnName:String)(implicit typeTag:ru.TypeTag[T],classTag:ClassTag[T]):DefaultColumn[T]={
    DefaultColumn(columnName,Column.getDataType[T])
  }
}