package catrix.model

import java.nio.ByteBuffer
import java.util.Date

import catrix.exception.ExecuteException
import catrix.query._
import com.datastax.driver.core.{DataType, Row}
import org.slf4j.LoggerFactory

import scala.collection.JavaConversions._
import scala.collection.JavaConverters._
import scala.reflect.runtime.{universe => ru}
import scala.reflect.ClassTag


case class ListColumn[T](override val columnName:String,
                              override val columnType:DataType,
                         override val isIndex:Boolean = false)
                             (implicit val typeTag:ru.TypeTag[T], val classTag: ClassTag[T])
  extends Column[Seq[T]](columnName = columnName,columnType = columnType,isIndex = isIndex){
  override def apply(row: Row): Seq[T] = {
    val clazz = Column.getRuntimeClass(typeTag.tpe)
    row.getList(this.columnName,clazz).toIndexedSeq.map(_.asInstanceOf[T])
  }

  def contains(t:T)={
    new ContainsQueryFilter[T](this,t)
  }
  def valueAsJava(v:Seq[T]):Any = {
    v.asInstanceOf[Seq[Object]].asJava
  }
  override def index = this.copy(isIndex = true)
}

case class SetColumn[T](override val columnName:String,
                         override val columnType:DataType,
                         override val isIndex:Boolean = false)
                        (implicit val typeTag:ru.TypeTag[T], val classTag: ClassTag[T])
  extends Column[Set[T]](columnName = columnName,columnType = columnType,isIndex = isIndex){
  override def apply(row: Row): Set[T] = {
    val clazz = Column.getRuntimeClass(typeTag.tpe)
    row.getSet(this.columnName,clazz).toSet[Any].map(_.asInstanceOf[T])
  }

  def contains(t:T)={
    new ContainsQueryFilter[T](this,t)
  }

  def valueAsJava(v:Set[T]):Any = {
    v.asInstanceOf[Set[Object]].asJava
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

  def valueAsJava(v:Map[K,V]):Any = {
    v.asInstanceOf[Map[Object,Object]].asJava
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
  extends SingleValueColumn[Option[T]](columnName = columnName,columnType = columnType,isIndex = isIndex){
  override def apply(row: Row): Option[T] = {
    this.getRawValue[T](row)
  }

  override def index = this.copy(isIndex = true)

}


//case class EnumerationColumn[T <:Enumeration#Value](override val columnName:String,
//                            override val columnType:DataType,
//                            override val isIndex:Boolean = false,override val defaultValue:Option[T]= None)
//                           (implicit override val typeTag:ru.TypeTag[T], override val classTag: ClassTag[T])
//  extends SingleValueColumn[T](columnName = columnName,columnType = columnType,isIndex = isIndex){
//  override def index = this.copy(isIndex = true)
//  override def default(t: T) = this.copy(defaultValue = Some(t))
//
//
//  private def getEnumerationValue(id:Int):T ={
//    val universeMirror = ru.runtimeMirror(getClass.getClassLoader)
//    val valueName = typeTag.tpe.toString
//    val className = valueName.substring(0,valueName.lastIndexOf('.'))
//    val clazz = Class.forName(className)
//    val cs = universeMirror.classSymbol(clazz)
//    val cm =universeMirror.reflectModule(cs.companion.asModule)
//    val enumInst = cm.instance.asInstanceOf[Enumeration]
//    enumInst(id).asInstanceOf[T]
//  }
//
//  override def apply(row: Row): T = {
//    val id = row.getInt(columnName)
//    this.getEnumerationValue(id)
//  }
//
//}


case class DefaultColumn[T](override val columnName:String,
                            override val columnType:DataType,
                            override val isIndex:Boolean = false,
                            val defaultValue:Option[T]=None)
                           (implicit val typeTag:ru.TypeTag[T], val classTag: ClassTag[T])
  extends SingleValueColumn[T](columnName = columnName,columnType = columnType,isIndex = isIndex){
  override def index = this.copy(isIndex = true)

  override def apply(row: Row): T = {
    this.getRawValue[T](row).getOrElse{
      defaultValue.getOrElse{
        //TOD add column type default value
        throw ExecuteException(s"Column ${columnName}'s value is none, use option column or set column's default value!",null)
      }
    }
  }

  def default(t: T) = this.copy(defaultValue = Some(t))
}


abstract class SingleValueColumn[T](override val columnName:String,
                            override val columnType:DataType,
                            override val isIndex:Boolean)

  extends Column[T](columnName = columnName,columnType = columnType,isIndex = isIndex){

  def apply(value:T):ColumnValue[T]={
    ColumnValue(this,value)
  }

  def valueAsJava(v:T):Any = {
    if(v.isInstanceOf[Enumeration#Value]){
      v.asInstanceOf[Enumeration#Value].id
    }else if(v.isInstanceOf[Array[Byte]]){
      ByteBuffer.wrap(v.asInstanceOf[Array[Byte]])
    }
    else{
      v
    }
  }
  private def getEnumerationValue[RT](id:Int)(implicit typeTag: ru.TypeTag[RT]):RT ={
    val universeMirror = ru.runtimeMirror(getClass.getClassLoader)
    val valueName = typeTag.tpe.toString
    val className = valueName.substring(0,valueName.lastIndexOf('.'))
    val clazz = Class.forName(className)
    val cs = universeMirror.classSymbol(clazz)
    val cm =universeMirror.reflectModule(cs.companion.asModule)
    val enumInst = cm.instance.asInstanceOf[Enumeration]
    enumInst(id).asInstanceOf[RT]
  }

  def typeOf[RT](implicit typeTag:ru.TypeTag[RT]) = typeTag.tpe
  def getRawValue[RT](row:Row)(implicit typeTag: ru.TypeTag[RT]):Option[RT] = {
    //  val universeMirror = ru.runtimeMirror(getClass.getClassLoader)

    val clazz = Column.getRuntimeClass(typeTag.tpe)
    //convert cassandra original java class to model defined class
    val rawValue = if(typeTag.tpe.baseClasses.contains(typeOf[Enumeration#Value].typeSymbol.asClass)){
      if(row.isNull(columnName)){
        null
      }else{
        val id = row.getInt(columnName)
        this.getEnumerationValue(id)
      }
    }else if(clazz.isInstanceOf[Array[Byte]]){
      val bf = row.getBytes(columnName)
      if(bf == null){
        null
      }else{
        bf.array()
      }
    }else{
      row.get(columnName,clazz)
    }
    //map null to None
    if(rawValue == null){
      None
    }else{
      Some(rawValue.asInstanceOf[RT])
    }
  }

  def in(t:Seq[T])={
    new InQueryFilter[T](this,t:_*)
  }

  def ==(t:T) = {
    EqualsQueryFilter(this,t)
  }

}
/**
  * Created by paul on 2017/9/14.
  */
abstract class Column[T](val columnName:String,val columnType:DataType,val isIndex:Boolean){
  private val logger = LoggerFactory.getLogger(getClass)

  def apply(row:Row):T

  def valueAsJava(t:T):Any

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

}
object Column{

  def getRuntimeClass(tpe:ru.Type) ={
    val m = ru.runtimeMirror(getClass.getClassLoader)
    val clazz =  m.runtimeClass(tpe)
    val className = clazz.getName
    className match{
      case "int"  => classOf[java.lang.Integer]
      case "long" => classOf[java.lang.Long]
      case "float" => classOf[java.lang.Float]
      case "double" => classOf[java.lang.Double]
      case _=> clazz
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