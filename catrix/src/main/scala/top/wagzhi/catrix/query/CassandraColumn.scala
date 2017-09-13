package top.wagzhi.catrix.query

import java.nio.ByteBuffer
import java.util
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
                           fieldName:String = "",
                             default:Option[T] = None
                          )(implicit val typeTag:ru.TypeTag[T], val classTag: ClassTag[T]){
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

  def withDefault(defaultValue:T): CassandraColumn[T] ={
    this.copy(default = Some(defaultValue))
  }


  lazy val isEnumerationType:Boolean={
    typeTag.tpe.baseClasses.contains(CassandraColumn.enumerationValueClassSymbol)
  }
  lazy val isOptionType:Boolean={
    typeTag.tpe.baseClasses.contains(CassandraColumn.optionClassSymbol)
  }

  /**
    * get column value from cassandra row
    * @param row
    * @return
    */
  def apply(row:Row):T = {
    val m = ru.runtimeMirror(this.getClass.getClassLoader)
    val columnTypeName = columnType.getName
    if(columnTypeName.equals(DataType.Name.SET)){
      val clazz = typeTag.tpe.typeArgs.map(m.runtimeClass).map(CassandraColumn.mapPrimitiveTypes).head
      row.getSet(this.columnName,clazz).toSet[Any].asInstanceOf[T]
    }else if(columnTypeName.equals(DataType.Name.MAP)){
      val classSeq = typeTag.tpe.typeArgs.map(m.runtimeClass).map(CassandraColumn.mapPrimitiveTypes)
      val (classKey,classValue) = (classSeq(0),classSeq(1))
      row.getMap(this.columnName,classKey,classValue).toMap[Any,Any].asInstanceOf[T]
    }else if(columnTypeName.equals(DataType.Name.LIST)){
      val clazz = typeTag.tpe.typeArgs.map(m.runtimeClass).map(CassandraColumn.mapPrimitiveTypes).head
      row.getList(this.columnName,clazz).toIndexedSeq.asInstanceOf[T]
    }
    else{
      val clazz = if(typeTag.tpe.baseClasses.contains(CassandraColumn.optionClassSymbol)){
        typeTag.tpe.typeArgs.map(m.runtimeClass).map(CassandraColumn.mapPrimitiveTypes).head
      }else if(this.isEnumerationType){
        m.runtimeClass(ru.typeOf[Int])
      }else{
        m.runtimeClass(typeTag.tpe)
      }
      //get raw value
      val rawValue = if(columnType.getName.equals(DataType.Name.BLOB)){
        val bfClass = classOf[ByteBuffer]
        if(clazz.equals(bfClass)){
            row.get(columnName,bfClass)
        }else{
            row.get(columnName,bfClass).asInstanceOf[ByteBuffer].array()
        }

      }else{
        row.get(columnName,clazz)
      }


      //map raw value to corresponding type of T
      if(rawValue == null){
        if(isOptionType){
          None.asInstanceOf[T]
        }else{
          this.default.getOrElse(throw new IllegalArgumentException(s"column $columnName get null value, but no default value for this column."))
        }
      }else{
        if(isOptionType){
          Some(rawValue).asInstanceOf[T]
        }else if(isEnumerationType){
          lazy val universeMirror = ru.runtimeMirror(getClass.getClassLoader)
          val dv = this.default.getOrElse{
            throw new IllegalStateException(s"column ${columnName} is enumeration, must have a default value")
          }
          val im = universeMirror.reflect(dv)(classTag)
          val eo = typeTag.tpe.decls.filter{
            scope=>
              scope.asTerm.fullName.contains("outerEnum")
          }.headOption.map{
            scope=>
              //Get enumeration Object
              val enumObject = im.reflectField(scope.asTerm).get
              enumObject.asInstanceOf[Enumeration].apply(rawValue.asInstanceOf[Int])
          }.get
          eo.asInstanceOf[T]

        }else{
          rawValue.asInstanceOf[T]
        }
      }
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

object CassandraColumn{
  lazy val m = ru.runtimeMirror(getClass.getClassLoader)
  lazy val setClassSymbol = m.classSymbol(classOf[Set[_]])
  lazy val seqClassSymbol = m.classSymbol(classOf[Seq[_]])
  lazy val mapClassSymbol = m.classSymbol(classOf[Map[_,_]])
  lazy val optionClassSymbol = m.classSymbol(classOf[Option[_]])
  lazy val enumerationValueClassSymbol =  m.classSymbol(classOf[Enumeration#Value])
  private val typeMap = Map(
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
  def getBaseDataType(tpe:ru.Type):DataType =
    typeMap.get(tpe).getOrElse(throw new IllegalArgumentException("Unsupported data type "+tpe))

  def mapPrimitiveTypes(clazz:Class[_]):Class[_]={
    val name = clazz.getName
    name match{
      case "int"=> classOf[java.lang.Integer]
      case "long" => classOf[java.lang.Long]
      case "byte" => classOf[java.lang.Byte]
      case "boolean" => classOf[java.lang.Boolean]
      case "short" => classOf[java.lang.Short]
      case "float" => classOf[java.lang.Float]
      case "double" => classOf[java.lang.Double]
      case "char" => classOf[java.lang.Character]
      case _=> clazz
    }
  }



  def apply[T](columnName:String)(implicit typeTag:ru.TypeTag[T],classTag:ClassTag[T]):CassandraColumn[T]={
    val tpe = typeTag.tpe
    if(tpe.baseClasses.contains(setClassSymbol)){
      val dataType = getBaseDataType(tpe.typeArgs.head)
      CassandraColumn(columnName,DataType.set(dataType))
    }else if(tpe.baseClasses.contains(seqClassSymbol)){
      val dataType = getBaseDataType(tpe.typeArgs.head)
      CassandraColumn(columnName,DataType.list(dataType))
    }else if(tpe.baseClasses.contains(mapClassSymbol)){
      val types = tpe.typeArgs.map(getBaseDataType)
      val (dataTypeKey,dataTypeValue) = (types(0),types(1))
      CassandraColumn(columnName,DataType.map(dataTypeKey,dataTypeValue))
    }else if(tpe.baseClasses.contains(optionClassSymbol)){
      val dataType = getBaseDataType(tpe.typeArgs.head)
      CassandraColumn(columnName,dataType)
    }else if(tpe.baseClasses.contains(enumerationValueClassSymbol)){
      CassandraColumn(columnName,DataType.cint())
    }
    else{
      CassandraColumn(columnName,getBaseDataType(tpe))
    }

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
            val v = if(value.isInstanceOf[Array[Byte]]){
              ByteBuffer.wrap(value.asInstanceOf[Array[Byte]])
            } else if(value.isInstanceOf[Enumeration#Value]){
              value.asInstanceOf[Enumeration#Value].id
            }else{
              value
            }
            ColumnValue(column,v)
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