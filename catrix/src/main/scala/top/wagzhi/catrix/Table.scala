package top.wagzhi.catrix

import java.nio.ByteBuffer
import java.sql.Timestamp
import java.util
import java.util.Date

import com.datastax.driver.core.DataType.CollectionType
import com.datastax.driver.core._
import com.google.common.reflect.TypeToken
import org.slf4j.LoggerFactory
import top.wagzhi.catrix.query._

import scala.reflect.{ClassTag, ManifestFactory}
import scala.reflect.runtime.{universe => ru}
import scala.collection.JavaConversions._


/**
  * Created by paul on 2017/7/20.
  */
trait Table[T] {
  val modelType:Class[T]
  val logger = LoggerFactory.getLogger(getClass)
  lazy val m = ru.runtimeMirror(getClass.getClassLoader)

  lazy val (modelName ,modelFields )= {
    val constructorMethod = m.classSymbol(modelType).asType.toType.decl(ru.termNames.CONSTRUCTOR).asMethod
    val name = m.classSymbol(modelType).name.toString
    val columnNames = constructorMethod.paramLists.flatMap{
      l=>
        l.map(_.name.toString)
    }
    (name,columnNames)
  }


  lazy val tableName = toTableOrColumnName(modelName)

  lazy val columnNames = modelFields.map(toTableOrColumnName)

//  lazy val columns:Seq[CassandraColumn] = {
//    val constructorMethod = m.classSymbol(modelType).asType.toType.decl(ru.termNames.CONSTRUCTOR).asMethod
//    constructorMethod.paramLists.flatMap {
//      l =>
//        l.map(_.asTerm)
//    }.map{
//      fieldTerm=>
//        val fieldName = fieldTerm.name.toString
//        val fieldType = fieldTerm.typeSignature.resultType
//
//        val column:CassandraColumn = if(fieldType.baseClasses.contains(classOf[Int])){
//          CassandraColumn(fieldName,classOf[Int],toTableOrColumnName(fieldName),DataType.cint())
//        }else if(fieldType.baseClasses.contains(classOf[String])){
//          CassandraColumn(fieldName,classOf[Int],toTableOrColumnName(fieldName),DataType.text())
//        }else if(fieldType.baseClasses.contains(classOf[Date])){
//          CassandraColumn(fieldName,classOf[Int],toTableOrColumnName(fieldName),DataType.timestamp())
//        }else if(fieldType.baseClasses.contains(classOf[Array[Byte]])){
//          CassandraColumn(fieldName,classOf[Int],toTableOrColumnName(fieldName),DataType.blob())
//        }else if(fieldType.baseClasses.contains(classOf[ByteBuffer])){
//          CassandraColumn(fieldName,classOf[Int],toTableOrColumnName(fieldName),DataType.blob())
//        }else if(fieldType.baseClasses.contains(classOf[Seq[String]])){
//          CassandraColumn(fieldName,classOf[Int],toTableOrColumnName(fieldName),DataType.list(DataType.text()))
//        }else if(fieldType.baseClasses.contains(classOf[Seq[Int]])){
//          CassandraColumn(fieldName,classOf[Int],toTableOrColumnName(fieldName),DataType.list(DataType.cint()))
//        }
//        else{
//          throw new IllegalArgumentException("Unsupported type "+fieldType.toString+s" in field $fieldName of model $modelName" )
//        }
//
//        column
//
//    }
//  }

  implicit class ResultSetWap(val rs:ResultSet){
    def asPage[T] = {
      val prs = pageRows
      PageResult(prs._1.map(_.as[T]),prs._2)
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

      val remaing = rs.getAvailableWithoutFetching
      val it = rs.iterator()
      val rows = new Array[Row](remaing)
      for(i <- (0 to remaing-1)){
          rows(i) = it.next()
      }

      (rows,ps)
    }
  }


  implicit class RowWrap(row:Row){
    val typeMap = Map[DataType.Name,Any](
      (DataType.cint().getName,TypeToken.of(classOf[Int])),
      (DataType.text().getName,TypeToken.of(classOf[String]))
    )

    def as[T]() :T={
      val m = ru.runtimeMirror(getClass.getClassLoader)
      val constructorMethod = m.classSymbol(modelType).asType.toType.decl(ru.termNames.CONSTRUCTOR).asMethod
      DataType.Name.values()
      val values = columnNames.map{
        name=>
          val tpe = row.getColumnDefinitions.getType(name)

          tpe.getName match{
            case DataType.Name.INT=>{
              row.getInt(name)
            }
            case DataType.Name.TEXT =>{
              row.getString(name)
            }
            case DataType.Name.VARCHAR =>{
              row.getString(name)
            }
            case DataType.Name.DOUBLE =>{
              row.getDouble(name)
            }
            case DataType.Name.TIMESTAMP=>{
              row.getTimestamp(name)
            }
            case DataType.Name.BLOB=>{
              row.getBytes(name).array()
            }
            case DataType.Name.SET =>{
              tpe.asInstanceOf[CollectionType].getTypeArguments.toIterable.headOption.map{
                setType=>
                     setType.getName match {
                      case DataType.Name.INT=>{
                        row.getSet(name,classOf[Int]).toSet
                      }
                      case DataType.Name.TEXT =>{
                        row.getSet(name,classOf[String]).toSet
                      }
                      case DataType.Name.VARCHAR =>{
                        row.getSet(name,classOf[String]).toSet
                      }
                      case DataType.Name.DOUBLE =>{
                        row.getSet(name,classOf[Double]).toSet
                      }
                      case DataType.Name.TIMESTAMP=>{
                        row.getSet(name,classOf[java.util.Date]).toSet
                      }
                      case _=>{
                        throw new IllegalArgumentException("Unsupported type "+tpe.getName.toString)
                      }
                    }
              }.get
            }
            case DataType.Name.LIST =>{
              tpe.asInstanceOf[CollectionType].getTypeArguments.toIterable.headOption.map{
                seqType=>
                  seqType.getName match {
                    case DataType.Name.INT=>{
                      row.getList(name,classOf[Int]).toIndexedSeq
                    }
                    case DataType.Name.TEXT =>{
                      row.getList(name,classOf[String]).toIndexedSeq
                    }
                    case DataType.Name.VARCHAR =>{
                      row.getList(name,classOf[String]).toIndexedSeq
                    }
                    case DataType.Name.DOUBLE =>{
                      row.getList(name,classOf[Double]).toIndexedSeq
                    }
                    case DataType.Name.TIMESTAMP=>{
                      row.getList(name,classOf[java.util.Date]).toIndexedSeq
                    }
                    case _=>{
                      throw new IllegalArgumentException("Unsupported type "+tpe.getName.toString)
                    }
                  }
              }.get
            }
            case DataType.Name.MAP =>{
              val kv=tpe.asInstanceOf[CollectionType].getTypeArguments.toIndexedSeq
              val keyType= kv(0)
              val valueType = kv(1)
              (keyType.getName,valueType.getName) match {
                case (DataType.Name.TEXT,DataType.Name.TEXT)=>{
                    row.getMap(name,classOf[String],classOf[String])
                }
                case _=>{
                  //TODO support all cassandra  type
                  throw new IllegalArgumentException("Unsupported map type: key="+keyType.getName.toString+", value="+valueType.getName.toString)
                }
              }

            }
            case _=>{
              throw new IllegalArgumentException("Unsupported type "+tpe.getName.toString)
            }

          }
      }

      try{
        m.reflectClass(m.classSymbol(modelType)).reflectConstructor(constructorMethod).apply(values:_*).asInstanceOf[T]
      }catch{
        case e:Throwable=>
          val vs =values.map(_.toString).mkString("(",",",")")
          logger.error(s"mapping values $vs to object:$modelName failed!")
          throw e
      }

    }
  }

  def toTableOrColumnName(name:String): String ={
    val n1= name.flatMap{
      c=>
        if(c.isUpper) {
          "_"+c.toLower
        }else{
          c.toString
        }
    }
    if(n1.startsWith("_")){
      n1.substring(1)
    }else{
      n1
    }
  }

  lazy val insertQuery :String = {
    val t_name = toTableOrColumnName(modelName)
    val columns = modelFields.map(toTableOrColumnName)
    val statementPart1 = columns.foldLeft(s"insert into $t_name ("){
      (l,r)=>
        if(l.last == '('){
          l+r
        }else{
          l+","+r
        }
    }+") values("
    columns.foldLeft(statementPart1){
      (l,r)=>
        if(l.last == '('){
          l+"?"
        }else{
          l+","+"?"
        }
    }+")"
  }

  protected def filter(filter:Filter) = Query(tableName,Seq(filter))(this)

  protected def all = Query(tableName)(this)

  protected def page(size:Int=20,state:String="")= Query(tableName,Seq[Filter](),Page(size=size,state=state))(this)

  protected def getTypeTag[T: ru.TypeTag](obj: T) = ru.typeTag[T]


  def save(obj:T)(implicit conn:Connection)= conn.withPreparedStatement(insertQuery){
    (stmt:PreparedStatement,session: Session)=>
      val m = ru.runtimeMirror(getClass.getClassLoader)
      val typeSymbol = m.classSymbol(modelType).toType
      val im = m.reflect(obj)(ClassTag(modelType))
      val values = this.modelFields.map{
        field=>
          val fieldTerm = typeSymbol.decl(ru.TermName(field)).asTerm
          val fm = im.reflectField(fieldTerm)
          val v = fm.get

          //TODO add all cassandra types
          val resultType= fieldTerm.typeSignature.resultType
          if(resultType.baseClasses.contains(m.classSymbol(classOf[scala.collection.Set[Any]]))){
            if(resultType.typeArgs.contains(m.typeOf[String])){
              val set = new java.util.HashSet[String]()
              v.asInstanceOf[Set[String]].foreach(set.add)
              set
            }else{
              throw new IllegalArgumentException(s"type of collection $resultType is unsupported!")
            }

          }else if(resultType.baseClasses.contains(m.classSymbol(classOf[scala.collection.Seq[Any]]))){
            if(resultType.typeArgs.contains(m.typeOf[String])){
              val set = new java.util.ArrayList[String]()
              v.asInstanceOf[Seq[String]].foreach(set.add)
              set
            }else{
              throw new IllegalArgumentException(s"type of collection $resultType is unsupported!")
            }

          }else if(v.isInstanceOf[Array[Byte]]){
            val bytes= v.asInstanceOf[Array[Byte]]
            ByteBuffer.wrap(bytes)
          }else{
            v
          }

      }.asInstanceOf[List[Object]]
      val bStmt = new BoundStatement(stmt).bind(values:_*)
      session.execute(bStmt)
  }
}
