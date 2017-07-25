package top.wagzhi.catrix

import java.nio.ByteBuffer
import java.sql.Timestamp
import java.util

import com.datastax.driver.core.DataType.CollectionType
import com.datastax.driver.core._
import org.slf4j.LoggerFactory
import top.wagzhi.catrix.query._

import scala.reflect.{ClassTag, ManifestFactory}
import scala.reflect.runtime.{universe => ru}
import scala.collection.JavaConverters._

/**
  * Created by paul on 2017/7/20.
  */
trait Table[T] {
  val logger = LoggerFactory.getLogger(getClass)
  val modelType:Class[T]

  lazy val (modelName ,modelFields )= {
    val m = ru.runtimeMirror(getClass.getClassLoader)
    val constructorMethod = m.classSymbol(modelType).asType.toType.decl(ru.termNames.CONSTRUCTOR).asMethod
    val name = m.classSymbol(modelType).name.toString
    val columnNames = constructorMethod.paramLists.flatMap{
      l=>
        l.map(_.name.toString)
    }
    (name,columnNames)
  }

  lazy val (tableName,columnNames) = (
    toTableOrColumnName(modelName),
    modelFields.map(toTableOrColumnName)
  )


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
    def as[T]() :T={
      val m = ru.runtimeMirror(getClass.getClassLoader)
      val constructorMethod = m.classSymbol(modelType).asType.toType.decl(ru.termNames.CONSTRUCTOR).asMethod

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
              tpe.asInstanceOf[CollectionType].getTypeArguments.asScala.headOption.map{
                setType=>
                     setType.getName match {
                      case DataType.Name.INT=>{
                        row.getSet(name,classOf[Int]).asScala.toSet
                      }
                      case DataType.Name.TEXT =>{
                        row.getSet(name,classOf[String]).asScala.toSet
                      }
                      case DataType.Name.VARCHAR =>{
                        row.getSet(name,classOf[String]).asScala.toSet
                      }
                      case DataType.Name.DOUBLE =>{
                        row.getSet(name,classOf[Double]).asScala.toSet
                      }
                      case DataType.Name.TIMESTAMP=>{
                        row.getSet(name,classOf[java.util.Date]).asScala.toSet
                      }
                      case _=>{
                        throw new IllegalArgumentException("Unsupported type "+tpe.getName.toString)
                      }
                    }
              }.get
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


  def save(obj:T)(implicit conn:Connection)= conn.withSession{
    session=>
      val stmt = session.prepare(insertQuery)
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
