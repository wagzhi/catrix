package top.wagzhi.catrix

import com.datastax.driver.core.{BoundStatement, Cluster, Session}
import top.wagzhi.catrix.query.{Column, Filter, Query}

import scala.reflect.{ClassTag, ManifestFactory}
import scala.reflect.runtime.{universe => ru}
/**
  * Created by paul on 2017/7/20.
  */
trait Table[T] {
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


//  implicit class ModelColumn(name:String) {
//    val cName = toTableOrColumnName(name)
//    def ===(value:Any):Filter = {
//      Column(name) === value
//    }
//  }

  def filter(filter:Filter) = Query(this.toTableOrColumnName(modelName),Seq(filter))

  def getTypeTag[T: ru.TypeTag](obj: T) = ru.typeTag[T]
  def save(obj:T)(implicit conn:Connector)= conn.withSession{
    session=>
      val stmt = session.prepare(insertQuery)
      val m = ru.runtimeMirror(getClass.getClassLoader)
      val typeSymbol = m.classSymbol(modelType).toType
      val im = m.reflect(obj)(ClassTag(modelType))
      val values = this.modelFields.map{
        field=>
          val fieldTerm = typeSymbol.decl(ru.TermName(field)).asTerm
          val fm = im.reflectField(fieldTerm)
          fm.get
      }.asInstanceOf[List[Object]]
//      val im = m.reflect[BaseModel](obj.asInstanceOf[BaseModel])
//      val f = typeSymbol.decl(ru.TermName("unapply")).asMethod
//      val fm = im.reflectMethod(f)
//
//      val values = fm.apply().asInstanceOf[Seq[Object]]

      //val values = obj.asInstanceOf[BaseModel].unapply().asInstanceOf[Seq[Object]]

      val bStmt = new BoundStatement(stmt).bind(values:_*)
      session.execute(bStmt)

  }
}
