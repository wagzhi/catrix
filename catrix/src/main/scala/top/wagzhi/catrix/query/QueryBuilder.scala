package top.wagzhi.catrix.query

import com.datastax.driver.core.{BoundStatement, PagingState, ResultSet, Row}
import org.slf4j.LoggerFactory
import top.wagzhi.catrix.{Connection, Table}

import scala.reflect.runtime.{universe => ru}

/**
  * Created by paul on 2017/7/20.
  */
object QueryBuilder {
//  implicit class RowWrap(val row:Row){
//    def as[T]:T = {
//      val m = ru.runtimeMirror(getClass.getClassLoader)
//      val typeSymbol = m.classSymbol(clazz).toType
//    }
//  }


  implicit class TableColumnName(filedName:String) {
    def name = {
      val n1 = filedName.flatMap {
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
    def asColumn = Column(this.name)

    def === (value:Any):Filter = Filter(Column(name),"=",value)
    def > (value:Any):Filter= Filter(Column(name),">",value)
    def >== (value:Any):Filter= Filter(Column(name),">=",value)
    def < (value:Any):Filter= Filter(Column(name),"<",value)
    def <== (value:Any):Filter= Filter(Column(name),"<=",value)
    def _contains (value:Any):Filter = Filter(Column(name),"contains",value)
  }

  class === extends FilterWord{
    val word = "="
  }
  class > extends FilterWord{
    val word =">"
  }

}
trait FilterWord{

}
case class Column(name:String)

case class Filter(column:Column,word:String,value:Any)

case class Page(size:Int=20,state:String="")

case class PageResult[T](rows:T,pagingState: String)

case class OrderBy(column:Column,order:String)

case class Query[T](tableName:String ,
                    filters:Seq[Filter]= Seq[Filter](),
                    page:Page=Page(),
                    orderBy: Option[OrderBy] = None,
                    isAllowFiltering:Boolean=false)
                   (implicit table:Table[T]){

  import QueryBuilder._
  private val logger = LoggerFactory.getLogger(getClass)

  def filter(filter: Filter):Query[T] ={
    this.copy(filters = filters :+ filter)
  }

  def allowFiltering = this.copy(isAllowFiltering=true)

  def page(size:Int=20,state:String="") = this.copy(page=Page(size,state))

  def order(fieldName:String,order:String="asc") = this.copy(orderBy= Some(OrderBy(fieldName.asColumn,order)))

  def pageState(pagingState:String):Query[T]={
    val p = this.page.copy(state = pagingState)
    this.copy(page = p)
  }
  def pageSize(pageSize:Int):Query[T] ={
    this.copy(page=page.copy(size=pageSize))
  }
  def deleteQuery  = {
    val query = queryString
    s"delete from $tableName $query"
  }
  def selectQuery = {
    val query = queryString
    s"select * from $tableName $query"
  }
  private def queryString = {
    val filter = if (filters.nonEmpty){
      val filterString= filters.map{
          f=>
            f.column.name + " "+f.word+" ?"
        }.mkString(" and ")
      filters.foldLeft(""){
        (l,f)=>
          l+" "+f.word +" ?"
          l
      }
      s" where $filterString"
    }else{
      ""
    }
    val allowFilteringString = if(isAllowFiltering){
      " allow filtering"
    }else{
      ""
    }


    val orderByString = orderBy.map{
      od=>
        " order by "+od.column.name +" "+od.order +" "
    }.getOrElse("")

    s"$filter$orderByString$allowFilteringString"

  }
  def queryValues= filters.map{
    f=>
      f.value
  }



  def update(values:Map[String,Any])(implicit conn:Connection) = conn.withSession{
    session=>
      val query = s"update $tableName set" + values.keySet.map(a=>s" $a = ? ").mkString(",")+this.queryString
      println(query)
      val stmt = session.prepare(query)
      val vs = values.values.toSeq ++: this.queryValues
      vs.foreach(println)
      val bstmt = new BoundStatement(stmt).bind(vs.asInstanceOf[Seq[Object]]:_*)
      bstmt.setFetchSize(page.size)
      if(page.state.length>0){
        bstmt.setPagingState(PagingState.fromString(page.state))
      }
      session.execute(bstmt)
  }

  def delete(implicit conn:Connection)= {
    execute(this.deleteQuery)
  }

  def select(implicit conn:Connection) = {
    val query = this.selectQuery
    logger.info(query)
    execute(query)
  }

  def execute(query:String)(implicit conn:Connection)=conn.withSession{
      session=>
        val stmt = session.prepare(query)

        val bstmt = new BoundStatement(stmt).bind(this.queryValues.asInstanceOf[Seq[Object]]:_*)
        bstmt.setFetchSize(page.size)
        if(page.state.length>0){
          bstmt.setPagingState(PagingState.fromString(page.state))
        }
        session.execute(bstmt)
  }
}