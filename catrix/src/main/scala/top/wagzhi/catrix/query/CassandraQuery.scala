package top.wagzhi.catrix.query

import com.datastax.driver.core.{BoundStatement, PagingState}
import org.slf4j.LoggerFactory
import top.wagzhi.catrix.Connection
import top.wagzhi.catrix.exception.ExecuteException

/**
  * Created by paul on 2017/7/31.
  */
case class CassandraQuery(
                           tableName:String,
                           queryAction: QueryAction.QueryAction,
                           values:Seq[Any] = Seq[Any](),
                           columns:Seq[CassandraColumn[_]]= Seq[CassandraColumn[_]](),
                           filters:Seq[QueryFilter[_]] =  Seq[QueryFilter[_]](),
                           page:Pagination = Pagination(),
                           orderBy: Option[Order]= None,
                           isAllowFiltering:Boolean=false
                         ) {
  private val logger = LoggerFactory.getLogger(classOf[CassandraQuery])

  def filter(filter:QueryFilter[_]) = this.copy(filters = filters :+ filter)

  def page(pagingState:String="",pageSize:Int=20) = this.copy(page = Pagination(pagingState,pageSize))

  def orderBy(order:Order):CassandraQuery = this.copy(orderBy = Some(order))

  def allowFiltering(allowFiltering:Boolean) = this.copy(isAllowFiltering=allowFiltering)

  def queryValues= filters.flatMap{
    f=>
      f.value
  }

  def execute(implicit conn:Connection)=conn.withPreparedStatement(queryString) {
    (stmt, session) =>
      val bindValues = queryAction match {
        case QueryAction.select=>{
          this.queryValues.asInstanceOf[Seq[Object]]
        }
        case QueryAction.insert=>{
          this.values.asInstanceOf[Seq[Object]]
        }
        case QueryAction.update=>{
          this.values ++: this.queryValues
        }
        case QueryAction.delete=>{
          this.queryValues
        }
        case _=>{
          throw new IllegalArgumentException("Unsupported query action: "+this.queryAction)
        }
      }
      try{
        val bstmt = new BoundStatement(stmt).bind(bindValues.asInstanceOf[Seq[Object]]: _*)
        bstmt.setFetchSize(page.pageSize)
        if (page.pagingState.length > 0) {
          bstmt.setPagingState(PagingState.fromString(page.pagingState))
        }
        session.execute(bstmt)
      }catch{
        case t:Throwable=>{
            val valueList = bindValues.map(_.toString).mkString(", ")
            throw new ExecuteException(s"Execute cql failed: $queryString,\n with values: ($valueList)",t)
        }
      }
  }

  def queryString:String = {
    val filter = if (filters.nonEmpty){
      val filterString= filters.map(_.queryString).mkString(" and ")
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
        " order by "+od.column.columnName +" "+od.orderType.toString
    }.getOrElse("")

    queryAction match {
      case QueryAction.select=>{
        val columnString = columns.map(_.columnName).mkString(", ")
        s"select $columnString from $tableName$filter$orderByString$allowFilteringString"
      }
      case QueryAction.update=>{
        val setString = this.columns.map(_.columnName +" = ?").mkString(", ")
        s"update $tableName set $setString$filter$allowFilteringString"
      }
      case QueryAction.insert=>{
        val columnString = columns.map(_.columnName).mkString(", ")
        val valueString = columns.map(_=>'?').mkString(", ")
        s"insert into $tableName ($columnString) values ($valueString)"
      }
      case QueryAction.delete=>{
        s"delete from $tableName$filter$allowFilteringString"
      }
      case _ =>{
        throw new IllegalArgumentException("Unsupported QueryAction!")
      }
    }

  }
}
object QueryAction extends Enumeration{
  type QueryAction = Value
  val select,update,insert,delete = Value
}
