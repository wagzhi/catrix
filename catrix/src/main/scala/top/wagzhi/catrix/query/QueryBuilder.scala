package top.wagzhi.catrix.query

import com.datastax.driver.core.BoundStatement
import top.wagzhi.catrix.Connector

/**
  * Created by paul on 2017/7/20.
  */
object QueryBuilder {
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
    def column = Column(this.name)

    def === (value:Any) = {
      Column(this.name) === value
    }
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
case class Column(name:String){
  def === (value:Any):Filter ={
      Filter(this,"=",value)
  }
}

case class Filter(column:Column,word:String,value:Any)




case class Query(tableName:String ,filters:Seq[Filter]= Seq[Filter]()){
  def filter(filter: Filter):Query ={
    this.copy(filters = filters :+ filter)
  }
  def queryString = {
    val base = s"select * from $tableName"
    if (filters.nonEmpty){

      val filterString= filters.map{
          f=>
            f.column.name + " "+f.word+" ?"
        }.mkString(" and ")
      filters.foldLeft(""){
        (l,f)=>
          l+" "+f.word +" ?"
          l
      }
      s"$base where $filterString"
    }else{
      base
    }

  }
  def queryValues= filters.map{
    f=>
      f.value
  }

  def execute(implicit conn:Connector)=conn.withSession{
      session=>
        val stmt = session.prepare(this.queryString)
        val bstmt = new BoundStatement(stmt).bind(this.queryValues.asInstanceOf[Seq[Object]]:_*)
        session.execute(bstmt)
  }
}