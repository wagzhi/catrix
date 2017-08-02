package top.wagzhi.catrix

import com.datastax.driver.core.ResultSet

/**
  * Created by paul on 2017/8/2.
  */
case class MappedResultSet[T](resultSet:ResultSet,rows:Seq[T],pagingState:String)


