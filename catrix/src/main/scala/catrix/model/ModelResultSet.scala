package catrix.model

import com.datastax.driver.core.ResultSet

/**
  * Created by paul on 2017/9/15.
  */
case class ModelResultSet[T](resultSet:ResultSet,rows:Seq[T],pagingState:String)

case class PageResult[T](results:Seq[T],pagingState:String)
