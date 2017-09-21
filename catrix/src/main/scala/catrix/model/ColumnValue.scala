package catrix.model

/**
  * Created by paul on 2017/9/14.
  */
case class ColumnValue[T](column:Column[T],value:T) {
   def valueToCassandra:Any = column.valueAsJava(value)
}
