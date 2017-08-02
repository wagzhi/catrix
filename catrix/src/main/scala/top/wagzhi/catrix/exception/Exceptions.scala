package top.wagzhi.catrix.exception


/**
  * Created by paul on 2017/8/2.
  */
case class ExecuteException(message:String,cause:Throwable) extends Exception(message,cause)
case class PrepareStatementException(message: String,cause:Throwable) extends Exception(message,cause)
