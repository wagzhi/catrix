package top.wagzhi.catrix.query

import com.datastax.driver.core.Row

/**
  * Created by paul on 2017/9/13.
  */
trait TupledColumns {

}

trait RowParser[T]{
  def paser(row:Row):T
}

case class TupledColumns1[T](t:CassandraColumn[T]) extends TupledColumns{
  def tupled = Tuple1(t)
  def ~[T](t:T) = TupledColumns2(this.t,t)
  def <>[M](f:Tuple1[T]=>M):RowParser[M] =new RowParser[M]{
    override def paser(row: Row): M = {
      val v = t.apply(row)
      f(Tuple1(v))
    }
  }
}

case class TupledColumns2[T1,T2](t1:T1,t2:T2) extends TupledColumns{
  def parser[M](f:Tuple2[T1,T2]=>M) = f(this.tupled)
  def tupled:Tuple2[T1,T2] = (t1,t2)
}

object R extends App{
  val t = TupledColumns2(1,"a")
  case class User(id:Int,name:String)
  t.parser(User.tupled)
}